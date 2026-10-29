"""The integration switch uses only disposable localhost Redis, never app config."""
import ast
import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from flask import Flask, request, make_response
from redis import Redis

import lanying_billing_hosting as hosting


def terms(**overrides):
    value = dict(app_id=uuid.uuid4().hex, order_id=uuid.uuid4().hex,
                 order_sequence=1, environment='TEST', product_id=9851,
                 legacy_price_cny=0, storage_mb=50, traffic_gb='0.5',
                 period_start=date.today().isoformat(),
                 period_end=(date.today() + timedelta(days=30)).isoformat(),
                 preserve_usage_period=False)
    value.update(overrides)
    return value


class HostingContractTest(unittest.TestCase):
    def test_strict_terms(self):
        for changes in ({'order_sequence': True}, {'storage_mb': -1}, {'product_id': 7001},
                        {'app_id': '../x'}, {'order_id': 'bad'}, {'traffic_gb': float('nan')},
                        {'traffic_gb': '-1'}, {'traffic_gb': '1e10'}, {'environment': 'prod'},
                        {'preserve_usage_period': 'true'}, {'period_start': '2026-02-30'},
                        {'period_end': '2020-01-01'}, {'unexpected': 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                hosting._terms(terms(**changes))
        self.assertEqual('0.5', hosting._terms(terms(traffic_gb='0.500'))['traffic_gb'])

    def test_http_auth_validation_and_conflict(self):
        # Import only this route; the full module initializes unrelated browser/storage services.
        path = Path(__file__).resolve().parents[1] / 'services/grow_ai_service.py'
        module = ast.parse(path.read_text())
        node = next(n for n in module.body if isinstance(n, ast.FunctionDef)
                    and n.name == 'billing_service_status')
        node.decorator_list = []
        context = dict(request=request, make_response=make_response,
                       check_access_token_valid=lambda: request.headers.get('access-token') == 'local-test')
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), context)
        app = Flask(__name__)
        app.add_url_rule('/provision', view_func=context['billing_service_status'], methods=['POST'])
        client = app.test_client()
        with patch.object(hosting, 'provision', return_value={'status': 'recorded'}) as provision:
            self.assertEqual(401, client.post('/provision', json={}).status_code)
            provision.assert_not_called()
            response = client.post('/provision', json=terms(), headers={'access-token': 'local-test'})
            self.assertEqual(200, response.status_code)
            self.assertEqual('recorded', response.json['data']['status'])
            for exception, status in ((ValueError('invalid'), 400), (hosting.ProvisionConflict('conflict'), 409)):
                provision.side_effect = exception
                self.assertEqual(status, client.post('/provision', json={}, headers={'access-token': 'local-test'}).status_code)


@unittest.skipUnless(os.environ.get('BILLING_TEST_LOCAL_REDIS') == '1', 'disposable local Redis only')
class HostingRedisTest(unittest.TestCase):
    def setUp(self):
        self.redis = Redis(host='127.0.0.1', port=36379, db=0)
        self.redis.ping()
        self.data = terms()
        self.key = 'lanying-connector:grow_ai:service_status:' + self.data['app_id']

    def tearDown(self):
        # Only the randomly named service from this test. Never FLUSHDB.
        self.redis.delete(self.key)
        self.redis.close()

    def provision(self, data=None):
        return hosting.provision(data or self.data, redis=self.redis)

    def test_replay_does_not_reset_or_write(self):
        first = self.provision()
        self.redis.hset(self.key, 'article_num', 42)
        before = self.redis.hgetall(self.key)
        self.assertEqual(first, self.provision())
        self.assertEqual(before, self.redis.hgetall(self.key))
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision(dict(self.data, storage_mb=500))
        self.assertEqual(before, self.redis.hgetall(self.key))

    def test_concurrent_duplicate_requests(self):
        def call(_):
            try:
                return self.provision()
            except hosting.ProvisionConflict:
                # Visible WATCH conflict can safely retry the same order.
                return self.provision()
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(call, range(8)))
        self.assertTrue(all(item == results[0] for item in results))
        self.assertEqual(self.data['order_id'].encode(), self.redis.hget(self.key, 'billing_order_id'))

    def test_newer_order_fences_late_delivery(self):
        self.provision()
        newer = dict(self.data, order_id=uuid.uuid4().hex, order_sequence=2, storage_mb=100)
        result = self.provision(newer)
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision()
        self.assertEqual(result, self.provision(newer))

    def test_test_order_cannot_replace_live(self):
        self.provision(dict(self.data, environment='LIVE'))
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision(dict(self.data, order_id=uuid.uuid4().hex, order_sequence=2))

    def test_legacy_paid_without_environment_cannot_be_replaced_by_test(self):
        self.redis.hset(self.key, mapping=dict(status='normal', product_id=9801,
            price=0, website_storage_limit=100, website_traffic_limit=1,
            pay_start_date=date.today().isoformat()))
        before = self.redis.hgetall(self.key)
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision()
        self.assertEqual(before, self.redis.hgetall(self.key))

    def test_legacy_write_cannot_reuse_stale_test_ownership(self):
        self.provision()
        original = self.redis.hgetall(self.key)
        for change in ({'product_id': '9801', 'price': '99'},
                       {'create_time': '1'}, {'pay_start_date': '2020-01-01'}, {'status': 'stopped'}):
            self.redis.hset(self.key, mapping=change)
            with self.subTest(change=change), self.assertRaises(hosting.ProvisionConflict):
                self.provision(dict(self.data, order_id=uuid.uuid4().hex, order_sequence=2))
            self.redis.hset(self.key, mapping=original)

    def test_legacy_same_second_same_terms_revokes_test_ownership(self):
        # Exercise the real legacy write, including its HMSET behavior, with
        # exactly the same second and all business values as the TEST record.
        self.data.update(product_id=9802, legacy_price_cny=99, storage_mb=100, traffic_gb='1')
        self.provision()
        original = self.redis.hgetall(self.key)
        path = Path(__file__).resolve().parents[1] / 'lanying_grow_ai.py'
        module = ast.parse(path.read_text())
        functions = [n for n in module.body if isinstance(n, ast.FunctionDef)
                     and n.name in ('open_service', 'close_service')]
        context = dict(lanying_redis=SimpleNamespace(get_redis_connection=lambda: self.redis),
                       get_service_status_key=lambda app: self.key, datetime=datetime,
                       time=SimpleNamespace(time=lambda: int(original[b'create_time'])))
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), 'exec'), context)
        result = context['open_service'](self.data['app_id'], 9802, 99, 100, 1)
        self.assertEqual('ok', result['result'])
        self.assertEqual(b'LEGACY', self.redis.hget(self.key, 'billing_writer'))
        for key, value in original.items():
            if key != b'billing_writer':
                self.assertEqual(value, self.redis.hget(self.key, key))
        for data in (self.data, dict(self.data, order_id=uuid.uuid4().hex, order_sequence=2)):
            with self.assertRaises(hosting.ProvisionConflict):
                self.provision(data)
        self.redis.hset(self.key, mapping=original)
        context['close_service'](self.data['app_id'], 9802)
        self.assertEqual(b'LEGACY', self.redis.hget(self.key, 'billing_writer'))
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision()

    def test_new_test_order_allows_verified_test_usage_changes(self):
        self.provision()
        self.redis.hset(self.key, 'article_num', 99)
        self.assertEqual('recorded', self.provision(dict(self.data, order_id=uuid.uuid4().hex,
            order_sequence=2, preserve_usage_period=True))['status'])
        self.assertEqual(b'99', self.redis.hget(self.key, 'article_num'))

    def test_reuse_preserves_usage_and_creation_anchor(self):
        anchor = (date.today() - timedelta(days=20)).isoformat()
        self.redis.hset(self.key, mapping=dict(status='normal', product_id=9851,
            website_storage_limit=50, website_traffic_limit='0.5', price=0,
            pay_start_date=anchor, create_time='123', article_num='7'))
        self.data['preserve_usage_period'] = True
        self.assertEqual(anchor, self.provision()['usage_period_start'])
        self.assertEqual(b'123', self.redis.hget(self.key, 'create_time'))
        self.assertEqual(b'7', self.redis.hget(self.key, 'article_num'))
        self.assertEqual(anchor, self.provision()['usage_period_start'])

    def test_reuse_rejects_changed_service(self):
        self.data['preserve_usage_period'] = True
        with self.assertRaises(hosting.ProvisionConflict):
            self.provision()
        self.assertFalse(self.redis.exists(self.key))

    def test_legacy_mutation_does_not_look_like_successful_replay(self):
        self.provision()
        before = self.redis.hgetall(self.key)
        for field, value in (('status', 'stopped'), ('product_id', '9802'), ('pay_start_date', '2020-01-01'),
                             ('website_storage_limit', '100'), ('price', '99')):
            self.redis.hset(self.key, field, value)
            with self.subTest(field=field), self.assertRaises(hosting.ProvisionConflict):
                self.provision()
            self.assertEqual(value.encode(), self.redis.hget(self.key, field))
            self.redis.hset(self.key, mapping=before)

    def test_expired_or_future_first_delivery_does_not_activate(self):
        for days in (-60, 10):
            start = date.today() + timedelta(days=days)
            with self.assertRaises(hosting.ProvisionConflict):
                self.provision(dict(self.data, period_start=start.isoformat(),
                                    period_end=(start + timedelta(days=30)).isoformat()))
        self.assertFalse(self.redis.exists(self.key))

    def test_failed_watch_preserves_competing_state(self):
        with self.redis.pipeline() as pipe:
            original_multi = pipe.multi
            def competing_change():
                self.redis.hset(self.key, 'status', 'stopped')
                return original_multi()
            with patch.object(self.redis, 'pipeline', return_value=pipe), patch.object(pipe, 'multi', side_effect=competing_change):
                with self.assertRaises(hosting.ProvisionConflict):
                    self.provision()
        self.assertEqual({b'status': b'stopped'}, self.redis.hgetall(self.key))

    def test_lost_response_retries_original_commit(self):
        self.provision()  # Simulate a committed response lost in transit.
        before = self.redis.hgetall(self.key)
        self.assertEqual('recorded', self.provision()['status'])
        self.assertEqual(before, self.redis.hgetall(self.key))


if __name__ == '__main__':
    unittest.main()
