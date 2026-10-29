import ast
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from redis import Redis
from flask import Flask, request, make_response

import lanying_billing_hosting as hosting
import lanying_billing_plan_quotas as quotas
from test_billing_hosting import terms


class QuotaContractTest(unittest.TestCase):
    def test_authentication_and_rejections(self):
        path = Path(__file__).resolve().parents[1] / 'services/grow_ai_service.py'
        node = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == 'billing_plan_quotas')
        node.decorator_list = []
        scope = dict(request=request, make_response=make_response,
                     check_access_token_valid=lambda: request.headers.get('access-token') == 'local')
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), scope)
        app = Flask(__name__)
        app.add_url_rule('/quota', view_func=scope['billing_plan_quotas'], methods=['POST'])
        client = app.test_client()
        with patch.object(quotas, 'apply', return_value={'status': 'applied'}) as call:
            self.assertEqual(401, client.post('/quota', json={}).status_code)
            call.assert_not_called()
            self.assertEqual(200, client.post('/quota', json={}, headers={'access-token': 'local'}).status_code)
            call.side_effect = quotas.QuotaConflict('conflict')
            self.assertEqual(409, client.post('/quota', json={}, headers={'access-token': 'local'}).status_code)
            call.side_effect = ValueError('bad')
            self.assertEqual(400, client.post('/quota', json={}, headers={'access-token': 'local'}).status_code)


@unittest.skipUnless(os.environ.get('BILLING_TEST_LOCAL_REDIS') == '1', 'disposable localhost Redis only')
class QuotaRedisTest(unittest.TestCase):
    def setUp(self):
        self.redis = Redis(host='127.0.0.1', port=36379)
        self.redis.ping()
        self.hosting = terms()
        hosting.provision(self.hosting, redis=self.redis)
        self.key = 'embedding:app_config:' + self.hosting['app_id']
        self.data = dict(app_id=self.hosting['app_id'], order_id=self.hosting['order_id'], order_sequence=1,
                         environment='TEST', knowledge_storage_mb=5, knowledge_payg=0,
                         hosting_storage_mb=50, hosting_traffic_gb='0.5', previous_knowledge_product_id=None)

    def tearDown(self):
        self.redis.delete(self.key, 'lanying-connector:grow_ai:service_status:' + self.data['app_id'])
        self.redis.close()

    def apply(self, **updates):
        return quotas.apply(dict(self.data, **updates), main=self.redis, stack=self.redis)

    def test_apply_replay_preserves_unrelated_settings_and_usage(self):
        self.redis.hset(self.key, mapping={'lanying_connector.auto_recharge': 'false', 'custom_setting': 'kept'})
        receipt = self.apply()
        self.assertEqual('applied', receipt['status'])
        state = self.redis.hgetall(self.key)
        self.assertEqual(b'5', state[b'lanying_connector.storage_limit'])
        self.assertEqual(b'50', state[b'lanying_connector.grow_ai_website_storage_limit'])
        self.assertEqual(b'false', state[b'lanying_connector.auto_recharge'])
        self.assertEqual(b'kept', state[b'custom_setting'])
        self.assertEqual(receipt, self.apply())
        self.assertEqual(state, self.redis.hgetall(self.key))

    def test_replay_changed_terms_or_modified_actual_quota_rejected(self):
        self.apply()
        with self.assertRaises(quotas.QuotaConflict):
            self.apply(knowledge_storage_mb=9)
        self.redis.hset(self.key, 'lanying_connector.storage_limit', 3)
        with self.assertRaises(quotas.QuotaConflict):
            self.apply()
        self.assertEqual(b'3', self.redis.hget(self.key, 'lanying_connector.storage_limit'))

    def test_newer_order_fences_old_delivery(self):
        self.apply()
        newer = dict(self.hosting, order_id=uuid.uuid4().hex, order_sequence=2)
        hosting.provision(newer, redis=self.redis)
        self.apply(order_id=newer['order_id'], order_sequence=2, knowledge_storage_mb=10)
        with self.assertRaises(quotas.QuotaConflict):
            self.apply()
        self.assertEqual(b'10', self.redis.hget(self.key, 'lanying_connector.storage_limit'))

    def test_main_period_failure_leaves_stack_untouched(self):
        self.redis.hset('lanying-connector:grow_ai:service_status:' + self.data['app_id'], 'billing_writer', 'LEGACY')
        with self.assertRaises(quotas.QuotaConflict):
            self.apply()
        self.assertFalse(self.redis.exists(self.key))

    def test_test_purchase_cannot_replace_live_knowledge(self):
        self.redis.hset(self.key, 'lanying_connector.storage_limit', 10)
        with self.assertRaises(quotas.QuotaConflict):
            self.apply(previous_knowledge_product_id=9001)
        self.assertEqual(b'10', self.redis.hget(self.key, 'lanying_connector.storage_limit'))
        self.redis.hset(self.key, 'lanying_connector.storage_limit', 5)
        with self.assertRaises(quotas.QuotaConflict):
            self.apply(previous_knowledge_product_id=9001)
        self.apply(previous_knowledge_product_id=9051)

    def test_live_purchase_can_replace_verified_test(self):
        self.apply()
        newer = dict(self.hosting, order_id=uuid.uuid4().hex, order_sequence=2, environment='LIVE')
        hosting.provision(newer, redis=self.redis)
        self.apply(order_id=newer['order_id'], order_sequence=2, environment='LIVE', knowledge_storage_mb=20)
        self.assertEqual(b'LIVE', self.redis.hget(self.key, 'billing_plan_quota.environment'))

    def test_invalid_fields_no_writes(self):
        for updates in ({'knowledge_storage_mb': -1}, {'knowledge_payg': True}, {'order_sequence': True},
                        {'previous_knowledge_product_id': 7001}, {'hosting_traffic_gb': 'NaN'}, {'extra': 1}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                self.apply(**updates)
        self.assertFalse(self.redis.exists(self.key))

    def test_remote_failure_is_visible_and_same_order_recovers(self):
        with patch.object(self.redis, 'pipeline', side_effect=ConnectionError('offline')):
            with self.assertRaises(ConnectionError):
                self.apply()
        self.assertFalse(self.redis.exists(self.key))
        self.assertEqual('applied', self.apply()['status'])


if __name__ == '__main__':
    unittest.main()
