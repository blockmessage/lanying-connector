import ast
import copy
import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock
from sqlalchemy import create_engine
import lanying_stripe_entitlements as stripe
from test_gift_quota import FakeRedis


class Cache(FakeRedis):
    def get(self, key):
        return self.values.get(key)
    def set(self, key, value, ex=None):
        if self.in_multi:
            self.commands.append(('set', (key, value, ex)))
        else:
            self.values[key] = value


def extract(name):
    tree = ast.parse(Path('services/openai_service.py').read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    scope = {'datetime': datetime}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), 'openai_service.py', 'exec'), scope)
    return scope[name]


class StripeEntitlementTest(unittest.TestCase):
    def setUp(self):
        self.db = create_engine('sqlite://')
        stripe.metadata.create_all(self.db)
        self.redis = Cache()
        for patch in (mock.patch.object(stripe.lanying_gift_quota, 'engine', return_value=self.db),
                      mock.patch.object(stripe.lanying_agent_tools_storage, 'is_enabled', return_value=True),
                      mock.patch.object(stripe.lanying_redis, 'get_redis_connection', return_value=self.redis)):
            patch.start(); self.addCleanup(patch.stop)
        self.addCleanup(self.db.dispose)
        self.value = dict(app_id='app', environment='test', revision=1, status='active', subscription_id='sub_test',
                          period_start=1793610000, period_end=1796202000, product_id=7003,
                          message_allowance=3000, storage_limit=10, hosting_product_id=9801,
                          website_storage_limit=100, website_traffic_limit=1, max_domain_num=1)

    def test_repeat_and_old_projection_cannot_reset_entitlements(self):
        self.assertTrue(stripe.apply(self.value)['applied'])
        self.assertTrue(stripe.apply(self.value)['applied'])
        newer = dict(self.value, revision=2, message_allowance=4000)
        stripe.apply(newer)
        self.assertFalse(stripe.apply(self.value)['applied'])
        self.assertEqual(4000, stripe.state('app')['message_allowance'])
        self.assertEqual(4000, stripe._cache(self.redis, 'app', self.value)['message_allowance'])
        self.assertEqual(4000, stripe._cache(self.redis, 'app', None)['message_allowance'])

    def test_identity_and_revision_collision_are_rejected(self):
        stripe.apply(self.value)
        for change in ({'environment':'live','revision':2}, {'message_allowance':4000}):
            with self.assertRaises(ValueError): stripe.apply(dict(self.value, **change))
        self.assertEqual(self.value, stripe.state('app'))

    def test_invalid_amount_never_persists(self):
        for field, value in [('message_allowance', -1), ('website_traffic_limit', float('nan')), ('revision', True)]:
            with self.assertRaises(ValueError): stripe.apply(dict(self.value, **{field:value}))
        self.assertIsNone(stripe.state('app'))

    def test_other_apps_and_original_model_policy_are_preserved(self):
        original = {'model': 'unchanged', 'message_per_month':30, 'product_id':7001, 'enabled':True}
        stripe.apply(self.value)
        self.assertEqual(original, stripe.overlay('other', original))
        result = stripe.overlay('app', original)
        self.assertEqual('unchanged', result['model'])
        self.assertEqual(3000, result['message_per_month'])
        self.assertEqual(30, original['message_per_month'])
        self.assertEqual(0, stripe.config_limit('app','lanying_connector.storage_payg'))

    def test_subscription_period_and_keys_do_not_roll_at_calendar_month(self):
        config = {'stripe_billing':self.value}
        keys = extract('get_message_statistic_keys')(config, 'app')
        self.assertIn('sub_test', keys[0]); self.assertIn(str(self.value['period_start']), keys[0])
        period = extract('get_message_quota_period')(config)
        self.assertEqual(self.value['period_end'], period['expires_epoch'])
        other = copy.deepcopy(config); other['stripe_billing']['period_start'] += 1
        self.assertNotEqual(keys[0], extract('get_message_statistic_keys')(other, 'app')[0])

    def test_usage_returns_actual_prorated_allowance(self):
        import types
        fn=extract('get_message_limit_state')
        fn.__globals__.update(lanying_config=types.SimpleNamespace(get_lanying_connector=lambda app: {'stripe_billing':self.value,'message_per_month':6500.1234}),
            lanying_redis=types.SimpleNamespace(get_redis_connection=lambda: self.redis),
            get_message_statistic_keys=lambda *args:['usage'],get_message_quota_period=lambda config:{},
            lanying_gift_quota=types.SimpleNamespace(enabled=lambda:False))
        self.redis.hgetall=lambda key:{}
        self.assertEqual(6500.1234,fn('app')['message_count_allowance'])

    def test_pause_and_exact_expiry_block_ai_not_other_apps(self):
        config = {'stripe_billing':self.value}
        self.assertFalse(stripe.paused(config, self.value['period_end']-1))
        self.assertTrue(stripe.paused(config, self.value['period_end']))
        self.assertTrue(stripe.paused({'stripe_billing':dict(self.value,status='paused')},0))
        self.assertFalse(stripe.paused({}))

    def test_hosting_recovers_without_legacy_redis_and_uses_stripe_cycle(self):
        stripe.apply(self.value)
        status = stripe.hosting('app', None)
        self.assertEqual(datetime.utcfromtimestamp(self.value['period_start']).strftime('%Y-%m-%d'), status['pay_start_date'])
        tree = ast.parse(Path('lanying_grow_ai.py').read_text())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_service_statistic_key_list')
        scope = {'datetime': datetime, 'get_service_status': lambda app: status}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'lanying_grow_ai.py', 'exec'), scope)
        key = scope['get_service_statistic_key_list']('app','website_traffic')[0]
        self.assertIn('sub_test',key)
        self.assertTrue(key.endswith(str(self.value['period_start'])))
        self.assertEqual(4,len(scope['get_service_statistic_key_list']('app','article_count')))

    def test_site_access_pause_and_infrastructure_failure_policy(self):
        stripe.apply(dict(self.value, status='paused'))
        self.redis.hget = lambda key, site: 'app'
        self.assertFalse(stripe.site_access('site', 'example.site.chatai101.com')['allowed'])
        with mock.patch.object(stripe, 'state', side_effect=RuntimeError('offline')):
            self.assertEqual('check_unavailable', stripe.site_access('site', 'example.site.chatai101.com')['reason'])
        self.redis.hget = lambda key, site: None
        self.assertFalse(stripe.site_access('missing', 'example.test')['allowed'])
        with self.assertRaises(ValueError): stripe.site_access('site', 'invalid/path')

    def test_downgrade_limits_custom_domains_without_deleting_sites(self):
        import types
        stripe.apply(self.value)
        self.redis.hget = lambda key, site: 'app'
        sites=[{'site_id':'first','domain_id':'domain1'},{'site_id':'second','domain_id':'domain2'}]
        grow=types.SimpleNamespace(get_site=lambda app, site: sites[0] if site=='first' else sites[1],
            get_custom_domain_info=lambda *args: {'domain_name':'custom.test'},
            get_site_list=lambda app: {'data':{'list':sites}})
        with mock.patch.dict('sys.modules', {'lanying_grow_ai':grow}), mock.patch.object(stripe,'paused',return_value=False):
            self.assertTrue(stripe.site_access('first','custom.test')['allowed'])
            self.assertFalse(stripe.site_access('second','custom.test')['allowed'])
            self.assertTrue(stripe.site_access('second','second.site.chatai101.com')['allowed'])
        self.assertEqual(2,len(sites))

    def test_cached_failure_recovers_from_mysql_without_regrant(self):
        with mock.patch.object(stripe, '_cache', side_effect=RuntimeError('redis offline')):
            with self.assertRaises(RuntimeError): stripe.apply(self.value)
        self.assertTrue(stripe.apply(self.value)['applied'])
        self.assertEqual(self.value, stripe.state('app'))
