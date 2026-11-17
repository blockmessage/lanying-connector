import ast
from pathlib import Path
from flask import Flask,request
import unittest
from datetime import datetime, timezone
from unittest import mock
from sqlalchemy import create_engine
import lanying_stripe_credits as stripe
import lanying_message_credits as messages
import lanying_gift_quota as quota
from test_gift_quota import FakeRedis

class StripeCreditTest(unittest.TestCase):
    def setUp(self):
        self.db=create_engine('sqlite://')
        stripe.metadata.create_all(self.db);quota.metadata.create_all(self.db)
        for patch in (mock.patch.object(quota,'engine',return_value=self.db),
                      mock.patch.object(quota,'purchases_enabled',return_value=True),
                      mock.patch.object(quota,'gift_enabled',return_value=True),
                      mock.patch.object(stripe.lanying_agent_tools_storage,'is_enabled',return_value=True)):
            patch.start();self.addCleanup(patch.stop)
        self.addCleanup(self.db.dispose)
    def epoch(self,value): return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())
    def test_calendar_month_clamps_and_retry_does_not_extend(self):
        for index,start,end in [(1,'2028-01-31T12:34:56','2028-02-29T12:34:56'),(2,'2027-01-31T12:34:56','2027-02-28T12:34:56'),(3,'2027-12-15T12:34:56','2028-01-15T12:34:56')]:
            identity='seenical-stripe-credit:'+str(index)*32
            result=stripe.grant(identity,'app',1000,'manual',self.epoch(start))
            self.assertEqual(self.epoch(end),result['expires_epoch'])
            self.assertEqual(result,stripe.grant(identity,'app',1000,'manual',self.epoch(start)+86400*40))
    def test_identity_mismatch_and_negative_rejected(self):
        identity='seenical-stripe-credit:'+'a'*32
        stripe.grant(identity,'app',1000,'auto')
        for app,points,source in [('other',1000,'auto'),('app',2000,'auto'),('app',1000,'manual'),('app',-1,'auto')]:
            with self.assertRaises(ValueError):stripe.grant(identity,app,points,source)
    def test_setting_revision_environment_and_old_wallet_isolation(self):
        stripe.set_preference('app','test',2,False)
        self.assertFalse(stripe.set_preference('app','test',1,True)['applied'])
        self.assertFalse(messages.auto_recharge('app',{'product_id':7003,'enable_extra_price':True}))
        stripe.set_preference('app','test',3,True)
        self.assertTrue(messages.auto_recharge('app',{'product_id':7001}))
        with self.assertRaises(ValueError):stripe.set_preference('app','live',4,True)
        with self.assertRaises(ValueError):stripe.set_preference('app','test',3,False)
    def test_calendar_credits_keep_existing_consumption_order(self):
        now=self.epoch('2027-01-31T00:00:00');redis=FakeRedis()
        quota.grant('seenical-invitation:1','app',10,now)
        stripe.grant('seenical-stripe-credit:'+'b'*32,'app',1000,'manual',now)
        result=quota.settle(dict(usage_id='one',app_id='app',quota=50,occurred_epoch=now+1,keys=['shared','app-period'],api_key_type='share',package_allowance=20),redis)
        self.assertEqual(10,result['gift_used']);self.assertEqual(20,result['purchase_used'])
        self.assertEqual(980,quota.status('app',now+1,purchased=True)['remaining'])
        self.assertEqual(0,quota.status('other',now+1,purchased=True)['remaining'])

    def test_admission_uses_stripe_consent_and_never_legacy_payg_buffer(self):
        tree=ast.parse(Path('services/openai_service.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_has_message_quota_remaining')
        notify=mock.Mock(return_value='processing')
        patch=mock.patch.object(stripe,'notify_purchase',notify);patch.start();self.addCleanup(patch.stop)
        ns={'lanying_gift_quota':quota,'lanying_message_credits':messages,'notify_butler':notify}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'quota_admission','exec'),ns)
        stripe.set_preference('app','test',1,True)
        for tier in (7001,7003):
            config={'_quota_app_id':'app','product_id':tier,'message_per_month':30,'enable_extra_price':True}
            self.assertFalse(ns['_has_message_quota_remaining'](FakeRedis(),'period',config,30,0))
        self.assertEqual(2,notify.call_count)
        with mock.patch.object(quota,'purchases_enabled',return_value=False):
            self.assertFalse(ns['_has_message_quota_remaining'](FakeRedis(),'period',config,30,0))
        self.assertEqual(2,notify.call_count)
        stripe.grant('seenical-stripe-credit:'+'c'*32,'app',1000,'auto')
        self.assertTrue(ns['_has_message_quota_remaining'](FakeRedis(),'period',config,30,0))

    def test_disabled_grant_capability_prevents_new_purchase(self):
        tree=ast.parse(Path('lanying_connector.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='stripe_credits');node.decorator_list=[]
        config=mock.Mock();config.get_lanying_connector.return_value={'product_id':7001,'message_per_month':30}
        service=mock.Mock();service.get_message_limit_state.return_value={}
        ns={'request':request,'accessToken':'test','lanying_config':config,'get_service_module':lambda _:service}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'credit_route','exec'),ns)
        app=Flask(__name__);app.add_url_rule('/credits/<operation>',view_func=ns['stripe_credits'],methods=['POST'])
        with mock.patch.object(quota,'purchases_enabled',return_value=False):
            result=app.test_client().post('/credits/state',json={'app_id':'app'},headers={'access-token':'test'})
        self.assertEqual(200,result.status_code);self.assertFalse(result.json['data']['purchase_enabled'])

    def test_operator_inspection_is_app_scoped_and_never_restores_consumed_points(self):
        identity='seenical-stripe-credit:'+'d'*32
        stripe.grant(identity,'app',1000,'manual')
        with self.db.begin() as conn:
            conn.execute(quota.grants.update().where(quota.grants.c.business_id==identity).values(remaining=700))
        self.assertEqual(300,stripe.inspect(identity,'app')['consumed'])
        self.assertEqual(700,stripe.inspect(identity,'app')['remaining'])
        with self.assertRaises(ValueError):stripe.inspect(identity,'other')
        with self.assertRaises(ValueError):stripe.inspect('seenical-invitation:1','app')


class StripeNotificationTest(unittest.TestCase):
    def setUp(self):
        self.values = {}
        self.redis = mock.Mock()
        def set_value(key, value, nx=False, ex=None):
            self.assertEqual(30, ex)
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True
        self.redis.set.side_effect = set_value
        self.redis.get.side_effect = lambda key: self.values.get(key)
        self.response = mock.Mock(ok=True)
        self.response.json.return_value = {'code': 200, 'data': {'credit_purchase_status': 'payment_required'}}
        for patch in (mock.patch.object(stripe.lanying_redis, 'get_redis_connection', return_value=self.redis),
                      mock.patch.object(stripe.requests, 'post', return_value=self.response)):
            patch.start(); self.addCleanup(patch.stop)

    def test_repeated_and_inflight_notifications_share_app_cooldown(self):
        def post(*args, **kwargs):
            self.assertEqual('processing', stripe.notify_purchase('app'))
            return self.response
        stripe.requests.post.side_effect = post
        for _ in range(10):
            self.assertEqual('payment_required', stripe.notify_purchase('app'))
        self.assertEqual(1, stripe.requests.post.call_count)
        self.values.clear()  # Redis TTL elapsed; the original order can be checked again.
        self.assertEqual('payment_required', stripe.notify_purchase('app'))
        self.assertEqual(2, stripe.requests.post.call_count)

    def test_timeout_is_unknown_and_not_reposted_during_cooldown(self):
        stripe.requests.post.side_effect = TimeoutError()
        for _ in range(10):
            self.assertEqual('unknown', stripe.notify_purchase('app'))
        self.assertEqual(1, stripe.requests.post.call_count)
        self.assertEqual('credit_payment_unconfirmed', stripe.purchase_error('unknown')['code'])
        self.assertNotEqual(stripe.purchase_error('unknown'), stripe.purchase_error('payment_required'))

    def test_old_response_and_redis_failure_do_not_claim_payment_success(self):
        self.response.json.return_value = {'code': 200, 'data': True}
        self.assertEqual('unknown', stripe.notify_purchase('app'))
        self.redis.set.side_effect = RuntimeError('offline')
        self.assertEqual('unknown', stripe.notify_purchase('other'))
        self.assertEqual(1, stripe.requests.post.call_count)

    def test_different_apps_do_not_share_cooldown(self):
        stripe.notify_purchase('one'); stripe.notify_purchase('two')
        self.assertEqual(2, stripe.requests.post.call_count)

    def test_timeout_but_delivered_credits_admits_without_waiting_for_cache(self):
        tree = ast.parse(Path('services/openai_service.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_has_message_quota_remaining')
        local_messages = mock.Mock()
        local_messages.auto_recharge.return_value = True
        local_messages.has_preference.return_value = True
        local_messages.remaining.return_value = 1000
        flags = mock.Mock(); flags.purchases_enabled.return_value = True
        ns = {'lanying_message_credits': local_messages, 'lanying_gift_quota': flags}
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'admission', 'exec'), ns)
        config = {'_quota_app_id': 'app', 'message_per_month': 30}
        with mock.patch.object(stripe, 'preference', return_value={'auto_recharge': True}), \
             mock.patch.object(stripe, 'notify_purchase', return_value='unknown'):
            self.assertTrue(ns['_has_message_quota_remaining'](FakeRedis(), 'key', config, 30, 0))
            local_messages.remaining.return_value = 0
            self.assertFalse(ns['_has_message_quota_remaining'](FakeRedis(), 'key', config, 30, 0))
            self.assertEqual('credit_payment_unconfirmed', config['_credit_purchase_error']['code'])

    def test_chat_api_and_tool_admission_return_payment_status_not_generic_quota(self):
        import lanying_stripe_entitlements
        tree = ast.parse(Path('services/openai_service.py').read_text())
        names = {'check_message_limit', 'check_deduct_message_quota'}
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        ns = {'lanying_redis': mock.Mock(), 'lanying_gift_quota': mock.Mock(),
              'get_message_statistic_keys': lambda *_: ['period'],
              '_gift_allowance': lambda *_: (30, 0),
              'check_daily_quota_fuse_limit': lambda *_: {'result': 'ok'}}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'admission_errors', 'exec'), ns)
        config = {'product_id': 7001, 'message_per_month': 30}
        with mock.patch.object(lanying_stripe_entitlements, 'paused', return_value=False):
            for status in ('processing', 'unknown', 'payment_required'):
                expected = stripe.purchase_error(status)
                def exhausted(redis, key, call_config, *args):
                    call_config['_credit_purchase_error'] = expected
                    return False
                ns['_has_message_quota_remaining'] = exhausted
                for chat in (True, False):
                    self.assertEqual(expected, ns['check_message_limit']('app', config, {'api_key_type': 'share'}, chat))
                self.assertEqual(expected, ns['check_deduct_message_quota']('app', config, 1))
                self.assertNotIn('_credit_purchase_error', config)
