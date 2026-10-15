import json
import unittest
from decimal import Decimal
from unittest import mock
from sqlalchemy import create_engine, select
from redis.exceptions import WatchError
import lanying_gift_quota as gifts


class QuotaFeatureFlagTest(unittest.TestCase):
    def test_gift_and_purchase_features_default_to_enabled(self):
        with mock.patch.dict(gifts.os.environ, {}, clear=True):
            self.assertTrue(gifts.gift_enabled())
            self.assertTrue(gifts.purchases_enabled())
            self.assertTrue(gifts.enabled())

    def test_explicit_overrides_still_control_each_feature(self):
        with mock.patch.dict(gifts.os.environ, {
                'SEENICAL_GIFT_QUOTA_ENABLED': 'off',
                'SEENICAL_MESSAGE_CREDITS_ENABLED': 'on'}, clear=True):
            self.assertFalse(gifts.gift_enabled())
            self.assertTrue(gifts.purchases_enabled())
            self.assertTrue(gifts.enabled())
        with mock.patch.dict(gifts.os.environ, {
                'SEENICAL_GIFT_QUOTA_ENABLED': 'true',
                'SEENICAL_MESSAGE_CREDITS_ENABLED': 'false'}, clear=True):
            self.assertTrue(gifts.gift_enabled())
            self.assertFalse(gifts.purchases_enabled())
        with mock.patch.dict(gifts.os.environ, {
                'SEENICAL_GIFT_QUOTA_ENABLED': 'off',
                'SEENICAL_MESSAGE_CREDITS_ENABLED': 'off'}, clear=True):
            self.assertFalse(gifts.enabled())


class FakeRedis:
    def __init__(self):
        self.hashes, self.values = {}, {}
        self.in_multi, self.commands = False, []
    def hset(self, key, field=None, value=None, mapping=None):
        if self.in_multi:
            self.commands.append(('hset', (key, field, value, mapping))); return
        self.hashes.setdefault(key, {}).update(mapping or {field: value})
    def hdel(self, key, field):
        existed = field in self.hashes.get(key,{})
        self.hashes.get(key, {}).pop(field, None)
        return int(existed)
    def hget(self,key,field):
        if self.in_multi: self.commands.append(('hget', (key, field))); return self
        return self.hashes.get(key,{}).get(field)
    def hsetnx(self,key,field,value):
        if field in self.hashes.get(key,{}): return 0
        self.hset(key,field,value); return 1
    def hgetall(self, key): return self.hashes.get(key, {})
    def hincrby(self, key, field, amount):
        value = int(self.hget(key, field) or 0) + amount
        self.hset(key, field, value); return value
    def hincrbyfloat(self, key, field, amount):
        value = float(self.hget(key, field) or 0) + amount
        self.hset(key, field, value); return value
    def hscan(self, key, cursor=0, count=100): return 0, self.hashes.get(key, {}).copy()
    def exists(self, key):
        if self.in_multi: self.commands.append(('exists', (key,))); return self
        return key in self.values
    def setex(self, key, seconds, value):
        if self.in_multi: self.commands.append(('setex', (key, seconds, value)))
        else: self.values[key] = value
    def pipeline(self): return self
    def __enter__(self): return self
    def __exit__(self, *args): self.in_multi = False; self.commands = []
    def watch(self, *args): pass
    def multi(self): self.in_multi = True
    def execute(self):
        commands=self.commands; self.commands=[]; self.in_multi=False
        return [getattr(self, method)(*args) for method, args in commands]


class GiftQuotaTest(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite://')
        gifts.metadata.create_all(self.engine)
        self.engine_patch=mock.patch.object(gifts,'engine',return_value=self.engine)
        self.flag=mock.patch.object(gifts,'enabled',return_value=True)
        self.gift_flag=mock.patch.object(gifts,'gift_enabled',return_value=True)
        self.gift_flag.start(); self.addCleanup(self.gift_flag.stop)
        # This suite exercises gift-only accounting; purchased-credit accounting has its own suite.
        self.purchase_flag=mock.patch.object(gifts,'purchases_enabled',return_value=False)
        self.purchase_flag.start(); self.addCleanup(self.purchase_flag.stop)
        self.engine_patch.start(); self.flag.start()
        self.addCleanup(self.engine_patch.stop); self.addCleanup(self.flag.stop)
        self.addCleanup(self.engine.dispose)
    def payload(self, **kwargs):
        value=dict(usage_id='use-1',app_id='app-1',quota=3,keys=['tenant-month','app-month'],
                   api_key_type='share',occurred_epoch=101,counters={'message_count':1},origin=None)
        value.update(kwargs); return value
    def capsule_payload(self):
        field = json.dumps({'capsule_id':'capsule-1', 'app_id':'app-1', 'product_id':7002,
                            'api_key_type':'share', 'day':'2026-09-30'})
        return self.payload(capsule_statistics=[{'key':'capsule-month', 'field':field,
            'app_ids_key':'capsule-owners', 'capsule_app_id':'owner-app'}])
    def assert_capsule_statistics(self, redis, payload, quota=3, count=1):
        entry = payload['capsule_statistics'][0]
        self.assertEqual(Decimal(quota), Decimal(redis.hget(entry['key'], entry['field'])))
        self.assertEqual(count, int(redis.hget(entry['app_ids_key'], entry['capsule_app_id'])))
    def test_grant_retry_does_not_extend_or_change_recipient(self):
        a=gifts.grant('seenical-invitation:1','app-1',10,now=100)
        b=gifts.grant('seenical-invitation:1','app-1',10,now=500)
        self.assertEqual(a,b)
        with self.assertRaises(ValueError): gifts.grant('seenical-invitation:1','app-2',10,now=500)
        with self.assertRaises(ValueError): gifts.grant('seenical-invitation:1','app-1',11,now=500)
    def test_priority_and_idempotent_usage(self):
        gifts.grant('seenical-invitation:1','app-1',5,now=100)
        self.assertEqual(3,gifts.settle(self.payload())['gift_used'])
        self.assertEqual(3,gifts.settle(self.payload())['gift_used'])
        self.assertEqual(2,gifts.status('app-1',now=102)['remaining'])
        self.assertEqual(2,gifts.settle(self.payload(usage_id='use-2',quota=4))['gift_used'])
        self.assertEqual(0,gifts.status('app-1',now=102)['remaining'])
        self.assertEqual(2,gifts.billable_used('tenant-month',7))
    def test_single_use_can_exceed_gift_without_negative_credit(self):
        gifts.grant('seenical-invitation:1','app-1',1,now=100)
        self.assertEqual(1,gifts.settle(self.payload(quota=30))['gift_used'])
        self.assertEqual(0,gifts.status('app-1',now=102)['remaining'])
        self.assertEqual(29,gifts.billable_used('app-month',30))
    def test_other_apps_cannot_use_credit_but_shared_net_usage_is_correct(self):
        gifts.grant('seenical-invitation:1','app-1',10,now=100)
        self.assertEqual(0,gifts.settle(self.payload(app_id='app-2'))['gift_used'])
        self.assertEqual(10,gifts.status('app-1',now=102)['remaining'])
        self.assertEqual(3,gifts.billable_used('tenant-month',3))
    def test_expiry_does_not_remove_historical_discount(self):
        gifts.grant('seenical-invitation:1','app-1',10,now=100)
        gifts.settle(self.payload())
        self.assertEqual(0,gifts.status('app-1',now=100+30*86400)['remaining'])
        self.assertEqual(0,gifts.billable_used('app-month',3))
        result=gifts.settle(self.payload(usage_id='use-2',occurred_epoch=100+30*86400))
        self.assertEqual(0,result['gift_used'])
    def test_cross_cycle_only_discounts_the_correct_cycle(self):
        gifts.grant('seenical-invitation:1','app-1',10,now=100)
        gifts.settle(self.payload(keys=['month-a']))
        gifts.settle(self.payload(usage_id='use-2',keys=['month-b']))
        self.assertEqual(0,gifts.billable_used('month-a',3))
        self.assertEqual(0,gifts.billable_used('month-b',3))
        self.assertEqual(4,gifts.status('app-1',now=200)['remaining'])
    def test_network_ack_retry_does_not_reincrement_statistics(self):
        gifts.grant('seenical-invitation:1','app-1',5,now=100)
        redis=FakeRedis()
        payload=self.capsule_payload()
        with mock.patch.object(gifts,'mark_applied',side_effect=RuntimeError('lost ack')):
            with self.assertRaises(RuntimeError): gifts.apply_statistics(redis,payload)
        self.assertEqual(3,redis.hashes['app-month']['message_count_quota'])
        self.assert_capsule_statistics(redis,payload)
        self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assertFalse(gifts.apply_statistics(redis,payload))
        self.assert_capsule_statistics(redis,payload)
        self.assertEqual(3,redis.hashes['app-month']['message_count_quota'])
        self.assertEqual(2,gifts.status('app-1',now=200)['remaining'])
        self.assertFalse(redis.hashes[gifts.PENDING_KEY])
    def test_database_failure_retries_quota_and_income_together(self):
        redis=FakeRedis(); payload=self.capsule_payload(); callback=mock.Mock()
        with mock.patch.object(gifts,'settle',side_effect=RuntimeError('database unavailable')):
            with self.assertRaises(RuntimeError): gifts.apply_statistics(redis,payload)
        self.assertFalse(redis.hgetall('app-month'))
        self.assertFalse(redis.hgetall('capsule-month'))
        gifts.retry_pending(redis,on_applied=callback)
        self.assertEqual(3,redis.hget('app-month','message_count_quota'))
        self.assert_capsule_statistics(redis,payload)
        callback.assert_called_once_with('app-1')
    def test_redis_failure_before_exec_does_not_leave_partial_statistics(self):
        redis=FakeRedis(); payload=self.capsule_payload()
        with mock.patch.object(redis,'execute',side_effect=RuntimeError('network unavailable')):
            with self.assertRaises(RuntimeError): gifts.apply_statistics(redis,payload)
        self.assertFalse(redis.hgetall('app-month'))
        self.assertFalse(redis.hgetall('capsule-month'))
        self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assert_capsule_statistics(redis,payload)
    def test_lost_exec_reply_does_not_duplicate_capsule_income(self):
        redis=FakeRedis(); payload=self.capsule_payload(); original=redis.execute
        def lost_reply():
            original(); raise RuntimeError('reply lost')
        with mock.patch.object(redis,'execute',side_effect=lost_reply):
            with self.assertRaises(RuntimeError): gifts.apply_statistics(redis,payload)
        self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assertEqual(3,redis.hget('app-month','message_count_quota'))
        self.assert_capsule_statistics(redis,payload)
    def test_watch_conflict_recalculates_capsule_statistics(self):
        redis=FakeRedis(); payload=self.capsule_payload(); entry=payload['capsule_statistics'][0]
        original=redis.execute; conflicted=False
        def competing_write():
            nonlocal conflicted
            if not conflicted:
                conflicted=True
                redis.hashes[entry['key']]={entry['field']:'7'}
                redis.hashes[entry['app_ids_key']]={entry['capsule_app_id']:1}
                raise WatchError()
            return original()
        with mock.patch.object(redis,'execute',side_effect=competing_write):
            self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assertEqual(3,redis.hget('app-month','message_count_quota'))
        self.assert_capsule_statistics(redis,payload,quota=10,count=2)
    def test_invalid_income_counter_is_detected_before_exec(self):
        redis=FakeRedis(); payload=self.capsule_payload(); entry=payload['capsule_statistics'][0]
        redis.hset(entry['key'],entry['field'],'not-numeric')
        with self.assertRaises(Exception): gifts.apply_statistics(redis,payload)
        self.assertFalse(redis.hgetall('app-month'))
        self.assertFalse(redis.exists('seenical:gift_quota:applied:'+payload['usage_id']))
        redis.hset(entry['key'],entry['field'],'0')
        self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assert_capsule_statistics(redis,payload)
    def test_shared_capsule_counter_updates_accumulate(self):
        redis=FakeRedis(); payload=self.capsule_payload()
        payload['capsule_statistics'] *= 2
        self.assertTrue(gifts.apply_statistics(redis,payload))
        self.assertEqual(3,redis.hget('app-month','message_count_quota'))
        self.assert_capsule_statistics(redis,payload,quota=6,count=2)
    def test_tampered_settlement_is_rejected(self):
        gifts.settle(self.payload())
        with self.assertRaises(ValueError): gifts.settle(self.payload(quota=4))
    def test_fractional_usage(self):
        gifts.grant('seenical-invitation:1','app-1',1,now=100)
        self.assertEqual(.5,gifts.settle(self.payload(quota=.5))['gift_used'])
        self.assertEqual(.5,gifts.status('app-1',now=200)['remaining'])
    def test_feature_off_never_accesses_mysql(self):
        with mock.patch.object(gifts,'enabled',return_value=False), mock.patch.object(gifts,'engine',side_effect=AssertionError):
            self.assertEqual(10,gifts.billable_used('key',10))
            self.assertIsNone(gifts.status('app-1')['remaining'])
    def test_failed_transaction_rolls_back_consumption(self):
        gifts.grant('seenical-invitation:1','app-1',10,now=100)
        with mock.patch.object(gifts,'insert',side_effect=RuntimeError('offset failed')):
            with self.assertRaises(RuntimeError): gifts.settle(self.payload())
        self.assertEqual(10,gifts.status('app-1',now=200)['remaining'])
        with self.engine.connect() as conn:
            self.assertEqual([],conn.execute(select(gifts.usage)).all())


if __name__=='__main__': unittest.main()
