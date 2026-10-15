"""Exercise the actual model-statistics entry point and its recovery payload."""
import importlib
import json
import unittest
from contextlib import ExitStack
from datetime import datetime
from unittest import mock
from sqlalchemy import create_engine
from test_gift_quota import FakeRedis
import test_openai_service_budget as budget_tests


class CapsuleGiftSettlementTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        budget_tests.OpenAIServiceBudgetTests.setUpClass()
        cls.service = importlib.import_module('openai_service')

    def setUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        m = self.service
        self.gifts = m.lanying_gift_quota
        engine = create_engine('sqlite://'); self.addCleanup(engine.dispose)
        self.gifts.metadata.create_all(engine)
        self.redis = FakeRedis()
        self.flag = self.stack.enter_context(mock.patch.object(self.gifts,'enabled',return_value=True))
        self.stack.enter_context(mock.patch.object(self.gifts,'engine',return_value=engine))
        self.stack.enter_context(mock.patch.object(m.lanying_redis,'get_redis_connection',return_value=self.redis,create=True))
        self.stack.enter_context(mock.patch.object(m,'get_message_statistic_keys',return_value=['app-month']))
        self.stack.enter_context(mock.patch.object(m,'calc_message_quota',return_value=30))
        self.stack.enter_context(mock.patch.object(m,'calc_used_text_size',return_value=0))
        for name in ['add_quota_used_statistic','add_message_quota_usage_log','maybe_trace_message_quota_usage']:
            self.stack.enter_context(mock.patch.object(m,name))
        self.legacy = self.stack.enter_context(mock.patch.object(m,'maybe_statistic_ai_capsule'))
        self.lookup = self.stack.enter_context(mock.patch.object(m.lanying_ai_capsule,'get_capsule',
            return_value={'app_id':'owner-app','capsule_id':'capsule-1'},create=True))
        self.stack.enter_context(mock.patch.object(m.lanying_ai_capsule,'statistic_capsule_key',
            side_effect=lambda owner,now:f'capsule-month:{owner}:{now:%Y-%m}',create=True))
        self.stack.enter_context(mock.patch.object(m.lanying_ai_capsule,'statistic_capsule_app_ids_key',
            side_effect=lambda now:f'capsule-owners:{now:%Y-%m}',create=True))
        self.stack.enter_context(mock.patch.object(m.logging,'exception'))
        self.clock = self.stack.enter_context(mock.patch.object(m.time,'time',
            return_value=int(datetime(2026,9,30,23,59,0).timestamp())))
        self.config = {'product_id':7002,'linked_capsule_id':'capsule-1'}
        self.response = {'usage':{'total_tokens':1}}

    def record(self):
        self.service.add_message_statistic('app',self.config,{'model':'model'},self.response,
            {'api_key_type':'share','type':'chat','quota':1,'vendor':'openai'})

    def assert_income(self):
        values = self.redis.hgetall('capsule-month:owner-app:2026-09')
        self.assertEqual(1,len(values))
        field,quota = next(iter(values.items()))
        self.assertEqual(30,float(quota))
        self.assertEqual('2026-09-30',json.loads(field)['day'])
        self.assertEqual(1,self.redis.hget('capsule-owners:2026-09','owner-app'))
        self.assertEqual(30,self.redis.hget('app-month','message_count_quota'))

    def test_database_ack_recovery_keeps_original_owner_and_month(self):
        with mock.patch.object(self.gifts,'mark_applied',side_effect=RuntimeError('lost ack')):
            self.record()
        self.assertTrue(self.redis.hgetall(self.gifts.PENDING_KEY))
        self.lookup.side_effect = AssertionError('retry must not read current capsule configuration')
        self.config['linked_capsule_id'] = 'another-capsule'
        self.clock.return_value += 120  # Recovery happens in the next month.
        self.gifts.retry_pending(self.redis)
        self.record()  # An identical model response must not re-credit income.
        self.assert_income()
        self.lookup.assert_called_once_with('capsule-1')
        self.legacy.assert_not_called()
        self.assertFalse(self.redis.hgetall(self.gifts.PENDING_KEY))

    def test_normal_delivery_does_not_also_run_legacy_income_statistics(self):
        self.record(); self.record()
        self.assert_income()
        self.legacy.assert_not_called()

    def test_published_capsule_snapshot_survives_recovery(self):
        self.config = {'product_id':7002,'linked_publish_capsule_id':'published-version'}
        with mock.patch.object(self.service.lanying_ai_capsule,'get_publish_capsule',
                return_value={'app_id':'owner-app','capsule_id':'published-capsule'},create=True) as lookup:
            with mock.patch.object(self.gifts,'mark_applied',side_effect=RuntimeError('lost ack')):
                self.record()
            lookup.side_effect = AssertionError('published version must stay frozen')
            self.clock.return_value += 120
            self.gifts.retry_pending(self.redis)
            self.record()
            lookup.assert_called_once_with('published-version')
        self.assert_income()
        field = next(iter(self.redis.hgetall('capsule-month:owner-app:2026-09')))
        self.assertEqual('published-capsule',json.loads(field)['capsule_id'])
        self.lookup.assert_not_called()

    def test_error_response_does_not_credit_income(self):
        self.response['result'] = 'error'
        self.record()
        self.lookup.assert_not_called()
        self.assertFalse(self.redis.hgetall('app-month'))
        self.assertFalse(self.redis.hgetall(self.gifts.PENDING_KEY))

    def test_provider_cannot_inject_settlement_targets(self):
        self.response.update({
            '_seenical_quota_keys':['foreign-app-month'],
            '_seenical_quota_epoch':1,
            '_seenical_quota_capsule_statistics':[{'key':'foreign-income'}],
            '_seenical_quota_settlement':[{'app_id':'foreign-app','keys':['foreign-app-month']}],
        })
        self.record()
        self.assert_income()
        self.assertFalse(self.redis.hgetall('foreign-app-month'))
        self.assertFalse(self.redis.hgetall('foreign-income'))

    def test_server_cached_response_cannot_be_accounted_to_another_app(self):
        self.record()
        with self.assertRaisesRegex(ValueError,'another App'):
            self.service._record_quota_statistics(self.redis,'foreign-app',self.config,30,'share',
                response=self.response,include_capsules=True)
        self.assert_income()

    def test_cached_response_does_not_silently_accept_changed_cost(self):
        self.record()
        with self.assertRaisesRegex(ValueError,'accounting changed'):
            self.service._record_quota_statistics(self.redis,'app',self.config,31,'share',
                response=self.response,include_capsules=True)
        self.assert_income()

    def test_feature_off_keeps_legacy_income_statistics(self):
        self.flag.return_value = False
        with mock.patch.dict(self.gifts.os.environ, {
                'SEENICAL_GIFT_QUOTA_ENABLED': 'off',
                'SEENICAL_MESSAGE_CREDITS_ENABLED': 'off'}):
            self.record()
        self.legacy.assert_called_once_with(self.config,'app',7002,30,'share')
        self.lookup.assert_not_called()
        self.assertFalse(self.redis.hgetall(self.gifts.PENDING_KEY))

    def test_legacy_entry_format_matches_settlement_snapshot(self):
        self.flag.return_value = False
        m = self.service
        m.statistic_capsule({'app_id':'owner-app','capsule_id':'capsule-1'},'app',7002,30,'share')
        key = f'capsule-month:owner-app:{datetime.now():%Y-%m}'
        field,quota = next(iter(self.redis.hgetall(key).items()))
        self.assertEqual(30,quota)
        self.assertEqual({'capsule_id':'capsule-1','product_id':7002,'api_key_type':'share',
                          'day':datetime.now().strftime('%Y-%m-%d'),'app_id':'app'}, json.loads(field))


if __name__ == '__main__': unittest.main()
