"""Admission only; execute actual functions without loading unrelated AI vendors."""
import time
import unittest
from unittest.mock import Mock
from test_billing_plan_periods import functions

class AutomaticCreditsTest(unittest.TestCase):
    def setUp(self):
        self.redis=Mock();self.redis.hincrby.return_value=0
        self.redis.hgetall.return_value={b'message_count_quota':b'30'}
        self.gifts=Mock();self.gifts.purchases_enabled.return_value=True;self.gifts.enabled.return_value=True
        self.gifts.billable_used.return_value=30;self.gifts.status.return_value={'remaining':0,'expires_epoch':None}
        self.credits=Mock();self.credits.auto_recharge.return_value=True;self.credits.remaining.return_value=0;self.credits.has_preference.return_value=False
        self.config={'product_id':7002,'message_per_month':30,'_quota_app_id':'app','billing_order_id':'order','billing_period_end_epoch':time.time()+1000}
        self.notify=Mock(return_value=True)
        self.namespace=functions('services/openai_service.py',{'_has_message_quota_remaining','get_message_limit_state'},
            dict(time=time,lanying_gift_quota=self.gifts,lanying_message_credits=self.credits,notify_butler=self.notify,
                 lanying_config=Mock(get_lanying_connector=Mock(return_value=self.config)),
                 lanying_redis=Mock(get_redis_connection=Mock(return_value=self.redis)),
                 get_message_statistic_keys=lambda *_:['key'],_gross_quota=lambda *_:30,get_message_quota_period=lambda _:{}))
    def allowed(self,used=30,gifts=0):return self.namespace['_has_message_quota_remaining'](self.redis,'key',self.config,used,gifts)
    def test_managed_service_funds_before_provider_call(self):
        self.assertFalse(self.allowed());self.notify.assert_called_once_with('app','message_count_quota_reached',{})
        self.credits.remaining.return_value=1000;self.assertTrue(self.allowed())
    def test_off_or_expired_never_triggers_purchase(self):
        self.credits.auto_recharge.return_value=False;self.assertFalse(self.allowed());self.notify.assert_not_called()
        self.credits.auto_recharge.return_value=True;self.config['billing_period_end_epoch']=1;self.assertFalse(self.allowed());self.notify.assert_not_called()
    def test_positive_allowance_or_gift_does_not_purchase(self):
        self.assertTrue(self.allowed(29));self.assertTrue(self.allowed(30,1));self.notify.assert_not_called()
    def test_managed_service_has_no_legacy_buffer_when_purchase_feature_disabled(self):
        self.gifts.purchases_enabled.return_value=False;self.assertFalse(self.allowed());self.notify.assert_not_called()
        self.config.pop('billing_order_id');self.assertTrue(self.allowed())
    def test_need_flag_tracks_discounts_and_app_owned_credits(self):
        state=self.namespace['get_message_limit_state']('app');self.assertTrue(state['automatic_purchase_needed'])
        self.gifts.billable_used.return_value=29.5;self.assertFalse(self.namespace['get_message_limit_state']('app')['automatic_purchase_needed'])
        self.gifts.billable_used.return_value=30;self.gifts.status.return_value={'remaining':1,'expires_epoch':None}
        self.assertFalse(self.namespace['get_message_limit_state']('app')['automatic_purchase_needed'])
