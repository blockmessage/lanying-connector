import unittest
from unittest import mock

from sqlalchemy import create_engine, select
from test_gift_quota import FakeRedis
import lanying_gift_quota as credits
import lanying_quota_simulation as simulation


class QuotaSimulationTest(unittest.TestCase):
    def setUp(self):
        self.db = create_engine('sqlite://')
        credits.metadata.create_all(self.db)
        self.addCleanup(self.db.dispose)
        self.redis = FakeRedis()
        self.service = mock.Mock()
        self.service.lanying_config.mode = 'etcd'
        self.config = dict(product_id=7001, tenant_id=42, message_per_month=30)
        self.service.lanying_config.get_lanying_connector.return_value = self.config
        self.service.lanying_redis.get_redis_connection.return_value = self.redis
        self.service.get_message_statistic_keys.return_value = ['tenant-period', 'app-period']
        self.service.get_message_limit_state.return_value = {'test': 'state'}
        self.service.check_message_deduct_failed.return_value = {'result': 'ok'}
        self.service.check_deduct_message_quota.return_value = {'result': 'ok'}
        self.service._record_quota_statistics.side_effect = lambda redis, app, cfg, quota, key_type, response, origin: credits.apply_statistics(
            redis, response['_seenical_quota_settlement'][0])
        for patch in (mock.patch.object(credits, 'engine', return_value=self.db),
                      mock.patch.object(credits, 'gift_enabled', return_value=True),
                      mock.patch.object(credits, 'purchases_enabled', return_value=True),
                      mock.patch.object(simulation, '_service', return_value=self.service),
                      mock.patch.object(simulation.time, 'time', return_value=110)):
            patch.start()
            self.addCleanup(patch.stop)

    def run_simulation(self, quota=50, test_id='test-1', app='app'):
        return simulation.simulate_message_quota(app, quota, test_id)

    def test_real_accounting_gift_then_package_then_purchase_and_repeat(self):
        credits.grant('seenical-invitation:1', 'app', 10, now=100)
        credits.grant_purchase('seenical-message-credit:' + 'a'*32, 'app', 100, 200, 'manual', 100)
        result = self.run_simulation()
        self.assertEqual('completed', result['status'])
        with self.db.connect() as conn:
            row = conn.execute(select(credits.usage)).mappings().one()
        self.assertEqual(10, row['gift_used'])
        self.assertEqual(10, row['purchase_used'])
        self.assertEqual(90, credits.status('app', now=120, purchased=True)['remaining'])
        self.assertEqual(30, credits.billable_used('tenant-period', 50, self.redis))
        self.assertEqual('already_completed', self.run_simulation()['status'])
        self.assertEqual(50, self.redis.hget('app-period', 'message_count_quota'))
        self.service._record_quota_statistics.assert_called_once()

    def test_same_id_cannot_change_amount(self):
        self.run_simulation()
        with self.assertRaisesRegex(ValueError, 'different arguments'):
            self.run_simulation(quota=51)

    def test_new_id_is_new_usage_and_app_ids_are_isolated(self):
        first = self.run_simulation()
        second = self.run_simulation(test_id='test-2')
        third = self.run_simulation(app='other')
        self.assertEqual(3, len({x['usage_id'] for x in (first, second, third)}))

    def test_lost_final_ack_reuses_frozen_payload(self):
        original = credits.mark_applied
        with mock.patch.object(credits, 'mark_applied', side_effect=RuntimeError('lost ack')):
            with self.assertRaisesRegex(RuntimeError, 'lost ack'):
                self.run_simulation()
        with mock.patch.object(simulation.time, 'time', return_value=150), \
                mock.patch.object(credits, 'mark_applied', side_effect=original):
            self.assertEqual('completed', self.run_simulation()['status'])
        self.assertEqual(50, self.redis.hget('app-period', 'message_count_quota'))
        self.service.check_deduct_message_quota.assert_called_once()

    def test_rejections_do_not_settle(self):
        for check in (self.service.check_message_deduct_failed, self.service.check_deduct_message_quota):
            check.return_value = {'result': 'error', 'code': 'quota_denied'}
            self.assertEqual('rejected', self.run_simulation()['status'])
            self.service._record_quota_statistics.assert_not_called()
            check.return_value = {'result': 'ok'}

    def test_invalid_inputs_do_not_load_runtime(self):
        for quota in (0, -1, True, 'NaN', 'Infinity', 10001):
            with self.assertRaises(ValueError):
                self.run_simulation(quota=quota)
        with self.assertRaises(ValueError):
            self.run_simulation(test_id='')
        simulation._service.assert_not_called()

    def test_missing_app_or_durable_accounting_rejected(self):
        with mock.patch.object(credits, 'purchases_enabled', return_value=False):
            with self.assertRaises(ValueError): self.run_simulation()
        self.service.lanying_config.mode = 'env'
        with self.assertRaises(ValueError): self.run_simulation()
        self.service.lanying_config.mode = 'etcd'
        self.service.lanying_config.get_lanying_connector.return_value = None
        with self.assertRaises(ValueError): self.run_simulation()
        self.service._record_quota_statistics.assert_not_called()


if __name__ == '__main__':
    unittest.main()
