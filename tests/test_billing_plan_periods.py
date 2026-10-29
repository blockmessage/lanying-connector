"""Exercise production quota functions without starting unrelated AI integrations."""
import ast
from datetime import datetime, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta
import unittest


def functions(path, names, context):
    tree = ast.parse(Path(path).read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), path, 'exec'), context)
    return context


class PlanPeriodTest(unittest.TestCase):
    def setUp(self):
        self.context = functions('services/openai_service.py', {'get_message_quota_period', 'get_message_statistic_keys'},
                                 dict(datetime=datetime, relativedelta=relativedelta))
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.config = dict(product_id=7003, tenant_id=7, start_date=today.strftime('%Y-%m-%d'),
                           billing_order_id='a'*32, billing_free_start_date=today.replace(day=1).strftime('%Y-%m-%d'),
                           billing_period_end=(today+relativedelta(months=1)).strftime('%Y-%m-%d'),
                           billing_period_end_epoch=int((today+relativedelta(months=1)).timestamp()))

    def test_same_day_switch_new_cycle_but_retry_same_cycle(self):
        keys = self.context['get_message_statistic_keys']
        first = keys(self.config, 'app')
        self.assertEqual(first, keys(dict(self.config), 'app'))
        second = keys(dict(self.config, billing_order_id='b'*32), 'app')
        self.assertNotEqual(first[0], second[0])
        self.assertEqual(first[1:], second[1:])  # daily safety and real calendar usage unchanged

    def test_free_switch_preserves_tenant_shared_pool_and_legacy_keys(self):
        keys = self.context['get_message_statistic_keys']
        free = dict(self.config, product_id=7001)
        legacy = dict(product_id=7001, tenant_id=7, start_date=free['billing_free_start_date'])
        self.assertEqual(keys(legacy, 'app'), keys(free, 'app'))
        self.assertEqual(keys(free, 'app')[0], keys(dict(free, billing_order_id='c'*32), 'other')[0])

    def test_paid_period_does_not_silently_renew_after_expiry(self):
        expired = dict(self.config, start_date='2020-01-01', billing_period_end='2020-02-01', billing_period_end_epoch=1580486400)
        self.assertEqual('2020-01-01', self.context['get_message_quota_period'](expired)['start'])
        self.assertEqual(1580486400, self.context['get_message_quota_period'](expired)['expires_epoch'])

    def test_hosting_switch_resets_period_usage_but_not_real_storage(self):
        status = dict(product_id=9802, billing_order_id='a'*32, pay_start_date=self.config['start_date'])
        context = functions('lanying_grow_ai.py', {'get_service_statistic_key_list'},
                            dict(datetime=datetime, relativedelta=relativedelta, get_service_status=lambda app: status))
        keys = context['get_service_statistic_key_list']
        traffic, storage = keys('app', 'website_traffic'), keys('app', 'website_storage')
        status['billing_order_id'] = 'b'*32
        self.assertNotEqual(traffic[0], keys('app', 'website_traffic')[0])
        self.assertEqual(storage, keys('app', 'website_storage'))
        self.assertEqual(traffic[1:], keys('app', 'website_traffic')[1:])


if __name__ == '__main__': unittest.main()
