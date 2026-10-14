import unittest
from unittest.mock import patch
from unittest.mock import Mock
import ast
import logging
import json
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

# Exercise the real deployment function without importing unrelated network/storage
# initializers (the Selenium loader creates /data/screenshot at import time).
source = Path(__file__).resolve().parents[1] / 'lanying_grow_ai.py'
tree = ast.parse(source.read_text())
node = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == 'check_deploy')
namespace = {'logging': logging}
for name in ('get_deploy_code', 'get_site', 'get_service_status', 'get_app_total_website_storage',
             'update_site_field', 'update_site_storage'):
    namespace[name] = Mock()
exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), namespace)
grow = SimpleNamespace(**namespace)


class FreeHostingTest(unittest.TestCase):
    def deploy(self, release_mb, old_mb=0, total_mb=0, product_id=9851):
        mb = 1024 * 1024
        update = Mock()
        with patch.dict(namespace, {
                'get_deploy_code': Mock(return_value={'app_id': 'a', 'site_id': 's'}),
                'get_site': Mock(return_value={'website_storage': old_mb * mb}),
                'get_service_status': Mock(return_value={'website_storage_limit': 50 if product_id == 9851 else 100, 'product_id': product_id}),
                'get_app_total_website_storage': Mock(return_value=total_mb * mb),
                'update_site_field': Mock(), 'update_site_storage': update}):
            result = grow.check_deploy('test', release_mb * mb)
            if result['result'] == 'error':
                update.assert_not_called()
            return result['result']

    def test_exact_quota_is_allowed(self):
        self.assertEqual('ok', self.deploy(50))

    def test_increased_usage_over_quota_is_rejected(self):
        self.assertEqual('error', self.deploy(51))

    def test_reducing_existing_over_quota_usage_is_allowed(self):
        self.assertEqual('ok', self.deploy(60, old_mb=70, total_mb=70))

    def test_other_sites_count_toward_shared_quota(self):
        self.assertEqual('error', self.deploy(30, old_mb=10, total_mb=40))

    def test_legacy_deploy_boundary_is_unchanged(self):
        self.assertEqual('error', self.deploy(100, product_id=9801))


class HostingTrafficTest(unittest.TestCase):
    def test_fractional_and_legacy_gb_roundtrip(self):
        redis = Mock()
        stored = {}
        redis.hmset.side_effect = lambda key, values: stored.update(values)
        helpers = SimpleNamespace(
            get_redis_connection=lambda: redis,
            get_redis_stack_connection=lambda: redis,
            redis_hgetall=lambda connection, key: {k: str(v) for k, v in stored.items()},
            redis_hget=lambda connection, key, field: "0.5")
        context = {'datetime': datetime, 'time': time, 'lanying_redis': helpers,
                   'lanying_config': SimpleNamespace(get_redis_app_config_key=lambda app: "config:" + app),
                   'get_service_status_key': lambda app: "service:" + app}
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in
                     ('open_service', 'get_service_status', 'get_website_traffic_limit')]
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), 'exec'), context)
        for traffic in (0.5, 1.0, 10.0):
            self.assertEqual("ok", context['open_service']('a', 9851, 0, 50, traffic)['result'])
            if float(traffic).is_integer():
                self.assertIsInstance(stored['website_traffic_limit'], int)
            dto = context['get_service_status']('a')
            self.assertEqual(traffic, dto['website_traffic_limit'])
            self.assertEqual(50, dto['website_storage_limit'])
        self.assertEqual(0.5, context['get_website_traffic_limit']('a'))

    def test_http_accepts_fractional_and_legacy_gb(self):
        route_source = source.parent / 'services/grow_ai_service.py'
        route_tree = ast.parse(route_source.read_text())
        route = next(n for n in route_tree.body if isinstance(n, ast.FunctionDef) and n.name == 'open_service')
        route.decorator_list = []
        service = Mock(return_value={'result': 'ok', 'data': {'success': True}})
        request = Mock()
        context = {'request': request, 'json': json, 'make_response': lambda value: value,
                   'check_access_token_valid': lambda: True,
                   'lanying_grow_ai': SimpleNamespace(open_service=service)}
        exec(compile(ast.Module(body=[route], type_ignores=[]), str(route_source), 'exec'), context)
        for traffic in (0.5, 1):
            request.get_data.return_value = json.dumps({'app_id': 'a', 'product_id': 9851,
                'price': 0, 'website_storage_limit': 50, 'website_traffic_limit': traffic})
            self.assertEqual(200, context['open_service']()['code'])
            service.assert_called_with('a', 9851, 0, 50, traffic)


if __name__ == '__main__':
    unittest.main()
