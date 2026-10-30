"""Paid WeChat service gating must preserve the existing user connection settings."""
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest
import json
from flask import request
from test_billing_plugin_config import BillingPluginConfigTests, BillingPluginMirrorTests


class WechatServiceSettingsTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('lanying_config.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_service_config')
        self.settings = dict(enable=True, wechat_app_secret='test-only', user_id='123')
        self.enabled = None
        self.get = Mock(side_effect=lambda app, key, default: self.enabled if key.endswith('wechat_ai_cs_enabled') else self.settings)
        scope = dict(mode='etcd', get_config=self.get)
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'lanying_config.py', 'exec'), scope)
        self.read = scope['get_service_config']

    def test_legacy_without_entitlement_is_unchanged(self):
        self.assertIs(self.settings, self.read('app', 'wechat_official_account'))

    def test_pause_blocks_reads_without_deleting_connection_settings(self):
        self.enabled = False
        self.assertIsNone(self.read('app', 'wechat_official_account'))
        self.assertEqual('test-only', self.settings['wechat_app_secret'])
        self.enabled = True
        self.assertIs(self.settings, self.read('app', 'wechat_official_account'))

    def test_resume_does_not_enable_user_disabled_service(self):
        self.enabled = True
        self.settings['enable'] = False
        self.assertIsNone(self.read('app', 'wechat_official_account'))

    def test_pause_does_not_disable_other_services(self):
        self.enabled = False
        for service in ('wechat', 'openai'):
            self.assertIs(self.settings, self.read('app', service))


class WechatPersistentConfigTests(BillingPluginConfigTests):
    def call(self, **changes):
        changes.setdefault('key', 'lanying_connector.billing.wechat_ai_cs_enabled')
        changes.setdefault('value', 'false')
        return super().call(**changes)

    def test_paid_acknowledgement_follows_both_stores_and_replication(self):
        response = self.call()
        self.assertEqual('success:app-config', response.get_data(as_text=True))
        self.config.save_config.assert_called_once_with('app', 'lanying_connector.billing.wechat_ai_cs_enabled', 'false')
        self.sync.assert_called_once_with('app', 'lanying_connector.billing.wechat_ai_cs_enabled', 'false', 'test-only', False, True)


class WechatMirrorTests(BillingPluginMirrorTests):
    def call(self, strict=True):
        with patch.dict('os.environ', {'SYNC_ETCD_CONFIG_TO_SERVER': 'https://mirror'}):
            with self.app.test_request_context('/config', method='POST', headers={'access-token': 'test-only'},
                                              json=dict(app_id='app', key='lanying_connector.billing.wechat_ai_cs_enabled',
                                                        value='false', require_app_config=strict)):
                return self.handler()

    def test_both_nodes_must_acknowledge_persistent_quota(self):
        self.http.post.return_value = SimpleNamespace(status_code=200, text='success:app-config')
        self.assertEqual('success:app-config', self.call().get_data(as_text=True))
        self.assertEqual('false', self.http.post.call_args.kwargs['json']['value'])


if __name__ == '__main__':
    unittest.main()
