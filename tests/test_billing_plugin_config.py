"""Exercise the actual /config handler without starting Connector workers."""
import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from flask import Flask, request


class BillingPluginConfigTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.config = SimpleNamespace(mode='etcd', etcd=object(), save_config=Mock())
        self.embedding = SimpleNamespace(save_app_config=Mock())
        self.sync = Mock()
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('lanying_connector.py').read_text())
        handler = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'saveConfig')
        handler.decorator_list = []
        scope = dict(app=self.app, request=request, json=json, accessToken='test-only',
                     logging=Mock(), lanying_config=self.config, lanying_embedding=self.embedding,
                     maybeSyncConfig=self.sync)
        exec(compile(ast.Module(body=[handler], type_ignores=[]), 'lanying_connector.py', 'exec'), scope)
        self.handler = scope['saveConfig']

    def call(self, **changes):
        body = dict(app_id='app', key='lanying_connector.function_num', value='20', require_app_config=True)
        body.update(changes)
        with self.app.test_request_context('/config', method='POST', json=body, headers={'access-token': 'test-only'}):
            return self.handler()

    def test_paid_quota_requires_etcd_before_any_write(self):
        for mode, client in [('env', None), ('etcd', None), ('env', object())]:
            self.config.mode, self.config.etcd = mode, client
            response = self.call()
            self.assertEqual(409, response.status_code)
        self.config.save_config.assert_not_called()
        self.embedding.save_app_config.assert_not_called()
        self.sync.assert_not_called()

    def test_paid_acknowledgement_follows_both_stores_and_replication(self):
        response = self.call(value='0')
        self.assertEqual('success:app-config', response.get_data(as_text=True))
        self.config.save_config.assert_called_once_with('app', 'lanying_connector.function_num', '0')
        self.embedding.save_app_config.assert_called_once_with('app', 'lanying_connector.function_num', '0')
        self.sync.assert_called_once_with('app', 'lanying_connector.function_num', '0', 'test-only', False, True)

    def test_failed_store_does_not_acknowledge_or_continue(self):
        self.config.save_config.side_effect = RuntimeError('etcd unavailable')
        with self.assertRaises(RuntimeError):
            self.call()
        self.embedding.save_app_config.assert_not_called()
        self.sync.assert_not_called()

    def test_old_configuration_contract_is_unchanged(self):
        self.config.mode, self.config.etcd = 'env', None
        response = self.call(require_app_config=False)
        self.assertEqual('success', response.get_data(as_text=True))
        self.config.save_config.assert_called_once()

    def test_new_acknowledgement_cannot_be_used_for_other_configuration(self):
        self.assertEqual(409, self.call(key='lanying_connector').status_code)
        self.config.save_config.assert_not_called()

    def test_authentication_still_precedes_configuration(self):
        with self.app.test_request_context('/config', method='POST', json={}):
            self.assertEqual('fail', self.handler().get_data(as_text=True))
        self.config.save_config.assert_not_called()


class BillingPluginMirrorTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.config = SimpleNamespace(mode='etcd', etcd=object(), save_config=Mock())
        self.http = Mock()
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('lanying_connector.py').read_text())
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name in ('saveConfig', 'maybeSyncConfig', 'syncConfig')]
        for function in functions:
            function.decorator_list = []
        scope = dict(app=self.app, request=request, json=json, os=os, accessToken='test-only',
                     logging=Mock(), lanying_config=self.config,
                     lanying_embedding=SimpleNamespace(save_app_config=Mock()), requests=self.http)
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'lanying_connector.py', 'exec'), scope)
        self.handler = scope['saveConfig']

    def call(self, strict=True):
        with patch.dict(os.environ, {'SYNC_ETCD_CONFIG_TO_SERVER': 'https://mirror'}):
            with self.app.test_request_context('/config', method='POST', headers={'access-token': 'test-only'},
                                              json=dict(app_id='app', key='lanying_connector.function_num',
                                                        value='0', require_app_config=strict)):
                return self.handler()

    def test_mirror_rejection_cannot_acknowledge_paid_quota(self):
        for status, body in [(401, 'fail'), (500, 'failed'), (200, 'fail'), (200, 'success'), (302, '')]:
            with self.subTest(status=status, body=body):
                self.http.post.return_value = SimpleNamespace(status_code=status, text=body)
                with self.assertRaises(RuntimeError):
                    self.call()
        self.assertEqual((5, 10), self.http.post.call_args.kwargs['timeout'])
        self.assertFalse(self.http.post.call_args.kwargs['allow_redirects'])
        self.assertTrue(self.http.post.call_args.kwargs['json']['require_app_config'])

    def test_mirror_timeout_cannot_acknowledge_paid_quota(self):
        self.http.post.side_effect = RuntimeError('timeout')
        with self.assertRaises(RuntimeError):
            self.call()

    def test_both_nodes_must_acknowledge_persistent_quota(self):
        self.http.post.return_value = SimpleNamespace(status_code=200, text='success:app-config')
        self.assertEqual('success:app-config', self.call().get_data(as_text=True))
        self.assertEqual('0', self.http.post.call_args.kwargs['json']['value'])

    def test_legacy_mirror_protocol_does_not_require_new_acknowledgement(self):
        self.http.post.return_value = SimpleNamespace(status_code=200, text='success')
        self.assertEqual('success', self.call(False).get_data(as_text=True))
        self.assertNotIn('require_app_config', self.http.post.call_args.kwargs['json'])


if __name__ == '__main__':
    unittest.main()
