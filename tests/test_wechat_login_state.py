import importlib.util
import json
import pathlib
import sys
import unittest
from unittest import mock


class WechatLoginStateTests(unittest.TestCase):
    def setUp(self):
        # Isolate network/storage dependencies; load the real lifecycle functions.
        dependencies = {name: mock.Mock() for name in (
            'requests', 'lanying_redis', 'lanying_chatbot',
            'lanying_slack', 'lanying_openclaw')}
        path = pathlib.Path(__file__).resolve().parents[1] / 'lanying_wechat_chatbot.py'
        spec = importlib.util.spec_from_file_location('wechat_login_state_test', path)
        self.module = importlib.util.module_from_spec(spec)
        with mock.patch.dict(sys.modules, dependencies):
            spec.loader.exec_module(self.module)
        self.redis = self.module.lanying_redis.get_redis_connection.return_value
        self.login = {
            'status': 'wait', 'reason': 'ok', 'wechat_chatbot_id': '',
            'wc_id': 'test-account', 'w_account': 'test-name',
            'result': json.dumps({'wcId': 'test-account', 'wAccount': 'test-name'})}
        self.sessions = {'session-1': self.login}
        self.module.get_wid_info = mock.Mock(
            side_effect=lambda app_id, wid: self.sessions.get(wid, {}).copy() or None)
        self.module.set_wid_info_field = mock.Mock(
            side_effect=lambda app_id, wid, key, value: self.sessions[wid].update({key: value}))
        self.module.get_wechat_chatbot = mock.Mock(return_value=None)
        self.module.lanying_openclaw.get_node.return_value = {'wechat_chatbot_id': ''}
        self.module.generate_chatbot_id = mock.Mock(return_value='binding-1')

    def create(self, wid='session-1'):
        return self.module.create_wechat_chatbot(
            'app-1', wid, 'openclaw', '22', ['chat'], 'forbidden', '', [])

    def configure(self, wid='session-1'):
        return self.module.configure_wechat_chatbot(
            'app-1', 'binding-1', wid, 'openclaw', '22', ['chat'], 'forbidden', '', [])

    def test_offline_callback_invalidates_cached_login_and_blocks_create(self):
        with self.assertLogs(level='INFO') as captured:
            self.module.change_wid_status('app-1', 'session-1', 'offline', 'kick')
        self.assertEqual('offline', self.login['status'])
        self.assertIn('from_status:wait, status:offline', '\n'.join(captured.output))
        self.assertNotIn('session-1', '\n'.join(captured.output))
        self.module.lanying_slack.async_send_message.assert_not_called()

        self.assertEqual({'result': 'error', 'message': 'expired'},
                         self.module.get_login_info('app-1', 'session-1'))
        self.assertEqual({'result': 'error', 'message': 'expired'}, self.create())
        self.module.requests.post.assert_not_called()
        self.redis.hmset.assert_not_called()
        self.redis.rpush.assert_not_called()
        self.module.generate_chatbot_id.assert_not_called()
        self.module.lanying_openclaw.update_node_field.assert_not_called()

    def test_fresh_session_can_bind_after_old_session_expires(self):
        self.sessions['session-2'] = self.login.copy()
        self.login['status'] = 'offline'
        self.assertEqual('expired', self.create()['message'])
        result = self.create('session-2')
        self.assertEqual('ok', result['result'])
        self.assertEqual('binding-1', result['data']['wechat_chatbot_id'])
        self.assertEqual('binding', self.sessions['session-2']['status'])
        self.assertEqual('offline', self.login['status'])
        self.module.lanying_openclaw.update_node_field.assert_called_once_with(
            'app-1', '22', 'wechat_chatbot_id', 'binding-1')

    def test_valid_cached_login_still_returns_account(self):
        for status in ('wait', 'binding'):
            with self.subTest(status=status):
                self.login['status'] = status
                self.assertEqual({'result': 'ok', 'data': {
                    'wc_id': 'test-account', 'w_account': 'test-name'}},
                    self.module.get_login_info('app-1', 'session-1'))
        self.module.requests.post.assert_not_called()

    def test_offline_login_without_cached_result_is_also_expired(self):
        self.login['status'] = 'offline'
        del self.login['result']
        self.assertEqual({'result': 'error', 'message': 'expired'},
                         self.module.get_login_info('app-1', 'session-1'))
        self.module.requests.post.assert_not_called()

    def test_missing_login_retains_existing_error(self):
        self.assertEqual('bad login info',
                         self.module.get_login_info('app-1', 'missing')['message'])
        self.assertEqual('bad login info', self.create('missing')['message'])

    def test_already_bound_login_cannot_create_another_binding(self):
        self.login.update(status='binding', wechat_chatbot_id='binding-1')
        self.assertEqual('bad login info', self.create()['message'])
        self.redis.hmset.assert_not_called()

    def test_offline_relogin_cannot_configure_binding(self):
        self.login.update(status='offline', wechat_chatbot_id='binding-1')
        self.assertEqual({'result': 'error', 'message': 'expired'}, self.configure())
        self.redis.hmset.assert_not_called()
        self.module.set_wid_info_field.assert_not_called()

    def test_configure_preserves_login_ownership_check(self):
        self.login.update(status='offline', wechat_chatbot_id='other-binding')
        self.assertEqual('bad login info', self.configure()['message'])
        self.redis.hmset.assert_not_called()

    def test_valid_relogin_can_configure_existing_binding(self):
        self.login['wechat_chatbot_id'] = 'binding-1'
        self.module.get_wechat_chatbot.return_value = {
            'bind_type': 'openclaw', 'openclaw_id': '22', 'chatbot_id': ''}
        self.assertEqual('ok', self.configure()['result'])
        self.assertEqual('binding', self.login['status'])
        self.assertEqual('online', self.redis.hmset.call_args.args[1]['status'])

    def test_plain_configuration_does_not_require_relogin(self):
        self.module.get_wechat_chatbot.return_value = {
            'bind_type': 'openclaw', 'openclaw_id': '22', 'chatbot_id': ''}
        self.assertEqual('ok', self.configure('')['result'])
        fields = self.redis.hmset.call_args.args[1]
        self.assertEqual('22', fields['openclaw_id'])
        self.assertNotIn('w_id', fields)
        self.assertNotIn('status', fields)
        self.module.set_wid_info_field.assert_not_called()


if __name__ == '__main__':
    unittest.main()
