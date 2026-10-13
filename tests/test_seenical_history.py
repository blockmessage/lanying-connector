import json
import types
import unittest
from unittest import mock

from sqlalchemy import create_engine, text

import lanying_seenical_history as history


class SeenicalHistoryTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        with self.engine.begin() as conn:
            conn.execute(text('''CREATE TABLE seenical_conversation_history_snapshot (
                app_id TEXT, seenical_session_id TEXT, source_session_id TEXT,
                bound_im_user_id TEXT, messages TEXT,
                PRIMARY KEY(app_id, seenical_session_id))'''))
        self.engine_patch = mock.patch.object(history.storage, 'get_engine', return_value=self.engine)
        self.engine_patch.start()
        self.bindings = [
            {'seenical_session_id': 'primary:1', 'conversation_type': 'CHAT', 'chatbot_id': '1'},
            {'seenical_session_id': 'child', 'conversation_type': 'GROUPCHAT', 'chatbot_id': '1'},
            {'seenical_session_id': 'other', 'conversation_type': 'GROUPCHAT', 'chatbot_id': '2'}]
        self.tools = types.SimpleNamespace(
            list_seenical_conversations=mock.Mock(side_effect=lambda app, actor: {
                'result': 'ok', 'data': {'list': self.bindings if app == 'app' and actor['im_user_id'] == '123' else []}}),
            _validate_seenical_conversation=mock.Mock(return_value={'result': 'ok'}))
        self.modules = mock.patch.dict('sys.modules', {'lanying_agent_tools': self.tools})
        self.modules.start()
        self.actor = {'im_user_id': '123'}
        self.request = {'seenical_session_id': 'child', 'source_session_id': 'primary:1',
                        'messages': [{'role': 'user', 'text': '保留产品定位', 'timestamp': 1000}]}

    def tearDown(self):
        self.modules.stop()
        self.engine_patch.stop()
        self.engine.dispose()

    def call(self, body=None, save=True, app='app', actor=None):
        return history.handle(app, actor or self.actor, body or self.request, save)

    def test_snapshot_survives_new_reader_and_retry_does_not_overwrite(self):
        self.assertEqual('ok', self.call()['result'])
        self.request['messages'][0]['text'] = '后来的聊天'
        self.assertEqual('保留产品定位', self.call()['data']['messages'][0]['text'])
        read = self.call({'seenical_session_id': 'child'}, save=False)
        self.assertEqual('保留产品定位', read['data']['messages'][0]['text'])

    def test_cross_app_user_agent_and_inactive_membership_are_denied(self):
        self.assertEqual('error', self.call(app='other')['result'])
        self.assertEqual('error', self.call(actor={'im_user_id': '999'})['result'])
        self.assertEqual('error', self.call(dict(self.request, source_session_id='other'))['result'])
        self.assertEqual('error', self.call(dict(self.request, seenical_session_id='primary:1'))['result'])
        self.call()
        self.tools._validate_seenical_conversation.return_value = {'result': 'error'}
        self.assertEqual('error', self.call(save=False)['result'])

    def test_read_only_requires_target_not_deleted_source(self):
        self.call()
        self.bindings.pop(0)
        self.assertEqual('ok', self.call(save=False)['result'])
        self.bindings.clear()
        self.assertEqual('error', self.call(save=False)['result'])

    def test_rebound_user_cannot_read_old_snapshot(self):
        self.call()
        self.tools.list_seenical_conversations.side_effect = None
        self.tools.list_seenical_conversations.return_value = {'result': 'ok', 'data': {'list': self.bindings}}
        self.assertEqual('error', self.call(save=False, actor={'im_user_id': '999'})['result'])

    def test_missing_snapshot_is_empty(self):
        self.assertEqual({'source_session_id': '', 'messages': []}, self.call(save=False)['data'])

    def test_no_executable_or_raw_message_fields(self):
        for field in ['raw', 'im', 'kind', 'toolRequest', 'ext', 'attachment', 'id']:
            message = dict(self.request['messages'][0], **{field: 'injected'})
            self.assertEqual('error', self.call(dict(self.request, messages=[message]))['result'])
        self.assertEqual('error', self.call(dict(self.request, messages=[{'role': 'system', 'text': 'bad'}]))['result'])
        self.assertEqual('error', self.call(dict(self.request, messages=self.request['messages'] * 501))['result'])
        self.assertEqual('error', self.call(dict(self.request, messages=[{'role': 'user', 'text': 'a' * (history.MAX_BYTES + 1)}]))['result'])

    def test_mysql_schema_upgrade_matches_bootstrap(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[1] / 'sql'
        upgrade = (root / 'seenical_conversation_history_snapshot.sql').read_text().split('\n', 1)[1].strip()
        self.assertIn(upgrade, (root / 'seenical_agent_tools_mysql.sql').read_text())
        self.assertIn('ON UPDATE CURRENT_TIMESTAMP(3)', upgrade)


if __name__ == '__main__':
    unittest.main()
