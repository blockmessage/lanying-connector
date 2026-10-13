import json
import types
import unittest
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine, text

import lanying_seenical_configuration as configuration


class SQLiteConnection:
    """Use SQLite for persistence tests, adapting only MySQL's upsert syntax."""
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.conn = self.connection.__enter__()
        return self

    def __exit__(self, *args):
        return self.connection.__exit__(*args)

    def execute(self, sql, params):
        statement = str(sql).replace('ON DUPLICATE KEY UPDATE configuration=VALUES(configuration)',
                                    'ON CONFLICT(app_id, seenical_session_id, bound_im_user_id) DO UPDATE SET configuration=excluded.configuration')
        return self.conn.execute(text(statement.replace(' FOR UPDATE', '')), params)


class ConfigurationTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        with self.engine.begin() as conn:
            conn.execute(text('''CREATE TABLE seenical_conversation_configuration (
                app_id TEXT, seenical_session_id TEXT, bound_im_user_id TEXT, configuration TEXT,
                PRIMARY KEY(app_id, seenical_session_id, bound_im_user_id))'''))
        adapter = types.SimpleNamespace(begin=lambda: SQLiteConnection(self.engine.begin()), connect=lambda: SQLiteConnection(self.engine.connect()))
        self.engine_patch = mock.patch.object(configuration.storage, 'get_engine', return_value=adapter)
        self.engine_patch.start()
        self.tools = types.SimpleNamespace(
            list_seenical_conversations=mock.Mock(side_effect=lambda app, actor: {
                'result': 'ok', 'data': {'list': [{'seenical_session_id': 'primary:a'}, {'seenical_session_id': 'child'}]}
            } if app == 'app' and actor['im_user_id'] == '123' else {'result': 'error'}),
            _validate_seenical_conversation=mock.Mock(return_value={'result': 'ok'}))
        self.modules = mock.patch.dict('sys.modules', {'lanying_agent_tools': self.tools})
        self.modules.start()
        self.actor = {'im_user_id': '123'}

    def tearDown(self):
        self.modules.stop()
        self.engine_patch.stop()
        self.engine.dispose()

    def save(self, session='primary:a', value=None, app='app', actor=None):
        return configuration.handle(app, actor or self.actor, {
            'seenical_session_id': session, 'configuration': value or {'prompt': '产品介绍', 'sessionPrompt': '不编造事实'}}, True)

    def test_main_and_child_restore_and_replace(self):
        self.assertEqual('ok', self.save()['result'])
        self.assertEqual('ok', self.save('child')['result'])
        self.save(value={'prompt': 'new topic', 'sessionPrompt': '', 'keywords': ''})
        result = configuration.handle('app', self.actor, {})['data']['list']
        values = {item['seenical_session_id']: item['configuration'] for item in result}
        self.assertEqual('', values['primary:a']['sessionPrompt'])
        self.assertEqual('不编造事实', values['child']['sessionPrompt'])

    def test_app_user_session_and_membership_checks(self):
        self.assertEqual('error', self.save(app='other')['result'])
        self.assertEqual('error', self.save(actor={'im_user_id': '999'})['result'])
        self.assertEqual('error', self.save(session='unknown')['result'])
        self.save()
        self.tools._validate_seenical_conversation.return_value = {'result': 'error'}
        self.assertEqual('error', self.save()['result'])
        self.assertEqual([], configuration.handle('app', self.actor, {})['data']['list'])

    def test_rebinding_does_not_expose_old_settings(self):
        self.save()
        self.tools.list_seenical_conversations.side_effect = None
        self.tools.list_seenical_conversations.return_value = {'result': 'ok', 'data': {'list': [{'seenical_session_id': 'primary:a'}]}}
        self.assertEqual([], configuration.handle('app', {'im_user_id': '999'}, {})['data']['list'])

    def test_summary_updates_only_prompt_and_initializes_only_once(self):
        request = {'seenical_session_id': 'primary:a', 'configuration': {'sessionPrompt': 'summary'},
                   'initial_configuration': {'prompt': 'old topic', 'keywords': 'old title', 'contentLanguage': 'zh-hans'}}
        self.assertEqual('old topic', configuration.handle('app', self.actor, request, True)['data']['configuration']['prompt'])
        self.save(value={'prompt': 'new topic', 'keywords': 'new title', 'contentLanguage': 'en'})
        updated = configuration.handle('app', self.actor, request, True)['data']['configuration']
        self.assertEqual({'prompt': 'new topic', 'keywords': 'new title', 'contentLanguage': 'en', 'sessionPrompt': 'summary'}, updated)
        request['configuration'] = {'sessionPrompt': None}
        self.assertEqual('error', configuration.handle('app', self.actor, request, True)['result'])

    def test_targeted_read_does_not_check_unrelated_group(self):
        self.save()
        self.save('child')
        def validate(app, actor, binding):
            if binding['seenical_session_id'] == 'child':
                raise TimeoutError('unavailable group')
            return {'result': 'ok'}
        self.tools._validate_seenical_conversation.reset_mock()
        self.tools._validate_seenical_conversation.side_effect = validate
        result = configuration.handle('app', self.actor, {'seenical_session_id': 'primary:a'})
        self.assertEqual(['primary:a'], [item['seenical_session_id'] for item in result['data']['list']])
        self.assertEqual(1, self.tools._validate_seenical_conversation.call_count)
        self.assertEqual('error', configuration.handle('app', self.actor, {'seenical_session_id': 'unknown'})['result'])
        # Background restoration skips a failed group without leaking its configuration.
        result = configuration.handle('app', self.actor, {})
        self.assertEqual(['primary:a'], [item['seenical_session_id'] for item in result['data']['list']])
        self.assertEqual(['child'], result['data']['unavailable_session_ids'])

    def test_validation_does_not_overwrite_saved_configuration(self):
        self.save()
        for value in [{'prompt': 'p', 'token': 'bad'}, {'prompt': 'p', 'wordMin': False},
                      {'prompt': 'p', 'sessionPrompt': None}, {'prompt': 'p', 'wordMin': 3000, 'wordMax': 1000},
                      {'prompt': 'p', 'contentLanguage': 'auto'}, {'prompt': ''}]:
            self.assertEqual('error', self.save(value=value)['result'])
        saved = configuration.handle('app', self.actor, {})['data']['list'][0]['configuration']
        self.assertEqual('产品介绍', saved['prompt'])

    def test_upgrade_and_bootstrap_match(self):
        directory = Path(__file__).resolve().parents[1] / 'sql'
        upgrade = (directory / 'seenical_conversation_configuration.sql').read_text().split('\n', 1)[1].strip()
        self.assertIn(upgrade, (directory / 'seenical_agent_tools_mysql.sql').read_text())
        self.assertIn('ON UPDATE CURRENT_TIMESTAMP(3)', upgrade)


if __name__ == '__main__':
    unittest.main()
