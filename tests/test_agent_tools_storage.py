import json
import os
import sys
import types
import unittest
from unittest import mock

import lanying_agent_tools_storage as storage


class FakeScalars:
    def __init__(self, values):
        self.values = values

    def all(self):
        return self.values


class FakeMappings:
    def __init__(self, values):
        self.values = values

    def first(self):
        return self.values[0] if self.values else None

    def all(self):
        return self.values


class FakeResult:
    def __init__(self, rowcount=1, scalar=None, values=None):
        self.rowcount = rowcount
        self.scalar = scalar
        self.values = values or []

    def scalar_one_or_none(self):
        return self.scalar

    def scalars(self):
        return FakeScalars(self.values)

    def mappings(self):
        return FakeMappings(self.values)


class FakeConnection:
    def __init__(self, results=None):
        self.results = list(results or [])
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return self.results.pop(0) if self.results else FakeResult()


class FakeEngine:
    def __init__(self, results=None):
        self.connection = FakeConnection(results)

    def begin(self):
        return self.connection

    def connect(self):
        return self.connection


class AgentToolsStorageTest(unittest.TestCase):
    def setUp(self):
        storage._engine = None

    def test_mysql_is_disabled_without_host(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(storage.is_enabled())
            self.assertIsNone(storage.get_active_public_skill_catalog())

    def test_revision_storage_is_not_required_when_feature_is_disabled(self):
        fake_redis = types.SimpleNamespace(
            get=lambda key: None)
        redis_module = types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_get=lambda redis, key: redis.get(key))
        environment = {
            'LANYING_AGENT_TOOLS_MYSQL_HOST': 'mysql',
            'LANYING_AGENT_TOOLS_PLATFORM_ENABLED': 'off',
        }
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.dict(
                sys.modules, {'lanying_redis': redis_module}):
            self.assertFalse(storage.should_save_config_revision('app', 'bot'))

    def test_platform_feature_defaults_to_disabled(self):
        fake_redis = types.SimpleNamespace(get=lambda key: 'on')
        redis_module = types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_get=lambda redis, key: redis.get(key))
        with mock.patch.dict(os.environ, {
                'LANYING_AGENT_TOOLS_MYSQL_HOST': 'mysql'}, clear=True), mock.patch.dict(
                sys.modules, {'lanying_redis': redis_module}):
            self.assertFalse(storage.is_feature_enabled('app', 'bot'))
            self.assertFalse(storage.should_save_config_revision('app', 'bot'))

    def test_revision_storage_uses_chatbot_and_app_feature_switches(self):
        values = {
            'lanying_connector:agent_tools:feature:app:*': 'on',
            'lanying_connector:agent_tools:feature:app:disabled-bot': 'off',
        }
        fake_redis = types.SimpleNamespace(get=lambda key: values.get(key))
        redis_module = types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_get=lambda redis, key: redis.get(key))
        environment = {
            'LANYING_AGENT_TOOLS_MYSQL_HOST': 'mysql',
            'LANYING_AGENT_TOOLS_PLATFORM_ENABLED': 'on',
        }
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.dict(
                sys.modules, {'lanying_redis': redis_module}):
            self.assertTrue(storage.should_save_config_revision('app', 'enabled-bot'))
            self.assertFalse(storage.should_save_config_revision('app', 'disabled-bot'))

    def test_platform_enabled_without_app_switch_remains_disabled(self):
        fake_redis = types.SimpleNamespace(get=lambda key: None)
        redis_module = types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_get=lambda redis, key: redis.get(key))
        environment = {
            'LANYING_AGENT_TOOLS_MYSQL_HOST': 'mysql',
            'LANYING_AGENT_TOOLS_PLATFORM_ENABLED': 'on',
        }
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.dict(
                sys.modules, {'lanying_redis': redis_module}):
            self.assertFalse(storage.is_feature_enabled('app', 'bot'))
            self.assertFalse(storage.should_save_config_revision('app', 'bot'))

    def test_mysql_engine_uses_bounded_socket_timeouts(self):
        with mock.patch.dict(os.environ, {
                'LANYING_AGENT_TOOLS_MYSQL_HOST': 'mysql'}, clear=True), mock.patch.object(
                storage, 'create_engine', return_value=object()) as create_engine:
            storage._get_engine()
        connect_args = create_engine.call_args.kwargs['connect_args']
        self.assertEqual(10, connect_args['connect_timeout'])
        self.assertEqual(10, connect_args['read_timeout'])
        self.assertEqual(10, connect_args['write_timeout'])

    def test_catalog_revision_and_active_pointer_are_saved_together(self):
        engine = FakeEngine()
        catalog = {
            'revision': 'catalog-revision',
            'source_commit': 'a' * 40,
            'manifest_sha': 'manifest-sha',
            'skills': [{
                'skill_id': 'seenical-console',
                'revision': 'skill-revision',
                'name': 'Seenical Console',
            }],
        }
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            self.assertEqual('ok', storage.save_public_skill_catalog(catalog)['result'])
        self.assertEqual(3, len(engine.connection.calls))
        self.assertIn('INSERT IGNORE INTO public_skill_catalog_revision',
                      engine.connection.calls[0][0])
        self.assertEqual(catalog,
                         json.loads(engine.connection.calls[0][1]['catalog']))
        self.assertIn('INSERT IGNORE INTO public_skill_revision',
                      engine.connection.calls[1][0])
        self.assertIn('ON DUPLICATE KEY UPDATE', engine.connection.calls[2][0])

    def test_active_catalog_and_skill_revision_are_decoded(self):
        catalog = {'revision': 'catalog-revision', 'skills': []}
        catalog_engine = FakeEngine([FakeResult(scalar=json.dumps(catalog))])
        with mock.patch.object(storage, '_get_engine', return_value=catalog_engine):
            self.assertEqual(catalog, storage.get_active_public_skill_catalog())

        skill = {'skill_id': 'seenical-console', 'revision': 'skill-revision'}
        skill_engine = FakeEngine([FakeResult(scalar=json.dumps(skill))])
        with mock.patch.object(storage, '_get_engine', return_value=skill_engine):
            self.assertEqual(skill, storage.get_public_skill_revision(
                skill['skill_id'], skill['revision']))

    def test_agent_tool_request_view_is_saved_and_loaded(self):
        snapshot = {
            'request_id': 'request-a', 'app_id': 'app',
            'actor_subject_id': '11', 'status': 'pending',
            'expires_at': 1700000000,
        }
        save_engine = FakeEngine()
        with mock.patch.object(storage, '_get_engine', return_value=save_engine):
            result = storage.save_agent_tool_request_view(snapshot)
        self.assertEqual('ok', result['result'])
        self.assertIn('INSERT INTO agent_tool_request_view',
                      save_engine.connection.calls[0][0])
        self.assertEqual(snapshot,
                         json.loads(save_engine.connection.calls[0][1]['snapshot']))
        self.assertEqual(1, len(save_engine.connection.calls))

        load_engine = FakeEngine([FakeResult(scalar=json.dumps(snapshot))])
        with mock.patch.object(storage, '_get_engine', return_value=load_engine):
            self.assertEqual(snapshot, storage.get_agent_tool_request_view(
                'app', 'request-a'))

    def test_seenical_conversation_binding_is_persisted_and_listed(self):
        engine = FakeEngine([
            FakeResult(values=[]), FakeResult(values=[]), FakeResult(),
        ])
        binding = {
            'app_id': 'app', 'seenical_session_id': 'session-a',
            'chatbot_id': 'bot', 'agent_user_id': '33',
            'conversation_type': 'GROUPCHAT', 'conversation_id': '1001',
            'conversation_name': 'Child', 'task_id': '',
            'bound_im_user_id': '22',
        }
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            result = storage.save_seenical_conversation_binding(binding)
        self.assertEqual('ok', result['result'])
        self.assertIn('INSERT INTO seenical_conversation_binding',
                      engine.connection.calls[-1][0])
        self.assertIsNone(engine.connection.calls[-1][1]['task_id'])

        row = dict(binding, task_id='191', status='ACTIVE', revision=2,
                   created_at=None, updated_at=None)
        list_engine = FakeEngine([FakeResult(values=[row])])
        with mock.patch.object(storage, '_get_engine', return_value=list_engine):
            values = storage.list_seenical_conversation_bindings('app')
        self.assertEqual('191', values[0]['task_id'])
        self.assertEqual(2, values[0]['revision'])

    def test_conversation_list_fails_when_mysql_is_disabled(self):
        with mock.patch.object(storage, '_get_engine', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'MySQL disabled'):
                storage.list_seenical_conversation_bindings('app')

    def test_seenical_conversation_binding_can_be_deactivated(self):
        engine = FakeEngine([FakeResult(rowcount=1)])
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            result = storage.deactivate_seenical_conversation_binding(
                'app', 'session-a', 'GROUPCHAT', '1001', '22')
        self.assertEqual('ok', result['result'])
        statement, params = engine.connection.calls[0]
        self.assertIn("status='INACTIVE'", statement)
        self.assertEqual('session-a', params['seenical_session_id'])
        self.assertEqual('22', params['bound_im_user_id'])

    def test_unregister_missing_legacy_conversation_is_idempotent(self):
        engine = FakeEngine([FakeResult(rowcount=0), FakeResult(values=[])])
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            result = storage.deactivate_seenical_conversation_binding(
                'app', 'session-a', 'GROUPCHAT', '1001', '22')
        self.assertEqual('ok', result['result'])

    def test_conversation_with_bound_loop_cannot_be_deactivated(self):
        engine = FakeEngine([
            FakeResult(rowcount=0),
            FakeResult(values=[{
                'conversation_type': 'GROUPCHAT',
                'conversation_id': '1001',
                'task_id': '191',
            }]),
        ])
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            result = storage.deactivate_seenical_conversation_binding(
                'app', 'session-a', 'GROUPCHAT', '1001', '22')
        self.assertEqual('error', result['result'])
        self.assertEqual('Seenical conversation has a bound LOOP',
                         result['message'])

    def test_duplicate_config_revision_must_have_same_snapshot(self):
        snapshot = {'name': 'Original', 'article_language': 'en'}
        same_engine = FakeEngine([
            FakeResult(rowcount=0), FakeResult(scalar=json.dumps(snapshot))])
        with mock.patch.object(storage, '_get_engine', return_value=same_engine):
            result = storage.save_seenical_config_revision(
                'app', 'plan', 'plan-1', 2, snapshot)
        self.assertEqual('ok', result['result'])

        different_engine = FakeEngine([
            FakeResult(rowcount=0), FakeResult(scalar=json.dumps({'name': 'Other'}))])
        with mock.patch.object(storage, '_get_engine', return_value=different_engine):
            result = storage.save_seenical_config_revision(
                'app', 'plan', 'plan-1', 2, snapshot)
        self.assertEqual('error', result['result'])

    def test_config_revision_list_is_bounded(self):
        engine = FakeEngine([FakeResult(values=[5, 4, 3])])
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            self.assertEqual([5, 4, 3], storage.list_seenical_config_revisions(
                'app', 'site', 'site-1', 1000))
        self.assertEqual(100, engine.connection.calls[0][1]['limit'])


if __name__ == '__main__':
    unittest.main()
