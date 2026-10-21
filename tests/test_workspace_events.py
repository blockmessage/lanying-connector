import ast
import logging
from pathlib import Path
import types
import unittest
from unittest import mock

import lanying_workspace_events as events


class EventTest(unittest.TestCase):
    def test_disabled_storage_skips_notification(self):
        with mock.patch('lanying_agent_tools_storage.is_enabled', return_value=False), \
                mock.patch('lanying_workspace_sync.request_check') as changed:
            events.materials_changed('a')
        changed.assert_not_called()

    def test_enabled_storage_marks_work_without_executing_sync(self):
        with mock.patch('lanying_agent_tools_storage.is_enabled', return_value=True), \
                mock.patch('lanying_workspace_sync.request_check') as changed, \
                mock.patch('lanying_workspace_sync.enqueue') as enqueue:
            events.materials_changed('a')
        changed.assert_called_once_with('a')
        enqueue.assert_not_called()

    def test_notification_failure_preserves_primary_operation(self):
        with mock.patch('lanying_agent_tools_storage.is_enabled', return_value=True), \
                mock.patch('lanying_workspace_sync.request_check', side_effect=RuntimeError('secret')), \
                self.assertLogs(level='WARNING') as logs:
            events.materials_changed('a')
        self.assertNotIn('secret', ''.join(logs.output))
        self.assertIn('manual check', ''.join(logs.output))

    def test_knowledge_event_decodes_app_and_ignores_session_indexes(self):
        # Load this small hook without booting embedding providers / network clients.
        source = ast.parse(Path('lanying_embedding.py').read_text())
        node = next(n for n in source.body if isinstance(n, ast.FunctionDef)
                    and n.name == 'notify_workspace_material_change')
        import lanying_redis
        redis = mock.Mock()
        redis.hget.side_effect = lambda key, field: {'type': b'text', 'app_id': b'app'}[field]
        namespace = {'logging': logging, 'get_embedding_uuid_key': lambda value: value,
                     'lanying_redis': types.SimpleNamespace(get_redis_stack_connection=lambda: redis,
                                                          redis_hget=lanying_redis.redis_hget)}
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'hook', 'exec'), namespace)
        with mock.patch.object(events, 'materials_changed') as changed:
            namespace['notify_workspace_material_change']('kb')
            changed.assert_called_once_with('app')
            changed.reset_mock()
            redis.hget.side_effect = lambda key, field: b'seenical_session'
            namespace['notify_workspace_material_change']('internal')
            changed.assert_not_called()
