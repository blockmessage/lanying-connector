import json
import unittest
from datetime import datetime
from unittest import mock

import lanying_operational_storage as storage


class FakeResult:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def fetchall(self):
        return self.rows


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


class OperationalStorageTest(unittest.TestCase):
    def test_message_quota_log_writes_mysql(self):
        engine = FakeEngine()
        with mock.patch.object(
                storage.lanying_agent_tools_storage, 'get_engine', return_value=engine):
            result = storage.append_message_quota_usage_log({
                'app_id': 'app-1',
                'quota': 1.5,
                'model_type': 'chat',
                'vendor': 'openai',
                'model': 'gpt-4o-mini',
                'api_key_type': 'share',
                'message_count': 1,
                'total_tokens': 40,
                'prompt_tokens': 30,
                'completion_tokens': 10,
                'text_size': 12,
                'content_security': 'off',
                'product_id': 7001,
                'extra_metadata': {'source': 'test'},
            })
        self.assertEqual('ok', result['result'])
        sql, params = engine.connection.calls[0]
        self.assertIn('INSERT INTO message_quota_usage_log', sql)
        self.assertEqual('app-1', params['app_id'])
        self.assertEqual({'source': 'test'}, json.loads(params['extra_metadata']))

    def test_message_quota_log_reads_mysql_with_app_filter(self):
        rows = [(
            1, datetime(2026, 5, 13, 10, 0, 0), 'app-1', 1.5, 'chat',
            'openai', 'gpt-4o-mini', 'share', 1, 40, 30, 10, 12, 'off',
            7001, '{"source":"test"}',
        )]
        engine = FakeEngine([FakeResult(rows)])
        with mock.patch.object(
                storage.lanying_agent_tools_storage, 'get_engine', return_value=engine):
            logs = storage.list_message_quota_usage_logs('app-1', 5)
        sql, params = engine.connection.calls[0]
        self.assertIn('WHERE app_id=:app_id', sql)
        self.assertEqual('app-1', params['app_id'])
        self.assertEqual(1.5, logs[0]['quota'])
        self.assertEqual({'source': 'test'}, logs[0]['extra_metadata'])

    def test_openclaw_session_map_log_writes_and_reads_mysql(self):
        write_engine = FakeEngine()
        entry = {
            'app_id': 'app-1',
            'node_id': 'node-1',
            'session_key': 'session-1',
            'previous_mapping': {'group_id': 'old'},
            'new_mapping': {'group_id': 'new'},
        }
        with mock.patch.object(
                storage.lanying_agent_tools_storage, 'get_engine', return_value=write_engine):
            result = storage.append_openclaw_session_map_log(entry)
        self.assertEqual('ok', result['result'])
        self.assertIn('INSERT INTO openclaw_session_map_log',
                      write_engine.connection.calls[0][0])

        rows = [(
            2, datetime(2026, 5, 13, 11, 0, 0), 'app-1', 'node-1',
            'session-1', '', '', 'update', '{}', '{}',
            '{"group_id":"old"}', '{"group_id":"new"}', '[]', '{}',
        )]
        read_engine = FakeEngine([FakeResult(rows)])
        with mock.patch.object(
                storage.lanying_agent_tools_storage, 'get_engine', return_value=read_engine):
            logs = storage.list_openclaw_session_map_logs('app-1', 'node-1')
        self.assertEqual({'group_id': 'new'}, logs[0]['new_mapping'])
        self.assertIn('WHERE app_id=:app_id AND node_id=:node_id',
                      read_engine.connection.calls[0][0])


if __name__ == '__main__':
    unittest.main()
