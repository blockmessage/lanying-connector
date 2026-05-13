import unittest
import importlib
from datetime import datetime
from unittest import mock
import sys
import types
from pathlib import Path


def _install_fake_psycopg2_if_needed():
    if 'psycopg2' in sys.modules:
        return
    try:
        import psycopg2  # noqa: F401
        return
    except Exception:
        pass
    psycopg2_mod = types.ModuleType('psycopg2')
    pool_mod = types.ModuleType('psycopg2.pool')
    extras_mod = types.ModuleType('psycopg2.extras')

    class _DummyPool:
        def __init__(self, *args, **kwargs):
            pass

        def getconn(self):
            return None

        def putconn(self, conn, close=False):
            return None

    class _DummyJson:
        def __init__(self, value):
            self.value = value

    psycopg2_mod.OperationalError = Exception
    psycopg2_mod.InterfaceError = Exception
    psycopg2_mod.DatabaseError = Exception
    pool_mod.ThreadedConnectionPool = _DummyPool
    extras_mod.Json = _DummyJson

    psycopg2_mod.pool = pool_mod
    sys.modules['psycopg2'] = psycopg2_mod
    sys.modules['psycopg2.pool'] = pool_mod
    sys.modules['psycopg2.extras'] = extras_mod


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class PgvectorMessageQuotaLogTests(unittest.TestCase):
    def test_list_message_quota_usage_logs_supports_all_and_app_filter(self):
        module_dir = str(Path(__file__).resolve().parents[1])
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
        _install_fake_psycopg2_if_needed()
        m = importlib.import_module('lanying_pgvector')
        rows = [
            (
                1,
                datetime(2026, 5, 13, 10, 0, 0),
                'app-1',
                1.5,
                'chat',
                'openai',
                'gpt-4o-mini',
                'share',
                1,
                40,
                30,
                10,
                12,
                'off',
                7001,
                {'k': 'v'},
            )
        ]
        conn = _FakeConn(rows)

        with (
            mock.patch.object(m, 'is_enabled', return_value=True),
            mock.patch.object(m, 'ensure_message_quota_usage_log_table'),
            mock.patch.object(m, 'get_connection', return_value=conn),
            mock.patch.object(m, 'put_connection'),
        ):
            all_logs = m.list_message_quota_usage_logs(limit=5)
            app_logs = m.list_message_quota_usage_logs(app_id='app-1', limit=5)

        self.assertEqual(2, len(conn.cursor_obj.executed))
        self.assertIn('FROM message_quota_usage_log', conn.cursor_obj.executed[0][0])
        self.assertNotIn('WHERE app_id = %s', conn.cursor_obj.executed[0][0])
        self.assertIn('WHERE app_id = %s', conn.cursor_obj.executed[1][0])
        self.assertEqual('app-1', app_logs[0]['app_id'])
        self.assertEqual(1.5, all_logs[0]['quota'])


if __name__ == '__main__':
    unittest.main()
