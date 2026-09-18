import importlib.util
import pathlib
import sys
import unittest
from unittest import mock


def load_pgvector():
    module_name = 'lanying_pgvector_pool_test'
    sys.modules.pop(module_name, None)
    path = pathlib.Path(__file__).resolve().parents[1] / 'lanying_pgvector.py'
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict('os.environ', {
            'LANYING_CONNECTOR_SQL_POOL_HOST': 'pgvector',
            'LANYING_CONNECTOR_SQL_POOL_MIN_CONNECTION': '2',
            'LANYING_CONNECTOR_SQL_POOL_MAX_CONNECTION': '32'}):
        spec.loader.exec_module(module)
    return module


class FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __init__(self):
        self.closed = False
        self.context_exits = []
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.context_exits.append(exc_type)
        return False

    def cursor(self):
        cursor = FakeCursor()
        self.cursors.append(cursor)
        return cursor

    def close(self):
        self.closed = True


class FakePool:
    def __init__(self):
        self.connection = FakeConnection()
        self.puts = []
        self.closed = False

    def getconn(self):
        return self.connection

    def putconn(self, conn, close=False):
        self.puts.append(close)

    def closeall(self):
        self.closed = True


class PgvectorPoolTest(unittest.TestCase):
    def setUp(self):
        self.module = load_pgvector()
        self.pool = FakePool()
        self.factory = mock.patch.object(
            self.module.pool, 'ThreadedConnectionPool',
            return_value=self.pool)
        self.factory_mock = self.factory.start()
        self.addCleanup(self.factory.stop)

    def test_pool_is_lazy_and_reused_in_process(self):
        self.assertTrue(self.module.is_enabled())
        self.assertIsNone(self.module.connection_pool)

        with self.module.connection():
            pass
        with self.module.connection():
            pass

        self.factory_mock.assert_called_once()
        self.assertEqual(2, self.factory_mock.call_args.kwargs['minconn'])
        self.assertEqual(32, self.factory_mock.call_args.kwargs['maxconn'])
        self.assertEqual([False, False, False], self.pool.puts)

    def test_connection_is_returned_when_operation_fails(self):
        with self.assertRaises(ValueError):
            with self.module.connection():
                raise ValueError('failed query')

        self.assertEqual([False, False], self.pool.puts)
        self.assertIs(ValueError, self.pool.connection.context_exits[-1])

    def test_broken_connection_is_removed_from_pool(self):
        with self.assertRaises(self.module.psycopg2.OperationalError):
            with self.module.connection():
                raise self.module.psycopg2.OperationalError('connection lost')

        self.assertEqual([False, True], self.pool.puts)


if __name__ == '__main__':
    unittest.main()
