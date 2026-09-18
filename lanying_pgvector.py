import atexit
from contextlib import contextmanager
import logging
import os
import threading
import time

import psycopg2
from psycopg2 import pool


connection_pool = None
connection_pool_pid = None
connection_pool_lock = threading.Lock()

sql_pool_host = os.getenv('LANYING_CONNECTOR_SQL_POOL_HOST')
sql_pool_min_connection = int(os.getenv(
    'LANYING_CONNECTOR_SQL_POOL_MIN_CONNECTION', '2'))
sql_pool_max_connection = int(os.getenv(
    'LANYING_CONNECTOR_SQL_POOL_MAX_CONNECTION', '32'))
sql_pool_db_name = os.getenv('LANYING_CONNECTOR_SQL_POOL_DBNAME', 'maxim')
sql_pool_port = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_PORT', '5432'))
sql_pool_user = os.getenv('LANYING_CONNECTOR_SQL_POOL_USER', 'user')
sql_pool_password = os.getenv('LANYING_CONNECTOR_SQL_POOL_PASSWORD', '')


def _create_connection_pool():
    if sql_pool_min_connection < 1:
        raise ValueError('pgvector pool min connections must be at least 1')
    if sql_pool_max_connection < sql_pool_min_connection:
        raise ValueError(
            'pgvector pool max connections must not be less than min connections')
    created_pool = pool.ThreadedConnectionPool(
        minconn=sql_pool_min_connection,
        maxconn=sql_pool_max_connection,
        dbname=sql_pool_db_name,
        user=sql_pool_user,
        password=sql_pool_password,
        host=sql_pool_host,
        port=sql_pool_port,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    conn = created_pool.getconn()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('CREATE EXTENSION IF NOT EXISTS vector;')
    except Exception:
        created_pool.putconn(conn, close=True)
        created_pool.closeall()
        raise
    created_pool.putconn(conn)
    logging.info(
        'initialized pgvector connection pool | pid:%s | min:%s | max:%s',
        os.getpid(), sql_pool_min_connection, sql_pool_max_connection)
    return created_pool


def _get_connection_pool():
    global connection_pool, connection_pool_pid
    if not sql_pool_host:
        return None
    current_pid = os.getpid()
    if connection_pool is not None and connection_pool_pid == current_pid:
        return connection_pool
    with connection_pool_lock:
        if connection_pool is not None and connection_pool_pid == current_pid:
            return connection_pool
        # Never reuse PostgreSQL connections inherited across a process fork.
        connection_pool = _create_connection_pool()
        connection_pool_pid = current_pid
        return connection_pool


def get_connection():
    current_pool = _get_connection_pool()
    if current_pool:
        retry_times = 30
        for i in range(retry_times):
            conn = current_pool.getconn()
            if is_connection_valid(conn):
                return conn
            logging.info('get_connection | get bad connection: %s/%s', i, retry_times)
            current_pool.putconn(conn, close=True)
            if i == retry_times - 1:
                raise Exception('fail to get pgvector connection')
            time.sleep(0.1)
    return None


def put_connection(conn, close=False):
    if conn is None:
        return None
    if connection_pool is not None and connection_pool_pid == os.getpid():
        return connection_pool.putconn(conn, close=close)
    conn.close()
    return None


@contextmanager
def connection():
    conn = get_connection()
    if conn is None:
        raise RuntimeError('pgvector is not configured')
    close = False
    try:
        with conn:
            yield conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        close = True
        raise
    finally:
        put_connection(conn, close=close)


def is_enabled():
    return bool(sql_pool_host)


def is_connection_valid(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return result and result[0] == 1
    except (psycopg2.OperationalError, psycopg2.InterfaceError,
            psycopg2.DatabaseError):
        return False
    except Exception as e:
        logging.info("is_connection_valid got other exception")
        logging.exception(e)
        return False


def close_connection_pool():
    global connection_pool, connection_pool_pid
    if connection_pool is None or connection_pool_pid != os.getpid():
        return
    connection_pool.closeall()
    connection_pool = None
    connection_pool_pid = None


atexit.register(close_connection_pool)
