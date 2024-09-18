import psycopg2
from psycopg2 import pool
import os
import logging
import time

connection_pool = None

def get_connection():
    if connection_pool:
        retry_times = 30
        for i in range(retry_times):
            conn = connection_pool.getconn()
            if is_connection_valid(conn):
                return conn
            else:
                logging.info(f"get_connection | get bad connection: {i}/{retry_times}")
                connection_pool.putconn(conn, close=True)
                if i == retry_times-1:
                    raise Exception('fail to get pgvector connection')
                time.sleep(0.1)

def put_connection(conn):
    if connection_pool:
        return connection_pool.putconn(conn)

def is_connection_valid(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return result and result[0] == 1
    except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.DatabaseError):
        return False
    except Exception as e:
        logging.info("is_connection_valid got other exception")
        logging.exception(e)
        return False

sql_pool_host = os.getenv('LANYING_CONNECTOR_SQL_POOL_HOST')
if sql_pool_host:
    sql_pool_min_connection = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_MIN_CONNECTION', '5'))
    sql_pool_max_connection = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_MAX_CONNECTION', '100'))
    sql_pool_db_name = os.getenv('LANYING_CONNECTOR_SQL_POOL_DBNAME', 'maxim')
    sql_pool_port = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_PORT', '5432'))
    sql_pool_user = os.getenv('LANYING_CONNECTOR_SQL_POOL_USER', 'user')
    sql_pool_password = os.getenv('LANYING_CONNECTOR_SQL_POOL_PASSWORD', '')
    # 创建连接池
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
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
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        conn.commit()
        cursor.close()
        put_connection(conn)
