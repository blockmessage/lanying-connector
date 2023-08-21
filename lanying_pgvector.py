import psycopg2
from psycopg2 import pool
import os

connection_pool = None
sql_pool_host = os.getenv('LANYING_CONNECTOR_SQL_POOL_HOST')
if sql_pool_host:
    sql_pool_min_connection = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_MIN_CONNECTION', '5'))
    sql_pool_max_connection = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_MAX_CONNECTION', '100'))
    sql_pool_db_name = os.getenv('LANYING_CONNECTOR_SQL_POOL_DBNAME', 'maxim')
    sql_pool_port = int(os.getenv('LANYING_CONNECTOR_SQL_POOL_PORT', '5432'))
    sql_pool_user = os.getenv('LANYING_CONNECTOR_SQL_POOL_USER', 'user')
    sql_pool_password = os.getenv('LANYING_CONNECTOR_SQL_POOL_PASSWORD', '')
    # 创建连接池
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=sql_pool_min_connection,
        maxconn=sql_pool_max_connection,
        dbname=sql_pool_db_name,
        user=sql_pool_user,
        password=sql_pool_password,
        host=sql_pool_host,
        port=sql_pool_port
    )

def get_connection():
    if connection_pool:
        return connection_pool.getconn()
