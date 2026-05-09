import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json
import os
import logging
import time
import threading

connection_pool = None
openclaw_session_map_log_table_ready = False
openclaw_session_map_log_table_lock = threading.Lock()

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

def is_enabled():
    return connection_pool is not None

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

def ensure_openclaw_session_map_log_table():
    global openclaw_session_map_log_table_ready
    if not is_enabled():
        return False
    if openclaw_session_map_log_table_ready:
        return True
    with openclaw_session_map_log_table_lock:
        if openclaw_session_map_log_table_ready:
            return True
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS openclaw_session_map_log (
                    id bigserial PRIMARY KEY,
                    created_at timestamptz NOT NULL DEFAULT NOW(),
                    app_id varchar(100) NOT NULL DEFAULT '',
                    node_id varchar(100) NOT NULL DEFAULT '',
                    session_key text NOT NULL DEFAULT '',
                    group_id varchar(100) NOT NULL DEFAULT '',
                    openclaw_user_id varchar(100) NOT NULL DEFAULT '',
                    change_source varchar(100) NOT NULL DEFAULT '',
                    previous_signature jsonb NOT NULL DEFAULT '{}'::jsonb,
                    new_signature jsonb NOT NULL DEFAULT '{}'::jsonb,
                    previous_mapping jsonb NOT NULL DEFAULT '{}'::jsonb,
                    new_mapping jsonb NOT NULL DEFAULT '{}'::jsonb,
                    legacy_session_keys jsonb NOT NULL DEFAULT '[]'::jsonb,
                    extra_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS openclaw_session_map_log_idx_session_created_at
                ON openclaw_session_map_log (app_id, node_id, session_key, created_at DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS openclaw_session_map_log_idx_created_at
                ON openclaw_session_map_log (created_at DESC);
            """)
            conn.commit()
            cursor.close()
            put_connection(conn)
        openclaw_session_map_log_table_ready = True
        return True

def append_openclaw_session_map_log(entry):
    if not isinstance(entry, dict):
        return {
            'result': 'ignored',
            'message': 'bad log entry'
        }
    if not is_enabled():
        return {
            'result': 'ignored',
            'message': 'pgvector disabled'
        }
    ensure_openclaw_session_map_log_table()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO openclaw_session_map_log (
                app_id,
                node_id,
                session_key,
                group_id,
                openclaw_user_id,
                change_source,
                previous_signature,
                new_signature,
                previous_mapping,
                new_mapping,
                legacy_session_keys,
                extra_metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            [
                str(entry.get('app_id', '')).strip(),
                str(entry.get('node_id', '')).strip(),
                str(entry.get('session_key', '')).strip(),
                str(entry.get('group_id', '')).strip(),
                str(entry.get('openclaw_user_id', '')).strip(),
                str(entry.get('change_source', '')).strip(),
                Json(entry.get('previous_signature', {})),
                Json(entry.get('new_signature', {})),
                Json(entry.get('previous_mapping', {})),
                Json(entry.get('new_mapping', {})),
                Json(entry.get('legacy_session_keys', [])),
                Json(entry.get('extra_metadata', {})),
            ]
        )
        conn.commit()
        cursor.close()
        put_connection(conn)
    return {
        'result': 'ok'
    }

def list_openclaw_session_map_logs(app_id, node_id, limit=100):
    if not is_enabled():
        return []
    normalized_app_id = str(app_id or '').strip()
    normalized_node_id = str(node_id or '').strip()
    normalized_limit = int(limit or 100)
    if normalized_app_id == '' or normalized_node_id == '' or normalized_limit <= 0:
        return []
    ensure_openclaw_session_map_log_table()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id,
                created_at,
                app_id,
                node_id,
                session_key,
                group_id,
                openclaw_user_id,
                change_source,
                previous_signature,
                new_signature,
                previous_mapping,
                new_mapping,
                legacy_session_keys,
                extra_metadata
            FROM openclaw_session_map_log
            WHERE app_id = %s AND node_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT %s;
            """,
            [normalized_app_id, normalized_node_id, normalized_limit]
        )
        rows = cursor.fetchall()
        cursor.close()
        put_connection(conn)
    results = []
    for row in rows:
        results.append({
            'id': row[0],
            'created_at': row[1].isoformat() if row[1] is not None else '',
            'app_id': row[2],
            'node_id': row[3],
            'session_key': row[4],
            'group_id': row[5],
            'openclaw_user_id': row[6],
            'change_source': row[7],
            'previous_signature': row[8] or {},
            'new_signature': row[9] or {},
            'previous_mapping': row[10] or {},
            'new_mapping': row[11] or {},
            'legacy_session_keys': row[12] or [],
            'extra_metadata': row[13] or {},
        })
    return results

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
