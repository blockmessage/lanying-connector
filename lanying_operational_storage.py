"""MySQL persistence for Connector operational logs."""

import json

from sqlalchemy import text

import lanying_agent_tools_storage


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def append_message_quota_usage_log(entry):
    if not isinstance(entry, dict):
        return {'result': 'ignored', 'message': 'bad log entry'}
    engine = lanying_agent_tools_storage.get_engine()
    if engine is None:
        return {'result': 'ignored', 'message': 'Connector MySQL disabled'}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO message_quota_usage_log (
                app_id, quota, model_type, vendor, model, api_key_type,
                message_count, total_tokens, prompt_tokens, completion_tokens,
                text_size, content_security, product_id, extra_metadata
            ) VALUES (
                :app_id, :quota, :model_type, :vendor, :model, :api_key_type,
                :message_count, :total_tokens, :prompt_tokens, :completion_tokens,
                :text_size, :content_security, :product_id, :extra_metadata
            )
        """), {
            'app_id': str(entry.get('app_id', '')).strip(),
            'quota': float(entry.get('quota', 0) or 0),
            'model_type': str(entry.get('model_type', '')).strip(),
            'vendor': str(entry.get('vendor', '')).strip(),
            'model': str(entry.get('model', '')).strip(),
            'api_key_type': str(entry.get('api_key_type', '')).strip(),
            'message_count': int(entry.get('message_count', 1) or 0),
            'total_tokens': int(entry.get('total_tokens', 0) or 0),
            'prompt_tokens': int(entry.get('prompt_tokens', 0) or 0),
            'completion_tokens': int(entry.get('completion_tokens', 0) or 0),
            'text_size': int(entry.get('text_size', 0) or 0),
            'content_security': str(entry.get('content_security', '')).strip(),
            'product_id': int(entry.get('product_id', 0) or 0),
            'extra_metadata': _json(entry.get('extra_metadata', {})),
        })
    return {'result': 'ok'}


def list_message_quota_usage_logs(app_id='', limit=100):
    engine = lanying_agent_tools_storage.get_engine()
    if engine is None:
        return []
    bounded_limit = min(1000, max(1, int(limit or 100)))
    normalized_app_id = str(app_id or '').strip()
    where = 'WHERE app_id=:app_id' if normalized_app_id else ''
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, created_at, app_id, quota, model_type, vendor, model,
                   api_key_type, message_count, total_tokens, prompt_tokens,
                   completion_tokens, text_size, content_security, product_id,
                   extra_metadata
            FROM message_quota_usage_log
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
        """), {
            'app_id': normalized_app_id,
            'limit': bounded_limit,
        }).fetchall()
    return [{
        'id': row[0],
        'created_at': row[1].isoformat() if row[1] is not None else '',
        'app_id': row[2],
        'quota': float(row[3] or 0),
        'model_type': row[4],
        'vendor': row[5],
        'model': row[6],
        'api_key_type': row[7],
        'message_count': row[8] or 0,
        'total_tokens': row[9] or 0,
        'prompt_tokens': row[10] or 0,
        'completion_tokens': row[11] or 0,
        'text_size': row[12] or 0,
        'content_security': row[13],
        'product_id': row[14] or 0,
        'extra_metadata': json.loads(row[15] or '{}'),
    } for row in rows]


def append_openclaw_session_map_log(entry):
    if not isinstance(entry, dict):
        return {'result': 'ignored', 'message': 'bad log entry'}
    engine = lanying_agent_tools_storage.get_engine()
    if engine is None:
        return {'result': 'ignored', 'message': 'Connector MySQL disabled'}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO openclaw_session_map_log (
                app_id, node_id, session_key, group_id, openclaw_user_id,
                change_source, previous_signature, new_signature,
                previous_mapping, new_mapping, legacy_session_keys,
                extra_metadata
            ) VALUES (
                :app_id, :node_id, :session_key, :group_id, :openclaw_user_id,
                :change_source, :previous_signature, :new_signature,
                :previous_mapping, :new_mapping, :legacy_session_keys,
                :extra_metadata
            )
        """), {
            'app_id': str(entry.get('app_id', '')).strip(),
            'node_id': str(entry.get('node_id', '')).strip(),
            'session_key': str(entry.get('session_key', '')).strip(),
            'group_id': str(entry.get('group_id', '')).strip(),
            'openclaw_user_id': str(entry.get('openclaw_user_id', '')).strip(),
            'change_source': str(entry.get('change_source', '')).strip(),
            'previous_signature': _json(entry.get('previous_signature', {})),
            'new_signature': _json(entry.get('new_signature', {})),
            'previous_mapping': _json(entry.get('previous_mapping', {})),
            'new_mapping': _json(entry.get('new_mapping', {})),
            'legacy_session_keys': _json(entry.get('legacy_session_keys', [])),
            'extra_metadata': _json(entry.get('extra_metadata', {})),
        })
    return {'result': 'ok'}


def list_openclaw_session_map_logs(app_id, node_id, limit=100):
    engine = lanying_agent_tools_storage.get_engine()
    normalized_app_id = str(app_id or '').strip()
    normalized_node_id = str(node_id or '').strip()
    if engine is None or not normalized_app_id or not normalized_node_id:
        return []
    bounded_limit = min(1000, max(1, int(limit or 100)))
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, created_at, app_id, node_id, session_key, group_id,
                   openclaw_user_id, change_source, previous_signature,
                   new_signature, previous_mapping, new_mapping,
                   legacy_session_keys, extra_metadata
            FROM openclaw_session_map_log
            WHERE app_id=:app_id AND node_id=:node_id
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
        """), {
            'app_id': normalized_app_id,
            'node_id': normalized_node_id,
            'limit': bounded_limit,
        }).fetchall()
    return [{
        'id': row[0],
        'created_at': row[1].isoformat() if row[1] is not None else '',
        'app_id': row[2],
        'node_id': row[3],
        'session_key': row[4],
        'group_id': row[5],
        'openclaw_user_id': row[6],
        'change_source': row[7],
        'previous_signature': json.loads(row[8] or '{}'),
        'new_signature': json.loads(row[9] or '{}'),
        'previous_mapping': json.loads(row[10] or '{}'),
        'new_mapping': json.loads(row[11] or '{}'),
        'legacy_session_keys': json.loads(row[12] or '[]'),
        'extra_metadata': json.loads(row[13] or '{}'),
    } for row in rows]
