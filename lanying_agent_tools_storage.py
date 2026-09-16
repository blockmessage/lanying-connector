"""MySQL persistence and shared connection pool for Seenical runtime data.

The database is intentionally separate from the pgvector database and also
stores Agent Tools data, conversation bindings, and operational logs. Schema creation is handled by
sql/seenical_agent_tools_mysql.sql so the runtime user only needs normal DML
permissions.
"""

import json
import logging
import os
import threading

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


_engine = None
_engine_lock = threading.Lock()


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def _load(value, default=None):
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def is_enabled():
    return bool(str(os.getenv('LANYING_AGENT_TOOLS_MYSQL_HOST', '')).strip())


def _truthy(value):
    return str(value or '').strip().lower() in ['1', 'true', 'yes', 'on']


def is_feature_enabled(app_id, chatbot_id=''):
    """Return whether Agent Tools are enabled for this App or Chatbot."""
    if not _truthy(os.getenv('LANYING_AGENT_TOOLS_PLATFORM_ENABLED', 'off')):
        return False
    try:
        import lanying_redis
        redis = lanying_redis.get_redis_connection()
        for key in [
                f'lanying_connector:agent_tools:feature:{app_id}:{chatbot_id or "*"}',
                f'lanying_connector:agent_tools:feature:{app_id}:*']:
            value = lanying_redis.redis_get(redis, key)
            if value is not None:
                return _truthy(value)
    except Exception:
        logging.exception('failed to read Agent Tools feature state')
    return False


def should_save_config_revision(app_id, chatbot_id=''):
    return is_enabled() and is_feature_enabled(app_id, chatbot_id)


def _get_engine():
    global _engine
    if not is_enabled():
        return None
    if _engine is not None:
        return _engine
    with _engine_lock:
        if _engine is not None:
            return _engine
        url = URL.create(
            'mysql+pymysql',
            username=os.getenv('LANYING_AGENT_TOOLS_MYSQL_USER', 'seenical'),
            password=os.getenv('LANYING_AGENT_TOOLS_MYSQL_PASSWORD', ''),
            host=os.getenv('LANYING_AGENT_TOOLS_MYSQL_HOST'),
            port=int(os.getenv('LANYING_AGENT_TOOLS_MYSQL_PORT', '3306')),
            database=os.getenv('LANYING_AGENT_TOOLS_MYSQL_DBNAME', 'seenical_agent_tools'),
            query={'charset': 'utf8mb4'},
        )
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={
                'connect_timeout': int(os.getenv(
                    'LANYING_AGENT_TOOLS_MYSQL_CONNECT_TIMEOUT_SECONDS', '10')),
                'read_timeout': int(os.getenv(
                    'LANYING_AGENT_TOOLS_MYSQL_READ_TIMEOUT_SECONDS', '10')),
                'write_timeout': int(os.getenv(
                    'LANYING_AGENT_TOOLS_MYSQL_WRITE_TIMEOUT_SECONDS', '10')),
            },
            pool_recycle=int(os.getenv(
                'LANYING_AGENT_TOOLS_MYSQL_POOL_RECYCLE_SECONDS', '1800')),
            pool_size=int(os.getenv('LANYING_AGENT_TOOLS_MYSQL_POOL_SIZE', '5')),
            max_overflow=int(os.getenv(
                'LANYING_AGENT_TOOLS_MYSQL_MAX_OVERFLOW', '10')),
        )
        return _engine


def get_engine():
    """Return the shared Connector MySQL engine."""
    return _get_engine()


def append_agent_tool_audit_log(entry):
    if not isinstance(entry, dict):
        return {'result': 'ignored', 'message': 'bad log entry'}
    engine = _get_engine()
    if engine is None:
        return {'result': 'ignored', 'message': 'Agent Tools MySQL disabled'}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO agent_tool_audit_log (
                `app_id`, `request_id`, `event`, `chatbot_id`, `conversation_type`,
                `conversation_id`, `actor_subject_id`, `tool_id`, `tool_version`,
                `skill_versions`, `arguments_hash`, `result_status`, `diff_summary`,
                `extra_metadata`
            ) VALUES (
                :app_id, :request_id, :event, :chatbot_id, :conversation_type,
                :conversation_id, :actor_subject_id, :tool_id, :tool_version,
                :skill_versions, :arguments_hash, :result_status, :diff_summary,
                :extra_metadata
            )
        """), {
            'app_id': str(entry.get('app_id', '')),
            'request_id': str(entry.get('request_id', '')),
            'event': str(entry.get('event', '')),
            'chatbot_id': str(entry.get('chatbot_id', '')),
            'conversation_type': str(entry.get('conversation_type', '')),
            'conversation_id': str(entry.get('conversation_id', '')),
            'actor_subject_id': str(entry.get('actor_subject_id', '')),
            'tool_id': str(entry.get('tool_id', '')),
            'tool_version': int(entry.get('tool_version', 0) or 0),
            'skill_versions': _json(entry.get('skill_versions', [])),
            'arguments_hash': str(entry.get('arguments_hash', '')),
            'result_status': str(entry.get('result_status', '')),
            'diff_summary': _json(entry.get('diff_summary', {})),
            'extra_metadata': _json(entry.get('extra_metadata', {})),
        })
    return {'result': 'ok'}


def save_agent_tool_request_view(snapshot):
    engine = _get_engine()
    if not isinstance(snapshot, dict) or engine is None:
        return {'result': 'ignored', 'message': 'Agent Tools MySQL disabled'}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO agent_tool_request_view (
                request_id, app_id, actor_subject_id, status, expires_at,
                retain_until, snapshot
            ) VALUES (
                :request_id, :app_id, :actor_subject_id, :status,
                FROM_UNIXTIME(:expires_at), DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 30 DAY),
                :snapshot
            )
            ON DUPLICATE KEY UPDATE
                actor_subject_id=VALUES(actor_subject_id),
                status=VALUES(status),
                expires_at=VALUES(expires_at),
                retain_until=VALUES(retain_until),
                snapshot=VALUES(snapshot),
                updated_at=CURRENT_TIMESTAMP(3)
        """), {
            'request_id': str(snapshot.get('request_id', '')),
            'app_id': str(snapshot.get('app_id', '')),
            'actor_subject_id': str(snapshot.get('actor_subject_id', '')),
            'status': str(snapshot.get('status', '')),
            'expires_at': int(snapshot.get('expires_at', 0) or 0),
            'snapshot': _json(snapshot),
        })
    return {'result': 'ok'}


def get_agent_tool_request_view(app_id, request_id):
    engine = _get_engine()
    if engine is None:
        return None
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT snapshot
            FROM agent_tool_request_view
            WHERE request_id=:request_id AND app_id=:app_id
              AND retain_until >= CURRENT_TIMESTAMP(3)
        """), {
            'request_id': str(request_id),
            'app_id': str(app_id),
        }).scalar_one_or_none()
    return _load(value)


def save_public_skill_catalog(catalog):
    engine = _get_engine()
    if not isinstance(catalog, dict) or engine is None:
        return {'result': 'error', 'message': 'Agent Tools MySQL disabled'}
    revision = str(catalog.get('revision', ''))
    source_commit = str(catalog.get('source_commit', ''))
    manifest_sha = str(catalog.get('manifest_sha', ''))
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT IGNORE INTO public_skill_catalog_revision (
                revision, source_commit, manifest_sha, catalog
            ) VALUES (:revision, :source_commit, :manifest_sha, :catalog)
        """), {
            'revision': revision,
            'source_commit': source_commit,
            'manifest_sha': manifest_sha,
            'catalog': _json(catalog),
        })
        for skill in catalog.get('skills', []):
            conn.execute(text("""
                INSERT IGNORE INTO public_skill_revision (
                    skill_id, revision, source_commit, skill
                ) VALUES (:skill_id, :revision, :source_commit, :skill)
            """), {
                'skill_id': str(skill.get('skill_id', '')),
                'revision': str(skill.get('revision', '')),
                'source_commit': source_commit,
                'skill': _json(skill),
            })
        conn.execute(text("""
            INSERT INTO public_skill_catalog_state (
                state_key, active_revision, source_commit
            ) VALUES ('active', :revision, :source_commit)
            ON DUPLICATE KEY UPDATE
                active_revision=VALUES(active_revision),
                source_commit=VALUES(source_commit),
                updated_at=CURRENT_TIMESTAMP(3)
        """), {'revision': revision, 'source_commit': source_commit})
    return {'result': 'ok'}


def get_active_public_skill_catalog():
    engine = _get_engine()
    if engine is None:
        return None
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT r.catalog
            FROM public_skill_catalog_state s
            JOIN public_skill_catalog_revision r
              ON r.revision=s.active_revision
            WHERE s.state_key='active'
        """)).scalar_one_or_none()
    return _load(value)


def get_public_skill_revision(skill_id, revision):
    engine = _get_engine()
    if engine is None:
        return None
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT skill
            FROM public_skill_revision
            WHERE skill_id=:skill_id AND revision=:revision
        """), {
            'skill_id': str(skill_id),
            'revision': str(revision),
        }).scalar_one_or_none()
    return _load(value)


def save_seenical_config_revision(app_id, resource_type, resource_id,
                                  revision, snapshot, request_id=''):
    engine = _get_engine()
    if engine is None:
        return {'result': 'error', 'message': 'Agent Tools MySQL disabled'}
    if resource_type not in ['agent', 'plan', 'site'] or not isinstance(snapshot, dict):
        return {'result': 'error', 'message': 'invalid configuration revision'}
    params = {
        'app_id': str(app_id),
        'resource_type': str(resource_type),
        'resource_id': str(resource_id),
        'revision': int(revision),
        'snapshot': _json(snapshot),
        'request_id': str(request_id or ''),
    }
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT IGNORE INTO seenical_config_revision (
                app_id, resource_type, resource_id, revision, snapshot,
                request_id
            ) VALUES (
                :app_id, :resource_type, :resource_id, :revision, :snapshot,
                :request_id
            )
        """), params)
        if result.rowcount == 0:
            value = conn.execute(text("""
                SELECT snapshot
                FROM seenical_config_revision
                WHERE app_id=:app_id AND resource_type=:resource_type
                  AND resource_id=:resource_id AND revision=:revision
            """), params).scalar_one_or_none()
            if _load(value) != snapshot:
                return {'result': 'error',
                        'message': 'configuration revision is inconsistent'}
    return {'result': 'ok'}


def get_seenical_config_revision(app_id, resource_type, resource_id, revision):
    engine = _get_engine()
    if engine is None:
        return None
    with engine.connect() as conn:
        value = conn.execute(text("""
            SELECT snapshot
            FROM seenical_config_revision
            WHERE app_id=:app_id AND resource_type=:resource_type
              AND resource_id=:resource_id AND revision=:revision
        """), {
            'app_id': str(app_id),
            'resource_type': str(resource_type),
            'resource_id': str(resource_id),
            'revision': int(revision),
        }).scalar_one_or_none()
    return _load(value)


def list_seenical_config_revisions(app_id, resource_type, resource_id,
                                   limit=20):
    engine = _get_engine()
    if engine is None:
        return []
    bounded_limit = min(100, max(1, int(limit)))
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT revision
            FROM seenical_config_revision
            WHERE app_id=:app_id AND resource_type=:resource_type
              AND resource_id=:resource_id
            ORDER BY revision DESC
            LIMIT :limit
        """), {
            'app_id': str(app_id),
            'resource_type': str(resource_type),
            'resource_id': str(resource_id),
            'limit': bounded_limit,
        }).scalars().all()
    return [int(value) for value in rows]


def _conversation_binding(row):
    if row is None:
        return None
    value = dict(row)
    for field in ['app_id', 'seenical_session_id', 'chatbot_id',
                  'agent_user_id', 'conversation_type', 'conversation_id',
                  'conversation_name', 'task_id', 'bound_im_user_id',
                  'status']:
        value[field] = '' if value.get(field) is None else str(value[field])
    value['revision'] = int(value.get('revision', 0) or 0)
    for field in ['created_at', 'updated_at']:
        timestamp = value.get(field)
        if timestamp is not None:
            value[field] = timestamp.isoformat(timespec='milliseconds')
    return value


def save_seenical_conversation_binding(binding):
    """Persist one verified Seenical child conversation and optional Loop."""
    engine = _get_engine()
    if engine is None:
        return {'result': 'error', 'message': 'Agent Tools MySQL disabled'}
    params = {
        'app_id': str(binding.get('app_id', '')),
        'seenical_session_id': str(binding.get('seenical_session_id', '')),
        'chatbot_id': str(binding.get('chatbot_id', '')),
        'agent_user_id': str(binding.get('agent_user_id', '')),
        'conversation_type': str(binding.get('conversation_type', '')),
        'conversation_id': str(binding.get('conversation_id', '')),
        'conversation_name': str(binding.get('conversation_name', ''))[:255],
        'task_id': str(binding.get('task_id', '')).strip() or None,
        'bound_im_user_id': str(binding.get('bound_im_user_id', '')),
    }
    required = [field for field in [
        'app_id', 'seenical_session_id', 'chatbot_id', 'agent_user_id',
        'conversation_type', 'conversation_id', 'bound_im_user_id'
    ] if not params[field]]
    if required:
        return {'result': 'error', 'message': 'invalid conversation binding'}
    unbound_task_ids = []
    with engine.begin() as conn:
        session_row = conn.execute(text("""
            SELECT seenical_session_id, conversation_type, conversation_id,
                   chatbot_id, agent_user_id, conversation_name, task_id,
                   bound_im_user_id, status
            FROM seenical_conversation_binding
            WHERE app_id=:app_id AND seenical_session_id=:seenical_session_id
            FOR UPDATE
        """), params).mappings().first()
        target_row = conn.execute(text("""
            SELECT seenical_session_id
            FROM seenical_conversation_binding
            WHERE app_id=:app_id AND conversation_type=:conversation_type
              AND conversation_id=:conversation_id
            FOR UPDATE
        """), params).mappings().first()
        if target_row and str(target_row['seenical_session_id']) != params['seenical_session_id']:
            return {'result': 'error', 'message': 'conversation is already registered'}
        if session_row and (
                str(session_row['conversation_type']) != params['conversation_type']
                or str(session_row['conversation_id']) != params['conversation_id']):
            return {'result': 'error', 'message': 'session is already registered'}
        if session_row:
            effective_task_id = params['task_id'] or (
                str(session_row.get('task_id') or '') or None)
            unchanged = (
                str(session_row.get('chatbot_id', '')) == params['chatbot_id']
                and str(session_row.get('agent_user_id', '')) == params['agent_user_id']
                and str(session_row.get('conversation_name', '')) == params['conversation_name']
                and (str(session_row.get('task_id') or '') or None) == effective_task_id
                and str(session_row.get('bound_im_user_id', '')) == params['bound_im_user_id']
                and str(session_row.get('status', '')) == 'ACTIVE')
            if unchanged:
                return {'result': 'ok', 'data': {'unbound_task_ids': []}}
        if params['task_id']:
            if (session_row and session_row.get('task_id')
                    and str(session_row['task_id']) != params['task_id']):
                unbound_task_ids.append(str(session_row['task_id']))
            task_row = conn.execute(text("""
                SELECT seenical_session_id
                FROM seenical_conversation_binding
                WHERE app_id=:app_id AND task_id=:task_id
                FOR UPDATE
            """), params).mappings().first()
            if task_row and str(task_row['seenical_session_id']) != params['seenical_session_id']:
                conn.execute(text("""
                    UPDATE seenical_conversation_binding
                    SET task_id=NULL, revision=revision + 1
                    WHERE app_id=:app_id AND task_id=:task_id
                """), params)
        if session_row:
            conn.execute(text("""
                UPDATE seenical_conversation_binding
                SET chatbot_id=:chatbot_id, agent_user_id=:agent_user_id,
                    conversation_name=:conversation_name,
                    task_id=COALESCE(:task_id, task_id),
                    bound_im_user_id=:bound_im_user_id, status='ACTIVE',
                    revision=revision + 1
                WHERE app_id=:app_id
                  AND seenical_session_id=:seenical_session_id
            """), params)
        else:
            conn.execute(text("""
                INSERT INTO seenical_conversation_binding (
                    app_id, seenical_session_id, chatbot_id, agent_user_id,
                    conversation_type, conversation_id, conversation_name,
                    task_id, bound_im_user_id, status, revision
                ) VALUES (
                    :app_id, :seenical_session_id, :chatbot_id, :agent_user_id,
                    :conversation_type, :conversation_id, :conversation_name,
                    :task_id, :bound_im_user_id, 'ACTIVE', 1
                )
            """), params)
    return {'result': 'ok', 'data': {
        'unbound_task_ids': unbound_task_ids,
    }}


def list_seenical_conversation_bindings(app_id):
    engine = _get_engine()
    if engine is None:
        raise RuntimeError('Agent Tools MySQL disabled')
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT app_id, seenical_session_id, chatbot_id, agent_user_id,
                   conversation_type, conversation_id, conversation_name,
                   task_id, bound_im_user_id, status, revision,
                   created_at, updated_at
            FROM seenical_conversation_binding
            WHERE app_id=:app_id AND status='ACTIVE'
            ORDER BY created_at ASC
        """), {'app_id': str(app_id)}).mappings().all()
    return [_conversation_binding(row) for row in rows]


def deactivate_seenical_conversation_binding(app_id, seenical_session_id,
                                             conversation_type,
                                             conversation_id,
                                             bound_im_user_id):
    """Deactivate one App-owned Seenical child conversation."""
    engine = _get_engine()
    if engine is None:
        return {'result': 'error', 'message': 'Agent Tools MySQL disabled'}
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE seenical_conversation_binding
            SET task_id=NULL, status='INACTIVE', revision=revision + 1
            WHERE app_id=:app_id
              AND seenical_session_id=:seenical_session_id
              AND conversation_type=:conversation_type
              AND conversation_id=:conversation_id
              AND bound_im_user_id=:bound_im_user_id
              AND task_id IS NULL
        """), {
            'app_id': str(app_id),
            'seenical_session_id': str(seenical_session_id),
            'conversation_type': str(conversation_type),
            'conversation_id': str(conversation_id),
            'bound_im_user_id': str(bound_im_user_id),
        })
    if result.rowcount == 0:
        with engine.connect() as conn:
            existing = conn.execute(text("""
                SELECT conversation_type, conversation_id, task_id
                FROM seenical_conversation_binding
                WHERE app_id=:app_id
                  AND seenical_session_id=:seenical_session_id
                  AND bound_im_user_id=:bound_im_user_id
            """), {
                'app_id': str(app_id),
                'seenical_session_id': str(seenical_session_id),
                'bound_im_user_id': str(bound_im_user_id),
            }).mappings().first()
        if existing:
            if existing.get('task_id') is not None:
                return {'result': 'error',
                        'message': 'Seenical conversation has a bound LOOP'}
            return {'result': 'error', 'message': 'Seenical conversation mismatch'}
        # Legacy local/IM conversations may not have reached server storage yet.
        # Treat their absence as an idempotent unregister operation.
        return {'result': 'ok'}
    return {'result': 'ok'}


def get_seenical_loop_conversation_binding(app_id, task_id):
    engine = _get_engine()
    if engine is None:
        return None
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT app_id, seenical_session_id, chatbot_id, agent_user_id,
                   conversation_type, conversation_id, conversation_name,
                   task_id, bound_im_user_id, status, revision,
                   created_at, updated_at
            FROM seenical_conversation_binding
            WHERE app_id=:app_id AND task_id=:task_id AND status='ACTIVE'
        """), {
            'app_id': str(app_id), 'task_id': str(task_id)
        }).mappings().first()
    return _conversation_binding(row)


def unbind_seenical_loop_conversation(app_id, task_id):
    engine = _get_engine()
    if engine is None:
        return {'result': 'error', 'message': 'Agent Tools MySQL disabled'}
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE seenical_conversation_binding
            SET task_id=NULL, revision=revision + 1
            WHERE app_id=:app_id AND task_id=:task_id
        """), {'app_id': str(app_id), 'task_id': str(task_id)})
    return {'result': 'ok'}
