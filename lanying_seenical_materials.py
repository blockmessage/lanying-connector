"""Private session documents. MySQL owns references; Redis owns shared KB usage.

The space row serializes short reference/state transactions across workers.
Only the claimed document worker writes vectors or changes its usage counter.
No original object is deleted by this module.
"""
import json
import base64
import binascii
import logging
import os
import stat
import tempfile
import zipfile
import uuid
from urllib.parse import urlsplit, urljoin, parse_qs

from sqlalchemy import text
from redis.exceptions import ResponseError
from psycopg2.errors import UndefinedTable

import lanying_agent_tools_storage as storage

MAX_FILE_BYTES = 30 * 1024 * 1024
MAX_ARCHIVE_BYTES = 100 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 200
INTERNAL_TYPE = 'seenical_session'
BUSY = {'indexing', 'cleaning'}


def message_ext(msg):
    raw = msg.get('ext', {})
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        return {}


class MaterialError(ValueError):
    pass


def _engine():
    engine = storage.get_engine()
    if engine is None:
        raise MaterialError('material_storage_unavailable')
    return engine


def _space(conn, app_id, lock=False):
    row = conn.execute(text('SELECT * FROM seenical_material_space WHERE app_id=:a'
                            + (' FOR UPDATE' if lock else '')), {'a': str(app_id)}).mappings().first()
    return dict(row) if row else None


def ensure_space(app_id):
    """Reserve an identity without requiring a usable model or vector service."""
    import lanying_embedding as embedding
    name = '__seenical_session_materials__'
    with _engine().begin() as conn:
        conn.execute(text('INSERT IGNORE INTO seenical_material_space '
                          '(app_id,embedding_name) VALUES (:a,:n)'), {'a': str(app_id), 'n': name})
        row = _space(conn, app_id, True)
        if not row['embedding_uuid']:
            row['embedding_uuid'] = str(embedding.generate_embedding_id())
            conn.execute(text('UPDATE seenical_material_space SET embedding_uuid=:u WHERE app_id=:a'),
                         {'a': str(app_id), 'u': row['embedding_uuid']})
        return row


def initialize_index(app_id, space):
    import lanying_embedding as embedding
    # Several first uploads may be claimed by different workers. Serialize
    # initial metadata/index creation, never reset another document's counters.
    with _engine().begin() as conn:
        _space(conn, app_id, True)
        result = embedding.create_embedding(str(app_id), space['embedding_name'], 350, 'COSINE', [], '', 30,
            'openai', 'text-embedding-ada-002', INTERNAL_TYPE, reserved_uuid=space['embedding_uuid'])
    if result.get('result') != 'ok':
        raise MaterialError('material_model_unavailable')


def validate_ids(values):
    if not isinstance(values, list) or len(values) > 500 or any(
            not isinstance(v, str) or not v or len(v) > 100 for v in values):
        raise MaterialError('invalid_reference_document_ids')
    return sorted(set(values))


def reference_ids(app_id, owner_type, owner_id, active=True):
    if not storage.is_enabled():
        return []
    with _engine().connect() as conn:
        rows = conn.execute(text('SELECT doc_id FROM seenical_material_reference '
            'WHERE app_id=:a AND owner_type=:t AND owner_id=:o'
            + (' AND active=1' if active else '') + ' ORDER BY doc_id'),
            {'a': str(app_id), 't': owner_type, 'o': str(owner_id)}).all()
        return [r[0] for r in rows]


def _document(conn, app_id, doc_id):
    row = conn.execute(text('SELECT * FROM seenical_material WHERE app_id=:a AND doc_id=:d'),
                       {'a': str(app_id), 'd': doc_id}).mappings().first()
    return dict(row) if row else None


def _has_references(conn, app_id, doc_id):
    return conn.execute(text('SELECT 1 FROM seenical_material_reference '
        'WHERE app_id=:a AND doc_id=:d AND active=1 LIMIT 1'),
        {'a': str(app_id), 'd': doc_id}).first() is not None


def _set_state(conn, app_id, doc_id, status, error=''):
    conn.execute(text('UPDATE seenical_material SET status=:s,error_code=:e,'
                      'updated_at=CURRENT_TIMESTAMP WHERE app_id=:a AND doc_id=:d'),
                 {'a': str(app_id), 'd': doc_id, 's': status, 'e': error})


def set_references(app_id, owner_type, owner_id, values, require_ready=False, persist_owner=None):
    """Roll back reference changes if persisting their owning resource fails.

    This does not make MySQL and Redis a distributed transaction; it prevents
    a failed Redis write from committing orphan references or releasing inputs.
    """
    values = validate_ids(values)
    if not storage.is_enabled() and not values:
        if persist_owner:
            persist_owner()
        return
    with _engine().begin() as conn:
        space = _space(conn, app_id, True)
        if not space and not values:
            if persist_owner:
                persist_owner()
            return
        if values and not space:
            raise MaterialError('material_not_found')
        current = {r[0] for r in conn.execute(text('SELECT doc_id FROM seenical_material_reference '
            'WHERE app_id=:a AND owner_type=:t AND owner_id=:o AND active=1'),
            {'a': str(app_id), 't': owner_type, 'o': str(owner_id)}).all()}
        for doc_id in values:
            doc = _document(conn, app_id, doc_id)
            if not doc:
                raise MaterialError('material_not_found')
            if doc_id not in current and doc['status'] in {'cleaning', 'cleanup_failed', 'cleanup_pending', 'unindexed'}:
                raise MaterialError('material_cleaning')
            if require_ready and doc['status'] != 'ready':
                raise MaterialError('material_not_ready')
        params = {'a': str(app_id), 't': owner_type, 'o': str(owner_id)}
        conn.execute(text('UPDATE seenical_material_reference SET active=0 '
                          'WHERE app_id=:a AND owner_type=:t AND owner_id=:o'), params)
        for doc_id in values:
            conn.execute(text('INSERT INTO seenical_material_reference '
                '(app_id,owner_type,owner_id,doc_id,active) VALUES (:a,:t,:o,:d,1) '
                'ON DUPLICATE KEY UPDATE active=1'), dict(params, d=doc_id))
        if persist_owner:
            persist_owner()
    if owner_type != 'run' or not values:
        enqueue(app_id)


def release_owner(app_id, owner_type, owner_id, persist_owner=None):
    set_references(app_id, owner_type, owner_id, [], persist_owner=persist_owner)


def remove_reference(app_id, session_id, doc_id):
    # Removing one reference must not validate or rewrite unrelated documents.
    with _engine().begin() as conn:
        _space(conn, app_id, True)
        conn.execute(text("UPDATE seenical_material_reference SET active=0 "
            "WHERE app_id=:a AND owner_type='session' AND owner_id=:s AND doc_id=:d"),
            {'a': app_id, 's': session_id, 'd': doc_id})
    enqueue(app_id)


def enqueue(app_id):
    from lanying_tasks import seenical_materials_task
    try:
        seenical_materials_task.apply_async(args=[str(app_id)])
    except Exception:
        # The durable pending state remains visible and can be retried.
        logging.exception('session material queue unavailable | app_id:%s', app_id)


def authorized_conversation(app_id, actor, session_id):
    import lanying_agent_tools as tools
    result = tools.list_seenical_conversations(app_id, actor)
    if result.get('result') != 'ok':
        raise MaterialError('material_conversation_unavailable')
    match = next((v for v in result['data']['list']
                  if v['seenical_session_id'] == str(session_id)), None)
    if not match:
        raise MaterialError('material_conversation_unavailable')
    # Revalidate membership, ownership and server group metadata on access.
    result = tools._validate_seenical_conversation(app_id, actor, match)
    if result.get('result') != 'ok':
        raise MaterialError('material_conversation_unavailable')
    return match


def message_conversation(msg):
    """Derive identity from the real IM envelope, never from declared user IDs."""
    import lanying_agent_tools as tools
    import lanying_chatbot
    if not storage.is_enabled():
        return None
    app_id = str(msg['appId'])
    binding = tools.get_im_binding_projection(app_id) or {}
    sender = str(msg.get('from', {}).get('uid', ''))
    if binding.get('status') != 'BOUND' or str(binding.get('im_user_id')) != sender:
        return None
    actor = {'im_user_id': sender}
    opposite = str(msg.get('to', {}).get('uid', ''))
    kind = msg.get('type')
    if kind == 'CHAT':
        bot_id = lanying_chatbot.get_user_chatbot_id(app_id, opposite)
        if not bot_id:
            return None
        session_id = 'primary:' + str(bot_id)
    elif kind == 'GROUPCHAT':
        rows = tools.list_seenical_conversations(app_id, actor)
        if rows.get('result') != 'ok':
            return None
        found = next((v for v in rows['data']['list'] if v['conversation_type'] == kind
                      and v['conversation_id'] == opposite), None)
        if not found:
            return None
        session_id = found['seenical_session_id']
    else:
        return None
    try:
        return authorized_conversation(app_id, actor, session_id)
    except MaterialError:
        return None


def ingest_message(msg):
    marker = message_ext(msg).get('seenical', {})
    if msg.get('ctype') != 'FILE' or not isinstance(marker, dict) or marker.get('material_version') != 1:
        return False
    conversation = message_conversation(msg)
    if not conversation:
        raise MaterialError('material_conversation_unavailable')
    attachment = msg.get('attachment', {})
    if isinstance(attachment, str):
        try:
            attachment = json.loads(attachment)
        except ValueError:
            attachment = {}
    if not isinstance(attachment, dict):
        attachment = {}
    filename = os.path.basename(str(attachment.get('dName', '')))
    import lanying_embedding as embedding
    ext = os.path.splitext(filename)[1].lower()
    url = str(attachment.get('url', ''))
    error = ''
    try:
        validate_source(url, str(msg['appId']))
        validate_attachment_identity(url, msg)
        if ext not in embedding.allow_exts() and ext != '.zip':
            raise MaterialError('material_unsupported_format')
        try:
            declared_size = int(attachment.get('fLen', 0))
        except (ValueError, TypeError):
            raise MaterialError('material_message_invalid')
        if declared_size > MAX_FILE_BYTES:
            raise MaterialError('material_file_too_large')
    except MaterialError as exc:
        error = str(exc)
        if error == 'material_source_invalid':
            url = ''  # Never persist an unverified download target for retries.
    message_id = str(msg.get('msgId', ''))
    if not message_id:
        raise MaterialError('material_message_invalid')
    app_id = str(msg['appId'])
    space = ensure_space(app_id)
    with _engine().begin() as conn:
        _space(conn, app_id, True)
        existing = conn.execute(text('SELECT doc_id FROM seenical_material '
            'WHERE app_id=:a AND message_id=:m'), {'a': app_id, 'm': message_id}).first()
        if existing:
            return True  # A replay must not restore an explicitly removed association.
        doc_id = space['embedding_uuid'] + '-' + uuid.uuid4().hex
        conn.execute(text('INSERT INTO seenical_material '
            '(app_id,doc_id,message_id,filename,source_url,status,error_code) VALUES (:a,:d,:m,:n,:u,:s,:e)'),
            {'a': app_id, 'd': doc_id, 'm': message_id, 'n': filename[:255], 'u': url,
             's': 'failed' if error else 'pending', 'e': error})
        conn.execute(text('INSERT INTO seenical_material_reference '
            '(app_id,owner_type,owner_id,doc_id,active) VALUES (:a,\'session\',:s,:d,1)'),
            {'a': app_id, 's': conversation['seenical_session_id'], 'd': doc_id})
    if not error:
        enqueue(app_id)
    return True


def validate_source(url, app_id=None, redirect=False):
    parsed = urlsplit(url)
    configured_host = ''
    if app_id:
        import lanying_config
        configured_host = urlsplit(lanying_config.get_lanying_api_endpoint(app_id)).netloc
    trusted = (parsed.hostname or '').endswith('.maximtop.cn') or parsed.hostname == 'api.maximtop.com'
    trusted = trusted or bool(configured_host and parsed.netloc == configured_host)
    if redirect and parsed.scheme == 'https':
        trusted = trusted or (parsed.hostname or '').endswith(('.aliyuncs.com', '.lanyingim.com'))
    if (parsed.scheme not in {'http', 'https'} or (parsed.scheme != 'https' and parsed.netloc != configured_host)
            or not parsed.hostname or parsed.username
            or parsed.password or not trusted):
        raise MaterialError('material_source_invalid')
    if not redirect:
        query = parse_qs(parsed.query)
        if (parsed.path != '/file/download/chat' or not query.get('file_sign')
                or len(url) > 16384 or set(query) - {'file_sign'}):
            raise MaterialError('material_source_invalid')


def validate_attachment_identity(url, msg):
    """Check Ratel's attachment envelope; Ratel still verifies its signature.

    OSS/AWS use a signed base64 tuple; Ceph uses a JWT with the same IDs.
    Decoding here does not authenticate the signature or grant file access.
    """
    try:
        sign = parse_qs(urlsplit(url).query)['file_sign'][0]
        if sign.count('.') == 2:
            payload = sign.split('.')[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
            sender, target, kind = claims['from_id'], claims['to_id'], claims['to_type']
        else:
            sender, target, _, kind, _ = base64.b64decode(sign, validate=True).decode('utf-8').split('|')
        if (str(sender) != str(msg.get('from', {}).get('uid'))
                or str(target) != str(msg.get('to', {}).get('uid'))
                or int(kind) != (2 if msg.get('type') == 'GROUPCHAT' else 1)):
            raise ValueError('attachment conversation mismatch')
    except (ValueError, TypeError, KeyError, IndexError, binascii.Error):
        raise MaterialError('material_source_invalid')


def _download_source(app_id, url, filename):
    import requests
    import lanying_config
    config = lanying_config.get_lanying_connector(app_id)
    import lanying_agent_tools
    binding = lanying_agent_tools.get_im_binding_projection(app_id) or {}
    headers = {'app_id': app_id, 'access-token': config['lanying_admin_token'],
               'user_id': str(binding.get('im_user_id', ''))}
    # Only platform attachment hosts can receive credentials. Off-platform
    # redirects are rejected rather than forwarding an administrator token.
    validate_source(url, app_id)
    response = None
    for _ in range(4):
        response = requests.get(url, headers=headers, stream=True, timeout=(10, 60), allow_redirects=False)
        if response.status_code not in {301, 302, 303, 307, 308}:
            break
        target = urljoin(url, response.headers.get('Location', ''))
        response.close()
        validate_source(target, app_id, redirect=True)
        if urlsplit(target).netloc != urlsplit(url).netloc:
            headers = {}
        url = target
    with response:
        if response.status_code not in {200, 206}:
            raise MaterialError('material_download_failed')
        # The IM gateway wraps business errors in HTTP 200 JSON. A real chat
        # attachment is redirected to storage or returned as a file response.
        # Do not persist this envelope as a .txt/.md source (even temporarily).
        if 'json' in response.headers.get('Content-Type', '').lower():
            raise MaterialError('material_download_failed')
        size = 0
        with open(filename, 'wb') as output:
            for block in response.iter_content(65536):
                size += len(block)
                if size > MAX_FILE_BYTES:
                    raise MaterialError('material_file_too_large')
                output.write(block)
        if not size:
            raise MaterialError('material_empty_file')
    return size


def _store_source(app_id, space, doc):
    """Retain a verified original independently of indexing and its quota."""
    import lanying_file_storage as files
    if doc['object_name']:
        return
    ext = os.path.splitext(doc['filename'])[1].lower()
    with tempfile.TemporaryDirectory(prefix='seenical-source-') as directory:
        path = os.path.join(directory, 'source' + ext)
        size = _download_source(app_id, doc['source_url'], path)
        object_name = f'seenical/materials/{app_id}/{doc["doc_id"]}{ext}'
        if files.upload(object_name, path).get('result') != 'ok':
            raise MaterialError('material_upload_failed')
        with _engine().begin() as conn:
            conn.execute(text('UPDATE seenical_material SET object_name=:o,file_size=:s '
                'WHERE app_id=:a AND doc_id=:d'),
                {'a': app_id, 'd': doc['doc_id'], 'o': object_name, 's': size})
        doc.update(object_name=object_name, file_size=size)


def stored_document_metadata(app_id, doc_id):
    with _engine().connect() as conn:
        doc = _document(conn, app_id, doc_id)
    if not doc or not doc['object_name']:
        return None
    return dict(doc, type='file', ext=os.path.splitext(doc['filename'])[1].lower())


def _clear_index(app_id, space, doc):
    import lanying_embedding as embedding
    import lanying_redis
    info = embedding.get_embedding_uuid_info(space['embedding_uuid'])
    if not info:
        # A source can be saved before the model/index has ever been available.
        # Never refund an existing reservation when index metadata is missing.
        meta = embedding.get_doc(space['embedding_uuid'], doc['doc_id']) or {}
        if int(meta.get('storage_file_size', 0)):
            raise MaterialError('material_storage_unavailable')
        return
    try:
        embedding.search_doc_data_and_delete(app_id, space['embedding_name'], doc['doc_id'],
            info['index'], -1, info['db_type'], info.get('db_table_name', ''))
        if info['db_type'] == 'redis':
            query = embedding.Query(embedding.query_by_doc_id(doc['doc_id'])).no_content().paging(0, 1).dialect(2)
            if lanying_redis.get_redis_stack_connection().ft(info['index']).search(query).total:
                raise MaterialError('material_cleanup_incomplete')
    except Exception as exc:
        # Metadata precedes first index creation. A missing physical index has
        # no vectors to clean; retry can finish idempotent initialization later.
        # Never treat connection failures or other query errors as successful cleanup.
        missing = (info['db_type'] == 'redis' and isinstance(exc, ResponseError)
                   and str(exc).lower().startswith('unknown index name'))
        missing = missing or (info['db_type'] == 'pgvector' and isinstance(exc, UndefinedTable))
        if not missing:
            raise
    # One Redis transaction changes all usage levels, so worker redelivery
    # cannot double-refund or lose an App-level decrement after a crash.
    update_usage(app_id, space['embedding_uuid'], doc['doc_id'], 0, clear=True)


def update_usage(app_id, embedding_uuid, doc_id, size, clear=False):
    import lanying_embedding as embedding
    import lanying_redis
    from redis.exceptions import WatchError
    redis = lanying_redis.get_redis_stack_connection()
    doc_key = embedding.get_embedding_doc_info_key(embedding_uuid, doc_id)
    app_key = embedding.get_app_embedding_app_info_key(app_id)
    library_key = embedding.get_embedding_uuid_key(embedding_uuid)
    for _ in range(5):
        try:
            with redis.pipeline() as pipe:
                pipe.watch(doc_key, app_key)
                meta = lanying_redis.redis_hgetall(pipe, doc_key)
                usage = lanying_redis.redis_hgetall(pipe, app_key)
                delta = size - int(meta.get('storage_file_size', 0))
                total = int(usage.get('storage_file_size', 0)) + delta
                limit = embedding.get_app_config_int(app_id, 'lanying_connector.storage_limit')
                payg = embedding.get_app_config_int(app_id, 'lanying_connector.storage_payg') == 1
                if not clear and (limit <= 0 or (not payg and total > limit * 1024 * 1024)):
                    raise MaterialError('material_storage_limit')
                pipe.multi()
                pipe.hincrby(app_key, 'storage_file_size', delta)
                pipe.hincrby(library_key, 'storage_file_size', delta)
                pipe.hset(doc_key, 'storage_file_size', size)
                if total > int(usage.get('storage_file_size_max', 0)):
                    pipe.hset(app_key, 'storage_file_size_max', total)
                if clear:
                    for field in ['embedding_count', 'embedding_size', 'text_size', 'token_cnt', 'char_cnt']:
                        pipe.hincrby(library_key, field, -int(meta.get(field, 0)))
                        pipe.hset(doc_key, field, 0)
                    pipe.hset(doc_key, 'status', 'unindexed')
                pipe.execute()
                return
        except WatchError:
            continue
    raise MaterialError('material_usage_busy')


def _interruption_key(app_id, worker_id):
    return f'lanying_connector:materials:interrupted:{app_id}:{worker_id}'


def worker_interrupted(app_id, worker_id):
    if not worker_id:
        return False
    import lanying_redis
    return bool(lanying_redis.get_redis_connection().get(_interruption_key(app_id, worker_id)))


def mark_worker_interrupted(app_id, worker_id):
    # Called only after process_pending has unwound: this worker is no longer
    # writing vectors. Keep the recovery marker even if MySQL is unavailable.
    import lanying_redis
    lanying_redis.get_redis_connection().set(_interruption_key(app_id, worker_id), '1')


def clear_worker_interrupted(app_id, worker_id):
    if worker_id:
        import lanying_redis
        lanying_redis.get_redis_connection().delete(_interruption_key(app_id, worker_id))


def process_pending(app_id, worker_id=''):
    """A failed job remains inspectable/retryable, not silently successful."""
    reconcile_plan_and_run_references(app_id)
    reconcile_deleted_agents(app_id)
    with _engine().connect() as conn:
        ids = conn.execute(text('SELECT m.doc_id FROM seenical_material m WHERE m.app_id=:a AND ('
            "m.status IN ('pending','cleanup_pending') "
            "OR m.status IN ('indexing','cleaning') "
            "OR (m.status IN ('ready','failed') AND NOT EXISTS ("
            'SELECT 1 FROM seenical_material_reference r WHERE r.app_id=m.app_id '
            'AND r.doc_id=m.doc_id AND r.active=1)))'), {'a': app_id, 'w': worker_id}).scalars().all()
    for doc_id in ids:
        with _engine().begin() as conn:
            space = _space(conn, app_id, True)
            doc = _document(conn, app_id, doc_id)
            referenced = _has_references(conn, app_id, doc_id)
            recovering = bool(worker_id and doc.get('worker_id') == worker_id and doc['status'] in BUSY)
            interrupted = worker_interrupted(app_id, doc.get('worker_id'))
            if doc['status'] in BUSY and not recovering and not interrupted:
                continue
            if interrupted:
                action = 'cleaning'
            elif recovering:
                action = doc['status']
            elif doc['status'] == 'cleanup_pending':
                action = 'cleaning'
            elif not referenced and doc['status'] not in {'unindexed', 'cleanup_failed'}:
                action = 'cleaning'
            elif referenced and doc['status'] == 'pending':
                action = 'indexing'
            else:
                continue
            _set_state(conn, app_id, doc_id, action)
            conn.execute(text('UPDATE seenical_material SET worker_id=:w WHERE app_id=:a AND doc_id=:d'),
                         {'w': worker_id, 'a': app_id, 'd': doc_id})
        if interrupted:
            clear_worker_interrupted(app_id, doc['worker_id'])
        error = ''
        index_cleared = False
        try:
            if action == 'cleaning':
                _clear_index(app_id, space, doc)
                index_cleared = True
                # Removing a queued document must not skip original retention.
                _store_source(app_id, space, doc)
                state = 'unindexed'
            else:
                if recovering:
                    _clear_index(app_id, space, doc)
                _store_source(app_id, space, doc)
                _index_document(app_id, space, doc)
                state = 'ready'
        except Exception as exc:
            error = str(exc) if isinstance(exc, MaterialError) else 'material_processing_failed'
            if str(exc) in {'no_quota', 'deduct_failed', 'bad_authorization'}:
                error = 'material_service_unavailable'
            logging.warning('session material failed | app_id:%s doc_id:%s action:%s code:%s',
                            app_id, doc_id, action, error)
            state = 'failed' if index_cleared else 'cleanup_failed'
            if action == 'indexing':
                try:
                    _clear_index(app_id, space, doc)
                    state = 'failed'
                except Exception:
                    logging.exception('session material cleanup failed | app_id:%s doc_id:%s', app_id, doc_id)
        with _engine().begin() as conn:
            _space(conn, app_id, True)
            _set_state(conn, app_id, doc_id, state, error)
            orphan = not _has_references(conn, app_id, doc_id)
            if state == 'unindexed' and not orphan:
                _set_state(conn, app_id, doc_id, 'pending')
                state = 'pending'
        if (state == 'ready' and orphan) or state == 'pending':
            enqueue(app_id)


def _index_document(app_id, space, doc):
    import lanying_embedding as embedding
    import lanying_file_storage as files
    import lanying_config
    config = lanying_config.get_lanying_connector(app_id) or {}
    if not config.get('product_id') or lanying_config.get_lanying_connector_deduct_failed(app_id):
        raise MaterialError('material_service_unavailable')
    ext = os.path.splitext(doc['filename'])[1].lower()
    with tempfile.TemporaryDirectory(prefix='seenical-material-') as directory:
        path = os.path.join(directory, 'source' + ext)
        if files.download(doc['object_name'], path).get('result') != 'ok':
            raise MaterialError('material_download_failed')
        size = os.path.getsize(path)
        if size > MAX_FILE_BYTES:
            raise MaterialError('material_file_too_large')
        if ext not in embedding.allow_exts() and ext != '.zip':
            raise MaterialError('material_unsupported_format')
        archive = None
        if ext == '.zip':
            archive = extract_archive_documents(path, directory, embedding.allow_exts())
        else:
            validate_file(path, ext)
        initialize_index(app_id, space)
        # Retain the source even when indexing cannot obtain quota. Source-only
        # files do not count towards knowledge storage until indexing starts.
        embedding_uuid = space['embedding_uuid']
        meta = embedding.get_doc(embedding_uuid, doc['doc_id'])
        object_name = doc['object_name'] or f'seenical/materials/{app_id}/{doc["doc_id"]}{ext}'
        if not meta or not meta.get('object_name'):
            embedding.create_doc_info(app_id, embedding_uuid, doc['filename'], object_name,
                doc['doc_id'], size, ext, 'file', '', 'openai', {})
        if archive:
            documents, indexed_size, skipped = archive
            for field, value in [('archive_document_count', len(documents)),
                                 ('archive_skipped_count', skipped),
                                 ('archive_indexed_size', indexed_size)]:
                embedding.update_doc_field(embedding_uuid, doc['doc_id'], field, value)
            # Compressed size is the download size, not the knowledge allowance.
            update_usage(app_id, embedding_uuid, doc['doc_id'], indexed_size)
            # A retry rebuilds the whole archive under the same document ID.
            for field in ['progress_total', 'progress_finish']:
                embedding.update_doc_field(embedding_uuid, doc['doc_id'], field, 0)
            for name, child_path, child_ext in documents:
                embedding.process_embedding_file('', app_id, embedding_uuid, child_path,
                    name, doc['doc_id'], child_ext, source_filename=name)
        else:
            update_usage(app_id, embedding_uuid, doc['doc_id'], size)
            embedding.process_embedding_file('', app_id, embedding_uuid, path,
                                             doc['filename'], doc['doc_id'], ext)
        if embedding.get_doc(embedding_uuid, doc['doc_id']).get('status') != 'finish':
            raise MaterialError('material_processing_failed')


def material_api(app_id, actor, operation, data):
    if operation == 'usage':
        import lanying_embedding
        return lanying_embedding.get_embedding_usage(app_id)
    session_id = str(data.get('seenical_session_id', ''))
    authorized_conversation(app_id, actor, session_id)
    active_ids = reference_ids(app_id, 'session', session_id)
    historical_ids = reference_ids(app_id, 'session', session_id, False)
    if operation == 'list':
        try:
            start = max(0, int(data.get('offset', 0)))
            limit = min(100, max(1, int(data.get('limit', 50))))
        except (TypeError, ValueError):
            raise MaterialError('invalid_material_pagination')
        removed = str(data.get('removed', 'false')).lower() == 'true'
        ids = [d for d in historical_ids if (d not in active_ids) == removed]
        with _engine().connect() as conn:
            processing_rows = conn.execute(text('SELECT m.status,m.worker_id FROM seenical_material m '
                'JOIN seenical_material_reference r ON m.app_id=r.app_id AND m.doc_id=r.doc_id '
                "WHERE r.app_id=:a AND r.owner_type='session' AND r.owner_id=:o "
                "AND m.status IN ('pending','indexing','cleaning','cleanup_pending')"),
                {'a': app_id, 'o': session_id}).mappings().all()
            processing = any(row['status'] not in BUSY or not worker_interrupted(app_id, row['worker_id'])
                             for row in processing_rows)
            values = []
            for doc_id in ids[start:start + limit]:
                doc = _document(conn, app_id, doc_id)
                value = {k: doc[k] for k in ['doc_id', 'filename', 'file_size', 'status', 'error_code']}
                if doc['filename'].lower().endswith('.zip'):
                    import lanying_embedding
                    meta = lanying_embedding.get_doc(_space(conn, app_id)['embedding_uuid'], doc_id) or {}
                    if 'archive_document_count' in meta:
                        value['archive_summary'] = {
                            'document_count': int(meta['archive_document_count']),
                            'skipped_count': int(meta.get('archive_skipped_count', 0)),
                            'indexed_size': int(meta.get('archive_indexed_size', 0)),
                        }
                if doc['status'] in BUSY and worker_interrupted(app_id, doc.get('worker_id')):
                    value.update(status='interrupted', error_code='material_processing_interrupted')
                value['references'] = [dict(r) for r in conn.execute(text(
                    'SELECT owner_type,owner_id FROM seenical_material_reference '
                    'WHERE app_id=:a AND doc_id=:d AND active=1'), {'a': app_id, 'd': doc_id}).mappings()]
                values.append(value)
        import lanying_embedding
        usage = lanying_embedding.get_embedding_usage(app_id)
        return {'list': values, 'total': len(ids), 'reference_document_ids': active_ids,
                'processing': processing, 'usage': usage}
    if operation == 'inherit':
        source = str(data.get('source_session_id', ''))
        authorized_conversation(app_id, actor, source)
        # Inheritance is additive and idempotent, never erases existing target documents.
        set_references(app_id, 'session', session_id,
                       sorted(set(active_ids + reference_ids(app_id, 'session', source))))
        return {'success': True}
    doc_id = str(data.get('doc_id', ''))
    if doc_id not in historical_ids:
        raise MaterialError('material_not_found')
    if operation == 'download':
        with _engine().connect() as conn:
            doc = _document(conn, app_id, doc_id)
            space = _space(conn, app_id)
        if not doc['object_name']:
            raise MaterialError('material_file_unavailable')
        return {'embedding_name': space['embedding_name'], 'doc_id': doc_id}
    if operation == 'remove':
        remove_reference(app_id, session_id, doc_id)
    elif operation in {'restore', 'retry'}:
        with _engine().begin() as conn:
            _space(conn, app_id, True)
            doc = _document(conn, app_id, doc_id)
            if doc['status'] in BUSY and worker_interrupted(app_id, doc.get('worker_id')):
                if operation == 'restore':
                    raise MaterialError('material_cleaning')
                _set_state(conn, app_id, doc_id, 'cleanup_pending')
            elif doc['status'] in BUSY or doc['status'] == 'cleanup_pending':
                raise MaterialError('material_processing')
            elif doc['status'] == 'cleanup_failed':
                _set_state(conn, app_id, doc_id, 'cleanup_pending')
                # Retry cleanup does not reactivate a removed document.
                if operation == 'restore':
                    raise MaterialError('material_cleaning')
            elif doc['status'] in {'unindexed', 'failed'}:
                if operation == 'retry' and doc_id not in active_ids and doc['status'] != 'failed':
                    raise MaterialError('material_not_found')
                _set_state(conn, app_id, doc_id, 'pending')
            if operation == 'restore':
                conn.execute(text('UPDATE seenical_material_reference SET active=1 '
                    'WHERE app_id=:a AND owner_type=\'session\' AND owner_id=:o AND doc_id=:d'),
                    {'a': app_id, 'o': session_id, 'd': doc_id})
        enqueue(app_id)
    else:
        raise MaterialError('material_operation_invalid')
    return {'success': True}


def retrieval_scope(msg, chatbot):
    import lanying_grow_ai
    app_id = str(msg['appId'])
    run_id = message_ext(msg).get('seenical_task_run_id')
    if run_id:
        run = lanying_grow_ai.get_task_run(app_id, str(run_id))
        if (not run or run.get('status') not in {'wait', 'running', 'retry', 'continue'}
                or str(run.get('user_id')) != str(msg.get('from', {}).get('uid'))
                or str(chatbot.get('user_id')) != str(msg.get('to', {}).get('uid'))):
            raise MaterialError('material_run_invalid')
        snapshot = json.loads(run.get('material_input_snapshot', '{}'))
        if str(snapshot.get('chatbot_id')) != str(chatbot.get('chatbot_id')):
            raise MaterialError('material_run_invalid')
        ids = reference_ids(app_id, 'run', run_id)
        if ids != validate_ids(snapshot.get('reference_document_ids', [])):
            raise MaterialError('material_run_invalid')
    else:
        conversation = message_conversation(msg)
        if not conversation:
            return None
        if msg.get('type') == 'GROUPCHAT':
            import lanying_im_api
            members = lanying_im_api.filter_group_member_ids(
                app_id, conversation['conversation_id'], [str(chatbot.get('user_id', ''))])
            if str(chatbot.get('user_id', '')) not in {str(v) for v in members}:
                return None
        ids = reference_ids(app_id, 'session', conversation['seenical_session_id'])
    removed_count = 0 if run_id else len(set(reference_ids(
        app_id, 'session', conversation['seenical_session_id'], False)) - set(ids))
    if not ids:
        return {'doc_ids': [], 'removed_count': removed_count} if removed_count else None
    with _engine().connect() as conn:
        space = _space(conn, app_id)
        ready = [d for d in ids if _document(conn, app_id, d)['status'] == 'ready']
    if run_id and len(ready) != len(ids):
        raise MaterialError('material_not_ready')
    if not ready:
        return {'doc_ids': [], 'unready_count': len(ids), 'removed_count': removed_count}
    import lanying_embedding
    info = lanying_embedding.get_embedding_name_info(app_id, space['embedding_name'])
    if not info:
        raise MaterialError('material_storage_unavailable')
    info = dict(info)
    info['embedding_max_tokens'] = int(info.get('embedding_max_tokens', 2048))
    info['embedding_max_blocks'] = int(info.get('embedding_max_blocks', 5))
    info['doc_ids'] = ready
    info['_seenical_scope'] = True
    info['unready_count'] = len(ids) - len(ready)
    info['removed_count'] = removed_count
    return info


def reconcile_plan_and_run_references(app_id):
    """Release terminal runs and deleted owners after an uncertain write result.

    Owner records are persisted before new references commit. Absence therefore
    means deletion, not a resource still being created by another worker.
    """
    import lanying_grow_ai
    with _engine().connect() as conn:
        owners = conn.execute(text('SELECT DISTINCT owner_type,owner_id FROM seenical_material_reference '
            "WHERE app_id=:a AND owner_type IN ('plan','run') AND active=1"), {'a': app_id}).all()
    for owner_type, owner_id in owners:
        if owner_type == 'plan':
            owner = lanying_grow_ai.get_task(app_id, owner_id)
        else:
            owner = lanying_grow_ai.get_task_run(app_id, owner_id)
        if owner is None or (owner_type == 'run' and owner.get('status') in {'success', 'error'}):
            release_owner(app_id, owner_type, owner_id)


def release_chatbot_sessions(app_id, chatbot_id):
    """Release conversation ownership only; saved LOOP/run inputs remain valid."""
    if not storage.is_enabled():
        return
    with _engine().begin() as conn:
        if not _space(conn, app_id, True):
            return
        conn.execute(text("UPDATE seenical_material_reference SET active=0 "
            "WHERE app_id=:a AND owner_type='session' AND owner_id IN ("
            'SELECT seenical_session_id FROM seenical_conversation_binding '
            'WHERE app_id=:a AND chatbot_id=:b)'), {'a': str(app_id), 'b': str(chatbot_id)})
    enqueue(app_id)


def reconcile_deleted_agents(app_id):
    import lanying_chatbot
    with _engine().connect() as conn:
        ids = conn.execute(text('SELECT DISTINCT b.chatbot_id FROM seenical_conversation_binding b '
            'JOIN seenical_material_reference r ON r.app_id=b.app_id '
            "AND r.owner_type='session' AND r.owner_id=b.seenical_session_id "
            'WHERE b.app_id=:a AND r.active=1'), {'a': str(app_id)}).scalars().all()
    for chatbot_id in ids:
        if not lanying_chatbot.get_chatbot(app_id, chatbot_id):
            release_chatbot_sessions(app_id, chatbot_id)


def archive_member_name(entry):
    name = entry.orig_filename
    if not entry.flag_bits & 0x800:
        # Legacy ZIP writers omit the UTF-8 flag and may use GBK filenames.
        raw = name.encode('cp437')
        for encoding in ['utf-8', 'gbk']:
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                continue
    return name


def extract_archive_documents(path, directory, allowed_exts):
    """Bounded ZIP reading; never extract to a caller-controlled member path.

    All member metadata is checked before any extraction or model call. Only
    supported text documents are read; nested archives/images are not indexed.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_ARCHIVE_ENTRIES:
                raise MaterialError('material_archive_limit')
            selected = []
            total = 0
            skipped = 0
            for entry in entries:
                name = archive_member_name(entry).replace('\\', '/')
                parts = name.split('/')
                mode = stat.S_IFMT(entry.external_attr >> 16)
                if (not name or len(name) > 1024 or '\x00' in name or name.startswith('/')
                        or '..' in parts or ':' in name
                        or mode not in {0, stat.S_IFREG, stat.S_IFDIR}):
                    raise MaterialError('material_archive_unsafe')
                if entry.flag_bits & 1 or entry.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise MaterialError('material_archive_unsupported')
                if entry.is_dir():
                    continue
                if entry.file_size > MAX_FILE_BYTES:
                    raise MaterialError('material_file_too_large')
                total += entry.file_size
                if total > MAX_ARCHIVE_BYTES:
                    raise MaterialError('material_archive_limit')
                ext = os.path.splitext(name)[1].lower()
                if (not entry.file_size or '__MACOSX' in parts or parts[-1] == '.DS_Store'
                        or ext == '.zip' or ext not in allowed_exts):
                    skipped += 1
                    continue
                selected.append((entry, name, ext))
            if not selected:
                raise MaterialError('material_archive_empty')
            documents = []
            indexed_size = 0
            for index, (entry, name, ext) in enumerate(selected):
                child_path = os.path.join(directory, f'member-{index}{ext}')
                size = 0
                with archive.open(entry) as source, open(child_path, 'wb') as output:
                    while True:
                        block = source.read(65536)
                        if not block:
                            break
                        size += len(block)
                        indexed_size += len(block)
                        if size > MAX_FILE_BYTES or indexed_size > MAX_ARCHIVE_BYTES:
                            raise MaterialError('material_archive_limit')
                        output.write(block)
                if size != entry.file_size:
                    raise MaterialError('material_archive_invalid')
                validate_file(child_path, ext)
                documents.append((name, child_path, ext))
            return documents, indexed_size, skipped
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError, EOFError) as exc:
        raise MaterialError('material_archive_invalid') from exc


def validate_file(path, ext):
    with open(path, 'rb') as source:
        header = source.read(8)
    if ext == '.pdf' and not header.startswith(b'%PDF-'):
        raise MaterialError('material_unsupported_format')
    if ext in {'.doc', '.xls'} and not header.startswith(bytes.fromhex('d0cf11e0a1b11ae1')):
        raise MaterialError('material_unsupported_format')
    if ext in {'.docx', '.xlsx', '.pptx'}:
        if not zipfile.is_zipfile(path):
            raise MaterialError('material_unsupported_format')
        with zipfile.ZipFile(path) as archive:
            if sum(item.file_size for item in archive.infolist()) > 100 * 1024 * 1024:
                raise MaterialError('material_file_too_large')
    if ext in {'.md', '.txt', '.csv', '.html', '.htm'} and b'\x00' in header:
        raise MaterialError('material_unsupported_format')
