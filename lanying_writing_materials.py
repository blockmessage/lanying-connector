"""Read-only export for local writing. Scope always comes from server relations.

This module never indexes, removes references, or returns storage URLs/credentials.
Download tickets are issued by Butler only after rechecking this scope.
"""
import hashlib
import re

import lanying_seenical_materials as materials

MAX_DOCUMENTS = 2000


def component(value):
    value = str(value)
    if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value):
        return value
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def filename(value, doc_id):
    # Keep readable original names, never interpret a name as a filesystem path.
    value = str(value or '').replace('\\', '/').rsplit('/', 1)[-1]
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', '_', value).strip(' .')
    return value[:180] or (component(doc_id) + '.bin')


def export_knowledge(embeddings, app_id, preset_name):
    """Enumerate owned bindings without retrieval's lazy model repairs."""
    import lanying_embedding as embedding
    import lanying_redis

    app_id = str(app_id)
    candidates = []
    if embeddings is None:
        redis = lanying_redis.get_redis_stack_connection()
        bindings = lanying_redis.redis_hgetall(redis, embedding.get_preset_name_key(app_id))
        for name, preset in bindings.items():
            if preset == preset_name:
                info = embedding.get_embedding_name_info(app_id, name)
                if info:
                    candidates.append((name, info['embedding_uuid'], info))
    else:
        for uuid in embeddings.get(preset_name, []):
            owner = embedding.get_app_embedding_uuid_info(app_id, uuid)
            if owner and owner.get('embedding_name'):
                name = owner['embedding_name']
                info = embedding.get_embedding_name_info(app_id, name)
                if info:
                    candidates.append((name, uuid, info))

    knowledge = []
    for name, uuid, info in candidates:
        # Retrieval permission is not export permission. Check both metadata
        # records, and reject stale name bindings that point at another UUID.
        if (str(info.get('app_id') or '') != app_id
                or str(info.get('embedding_uuid') or '') != str(uuid)):
            continue
        owner = embedding.get_app_embedding_uuid_info(app_id, uuid)
        if not owner or str(owner.get('app_id') or '') != app_id:
            continue
        if (info.get('type') in (materials.INTERNAL_TYPE, 'function')
                or owner.get('type') in (materials.INTERNAL_TYPE, 'function')
                or name == materials.INTERNAL_NAME):
            continue
        knowledge.append(dict(info, embedding_name=name, embedding_uuid=uuid))
    return knowledge


def scope(app_id, actor, data):
    import lanying_chatbot
    import lanying_config
    import lanying_grow_ai

    session_id = str(data.get('seenical_session_id', ''))
    conversation = materials.authorized_conversation(app_id, actor, session_id)
    chatbot_id = str(conversation['chatbot_id'])
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if not chatbot:
        raise materials.MaterialError('material_conversation_unavailable')
    task_id = str(data.get('task_id') or '')
    if task_id:
        task = lanying_grow_ai.get_task(app_id, task_id)
        if (not task or str(task.get('chatbot_id')) != chatbot_id
                or str(conversation.get('task_id') or '') != task_id):
            raise materials.MaterialError('material_plan_unavailable')
        ids = materials.reference_ids(app_id, 'plan', task_id)
    else:
        ids = materials.reference_ids(app_id, 'session', session_id)

    config = lanying_config.get_service_config(app_id, 'openai') or {}
    knowledge = export_knowledge(config.get('embeddings'), app_id, chatbot['name'])
    return session_id, ids, knowledge


def item(kind, owner_id, doc_id, doc, source_name):
    name = filename(doc.get('filename'), doc_id)
    return {
        'source_type': kind, 'source_id': str(owner_id), 'source_name': source_name,
        'doc_id': str(doc_id), 'filename': name,
        'path': 'materials/{}/{}/{}/{}'.format(
            'conversations' if kind == 'conversation' else 'knowledge-bases',
            component(owner_id), component(doc_id), name),
        'read_only': kind == 'knowledge_base',
        'file_size': int(doc.get('file_size') or doc.get('size') or 0),
        'status': str(doc.get('status') or 'unknown'),
        'error_code': str(doc.get('error_code') or ''),
        'available': bool(doc.get('object_name')),
    }


def manifest(app_id, actor, data):
    import lanying_embedding
    session_id, ids, knowledge = scope(app_id, actor, data)
    if len(ids) > MAX_DOCUMENTS:
        raise materials.MaterialError('material_export_too_large')
    entries = []
    with materials._engine().connect() as conn:
        for doc_id in ids:
            doc = materials._document(conn, app_id, doc_id)
            if not doc:
                raise materials.MaterialError('material_not_found')
            entries.append(item('conversation', session_id, doc_id, doc, session_id))
    for info in knowledge:
        doc_ids = lanying_embedding.get_embedding_doc_id_list(
            info['embedding_uuid'], 0, MAX_DOCUMENTS)
        if len(entries) + len(doc_ids) > MAX_DOCUMENTS:
            raise materials.MaterialError('material_export_too_large')
        for doc_id in doc_ids:
            doc = lanying_embedding.get_doc(info['embedding_uuid'], doc_id)
            if doc:
                entries.append(item('knowledge_base', info['embedding_uuid'], doc_id,
                                    doc, info['embedding_name']))
    return {
        'schema_version': 1, 'source': 'seenical', 'app_id': str(app_id),
        'seenical_session_id': session_id, 'task_id': str(data.get('task_id') or ''),
        'documents': entries,
    }


def download(app_id, actor, data):
    import lanying_embedding
    session_id, ids, knowledge = scope(app_id, actor, data)
    doc_id = str(data.get('doc_id', ''))
    kind = data.get('source_type')
    if kind == 'conversation' and str(data.get('source_id')) == session_id and doc_id in ids:
        with materials._engine().connect() as conn:
            doc = materials._document(conn, app_id, doc_id)
        if doc and doc.get('object_name'):
            return {'embedding_name': materials.INTERNAL_NAME, 'doc_id': doc_id}
    elif kind == 'knowledge_base':
        for info in knowledge:
            if str(info['embedding_uuid']) != str(data.get('source_id')):
                continue
            doc = lanying_embedding.get_doc(info['embedding_uuid'], doc_id)
            if doc and doc.get('object_name'):
                return {'embedding_name': info['embedding_name'], 'doc_id': doc_id}
    raise materials.MaterialError('material_not_found')
