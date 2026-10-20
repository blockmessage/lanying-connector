"""Stage one replacement, then atomically switch the logical document.

The workspace worker serializes callers. Space-row transactions arbitrate with
normal reference/cleanup workers. Interrupted indexing is discarded, not billed
again automatically. Old originals are retained under the existing policy.
"""
import io
import uuid

from sqlalchemy import text

import lanying_seenical_materials as materials


def load(conn, app_id, doc_id):
    row = conn.execute(text('SELECT * FROM seenical_material_replacement '
                            'WHERE app_id=:a AND doc_id=:d'),
                       {'a': str(app_id), 'd': str(doc_id)}).mappings().first()
    return dict(row) if row else None


def runs_using(conn, app_id, doc_id):
    return conn.execute(text("SELECT 1 FROM seenical_material_reference WHERE app_id=:a "
        "AND doc_id=:d AND owner_type='run' AND active=1 LIMIT 1"),
        {'a': str(app_id), 'd': str(doc_id)}).first() is not None


def state(app_id, doc_id, status, **fields):
    allowed = {'new_usage', 'error_code'}
    if not fields.keys() <= allowed:
        raise ValueError('invalid replacement fields')
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_material_replacement SET status=:s'
                          + ''.join(',%s=:%s' % (k, k) for k in fields)
                          + ' WHERE app_id=:a AND doc_id=:d'),
                     dict(fields, a=str(app_id), d=doc_id, s=status))


def start(app_id, doc_id, content, content_sha):
    import lanying_embedding as embedding
    import lanying_file_storage as files
    if len(content) > materials.MAX_FILE_BYTES:
        raise materials.MaterialError('material_file_too_large')
    with materials._engine().begin() as conn:
        space = materials._space(conn, app_id, True)
        doc = materials._document(conn, app_id, doc_id)
        if not doc or not materials._has_references(conn, app_id, doc_id):
            raise materials.MaterialError('material_not_found')
        existing = load(conn, app_id, doc_id)
        if existing and existing['status'] not in {'done', 'failed'}:
            return 'processing'
        if existing and existing['content_sha'] == content_sha:
            return existing['status']
        if doc.get('replacement_id') or doc['status'] in materials.BUSY | {'pending', 'cleanup_pending', 'cleanup_failed'}:
            return 'processing'
        if runs_using(conn, app_id, doc_id):
            return 'waiting_runs'
        old = embedding.get_doc(space['embedding_uuid'], doc_id) or {}
        rid = uuid.uuid4().hex
        row = dict(a=str(app_id), d=doc_id, r=rid, p=space['embedding_uuid'] + '-' + rid,
                   previous=materials.index_document_id(doc), filename=doc['filename'],
                   object='seenical/material-updates/' + str(app_id) + '/' + rid,
                   size=len(content), old=int(old.get('storage_file_size', 0)), sha=content_sha)
        conn.execute(text('DELETE FROM seenical_material_replacement WHERE app_id=:a AND doc_id=:d'), row)
        conn.execute(text('INSERT INTO seenical_material_replacement '
            '(app_id,doc_id,replacement_id,physical_id,previous_physical_id,object_name,'
            'filename,file_size,old_usage,content_sha) '
            'VALUES (:a,:d,:r,:p,:previous,:object,:filename,:size,:old,:sha)'), row)
        conn.execute(text('UPDATE seenical_material SET replacement_id=:r WHERE app_id=:a AND doc_id=:d'), row)
    try:
        if files.put_object(row['object'], io.BytesIO(content), len(content)).get('result') != 'ok':
            raise materials.MaterialError('material_upload_failed')
    except Exception:
        # Resume can safely clear an unfinished candidate. Never clear the old index.
        state(app_id, doc_id, 'discarding', error_code='material_update_failed')
        resume(app_id, doc_id, recover=True)
        raise
    return resume(app_id, doc_id, recover=False)


def resume(app_id, doc_id, recover=True):
    import lanying_embedding as embedding
    import lanying_file_storage as files
    with materials._engine().connect() as conn:
        row = load(conn, app_id, doc_id)
        space = materials._space(conn, app_id)
    if not row or row['status'] in {'done', 'failed'}:
        return row['status'] if row else 'failed'
    if row['status'] in {'pending', 'building'}:
        if recover:
            # Process death/unknown model result: do not replay the paid call.
            state(app_id, doc_id, 'discarding', error_code='material_update_interrupted')
        else:
            state(app_id, doc_id, 'building')
            try:
                def reserve(size):
                    materials.update_usage(app_id, space['embedding_uuid'], doc_id, max(row['old_usage'], size))
                size = materials._index_document(app_id, space, {
                    'doc_id': row['physical_id'], 'filename': row['filename'],
                    'object_name': row['object_name']}, reserve_usage=reserve)
                state(app_id, doc_id, 'ready', new_usage=size)
            except Exception as exc:
                code = str(exc) if isinstance(exc, materials.MaterialError) else 'material_update_failed'
                state(app_id, doc_id, 'discarding', error_code=code)
        with materials._engine().connect() as conn:
            row = load(conn, app_id, doc_id)
    if row['status'] == 'ready':
        with materials._engine().begin() as conn:
            materials._space(conn, app_id, True)
            if runs_using(conn, app_id, doc_id):
                return 'waiting_runs'
            # New run references take this same lock; they see only one active index.
            changed = conn.execute(text('UPDATE seenical_material SET index_doc_id=:p,object_name=:o,'
                "file_size=:z,status='ready',error_code='',source_url='' "
                'WHERE app_id=:a AND doc_id=:d AND replacement_id=:r'),
                {'p': row['physical_id'], 'o': row['object_name'], 'z': row['file_size'],
                 'a': str(app_id), 'd': doc_id, 'r': row['replacement_id']})
            if changed.rowcount != 1:
                raise materials.MaterialError('material_update_changed')
            conn.execute(text("UPDATE seenical_material_replacement SET status='cleanup' "
                'WHERE app_id=:a AND doc_id=:d'), {'a': str(app_id), 'd': doc_id})
        row['status'] = 'cleanup'
    if row['status'] in {'discarding', 'cleanup'}:
        committed = row['status'] == 'cleanup'
        physical = row['previous_physical_id'] if committed else row['physical_id']
        cleared = materials._clear_vectors(app_id, space, physical)
        # Logical document owns the single capacity reservation; physical
        # candidate counters contain only indexing metrics, never capacity.
        kept = (embedding.get_doc(space['embedding_uuid'], doc_id) or {}).get('storage_file_size', 0)
        if cleared is False and int(kept):
            raise materials.MaterialError('material_storage_unavailable')
        materials.update_usage(app_id, space['embedding_uuid'], physical,
                               int(kept) if physical == doc_id else 0, clear=True)
        materials.update_usage(app_id, space['embedding_uuid'], doc_id,
                               row['new_usage'] if committed else row['old_usage'], release_only=True)
        if not committed:
            # Only this unsuccessful candidate is disposable. Never remove an
            # original, a committed replacement, or an arbitrary storage path.
            expected = 'seenical/material-updates/' + str(app_id) + '/' + row['replacement_id']
            with materials._engine().connect() as conn:
                current = materials._document(conn, app_id, doc_id)
            if row['object_name'] != expected or (current and current['object_name'] == expected):
                raise materials.MaterialError('material_update_changed')
            files.client.remove_object(files.bucket_name, expected)
        status = 'done' if committed else 'failed'
        with materials._engine().begin() as conn:
            materials._space(conn, app_id, True)
            conn.execute(text("UPDATE seenical_material SET replacement_id='' "
                'WHERE app_id=:a AND doc_id=:d AND replacement_id=:r'),
                {'a': str(app_id), 'd': doc_id, 'r': row['replacement_id']})
            conn.execute(text('UPDATE seenical_material_replacement SET status=:s '
                'WHERE app_id=:a AND doc_id=:d'), {'a': str(app_id), 'd': doc_id, 's': status})
        materials.enqueue(app_id)
        return status
    return row['status']


def retry_failed(app_id, doc_id):
    """Explicit retry only. An interrupted cleanup must finish before this."""
    with materials._engine().begin() as conn:
        materials._space(conn, app_id, True)
        row = load(conn, app_id, doc_id)
        if row and row['status'] == 'failed':
            conn.execute(text('DELETE FROM seenical_material_replacement WHERE app_id=:a AND doc_id=:d'),
                         {'a': str(app_id), 'd': doc_id})


def discard(app_id, doc_id):
    with materials._engine().connect() as conn:
        row = load(conn, app_id, doc_id)
    if not row or row['status'] in {'done', 'failed'}:
        return
    # A committed switch can only finish cleanup; it must never be rolled back
    # by treating the now-active physical index as a failed candidate.
    if row['status'] != 'cleanup':
        state(app_id, doc_id, 'discarding', error_code='material_update_superseded')
    resume(app_id, doc_id)
