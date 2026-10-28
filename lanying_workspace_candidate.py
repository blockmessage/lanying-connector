"""Partition-scoped updates: shared originals copy on write, exclusive originals replace.

The existing material-space transaction arbitrates reference switches with run
creation and inheritance. No transaction spans indexing or object storage I/O.
"""
import io
import uuid
from sqlalchemy import text
import lanying_seenical_materials as materials
from lanying_material_replacement import runs_using
from lanying_workspace_store import params

WHERE = ' WHERE app_id=:a AND workspace_id=:w AND skill_id=:s AND path_hash=:h'


def arguments(app_id, workspace, file, **fields):
    return params(app_id, workspace, h=file['path_hash'], **fields)


def load(conn, app_id, workspace, file):
    row = conn.execute(text('SELECT * FROM seenical_workspace_candidate' + WHERE),
                       arguments(app_id, workspace, file)).mappings().first()
    return dict(row) if row else None


def state(app_id, workspace, file, status, **fields):
    if not fields.keys() <= {'new_usage', 'error_code'}:
        raise ValueError('invalid candidate fields')
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace_candidate SET status=:status' +
            ''.join(',%s=:%s' % (k, k) for k in fields) + WHERE),
            arguments(app_id, workspace, file, status=status, **fields))


def current_scope(conn, app_id, workspace, file, doc_id):
    try:
        materials.check_session_not_deleted(conn, app_id, file['source_id'])
    except materials.MaterialError:
        return False
    return conn.execute(text('SELECT 1 FROM seenical_workspace_partition p '
        'JOIN seenical_workspace_file f ON f.workspace_id=p.workspace_id AND f.skill_id=p.skill_id '
        'JOIN seenical_material_reference r ON r.app_id=p.app_id AND r.owner_id=p.seenical_session_id '
        "AND r.owner_type='session' AND r.active=1 AND r.doc_id=f.doc_id "
        'WHERE p.app_id=:a AND p.workspace_id=:w AND p.skill_id=:s AND p.active=1 AND p.paused=0 '
        'AND f.path_hash=:h AND f.removed=0 AND f.doc_id=:d'),
        arguments(app_id, workspace, file, d=doc_id)).first() is not None


def shared(conn, app_id, session, doc_id):
    # The session's bound LOOP belongs to this editing scope, other plans do not.
    plans = set(conn.execute(text('SELECT task_id FROM seenical_conversation_binding '
        "WHERE app_id=:a AND seenical_session_id=:session AND status='ACTIVE'"),
        dict(a=str(app_id), session=session)).scalars())
    refs = conn.execute(text('SELECT owner_type,owner_id FROM seenical_material_reference '
        'WHERE app_id=:a AND doc_id=:d AND active=1'), dict(a=str(app_id), d=doc_id)).all()
    return any((kind == 'session' and owner != session) or (kind == 'plan' and owner not in plans)
               for kind, owner in refs)


def start(app_id, workspace, file, content, content_sha):
    import lanying_file_storage as files
    if len(content) > materials.MAX_FILE_BYTES:
        raise materials.MaterialError('material_file_too_large')
    doc_id, session = file['doc_id'], file['source_id']
    with materials._engine().begin() as conn:
        space = materials._space(conn, app_id, True)
        doc = materials._document(conn, app_id, doc_id)
        if not doc or not current_scope(conn, app_id, workspace, file, doc_id):
            raise materials.MaterialError('material_not_found')
        existing = load(conn, app_id, workspace, file)
        if existing and existing['status'] not in {'done', 'failed'}:
            return 'processing'
        if existing and existing['content_sha'] == content_sha:
            return existing['status']
        if doc.get('replacement_id') or doc['status'] in materials.BUSY | {'pending', 'cleanup_pending', 'cleanup_failed'}:
            return 'processing'
        copy = shared(conn, app_id, session, doc_id)
        if not copy and runs_using(conn, app_id, doc_id):
            return 'waiting_runs'
        rid = uuid.uuid4().hex
        values = arguments(app_id, workspace, file, d=doc_id, session=session, rid=rid, copy=int(copy),
            physical=space['embedding_uuid'] + '-' + rid, previous=materials.index_document_id(doc),
            source_object=doc['object_name'], object_name='seenical/material-updates/' + str(app_id) + '/' + rid,
            filename=doc['filename'], size=len(content), sha=content_sha,
            old=0 if copy else materials.document_storage_usage(space['embedding_uuid'], doc_id))
        conn.execute(text('DELETE FROM seenical_workspace_candidate' + WHERE), values)
        conn.execute(text('INSERT INTO seenical_workspace_candidate '
            '(app_id,workspace_id,skill_id,path_hash,doc_id,session_id,replacement_id,copy_on_write,physical_id,'
            'previous_physical_id,source_object,object_name,filename,file_size,content_sha,old_usage) '
            'VALUES (:a,:w,:s,:h,:d,:session,:rid,:copy,:physical,:previous,:source_object,:object_name,:filename,:size,:sha,:old)'), values)
        if not copy:
            conn.execute(text('UPDATE seenical_material SET replacement_id=:rid WHERE app_id=:a AND doc_id=:d'), values)
    try:
        if files.put_object(values['object_name'], io.BytesIO(content), len(content)).get('result') != 'ok':
            raise materials.MaterialError('material_upload_failed')
    except Exception:
        state(app_id, workspace, file, 'discarding', error_code='material_update_failed')
        resume(app_id, workspace, file)
        raise
    return resume(app_id, workspace, file, recover=False)


def resume(app_id, workspace, file, recover=True):
    import lanying_file_storage as files
    with materials._engine().connect() as conn:
        row = load(conn, app_id, workspace, file)
        space = materials._space(conn, app_id)
    if not row or row['status'] in {'done', 'failed'}:
        return row['status'] if row else 'failed'
    usage_id = row['physical_id'] if row['copy_on_write'] else row['doc_id']
    if row['status'] in {'pending', 'building'}:
        if recover:
            state(app_id, workspace, file, 'discarding', error_code='material_update_interrupted')
        else:
            state(app_id, workspace, file, 'building')
            try:
                def reserve(size):
                    materials.update_usage(app_id, space['embedding_uuid'], usage_id, max(row['old_usage'], size))
                size = materials._index_document(app_id, space, {
                    'doc_id': row['physical_id'], 'filename': row['filename'],
                    'object_name': row['object_name']}, reserve_usage=reserve)
                state(app_id, workspace, file, 'ready', new_usage=size)
            except Exception as exc:
                code = str(exc) if isinstance(exc, materials.MaterialError) else 'material_update_failed'
                state(app_id, workspace, file, 'discarding', error_code=code)
        with materials._engine().connect() as conn:
            row = load(conn, app_id, workspace, file)
    if row['status'] == 'ready':
        with materials._engine().begin() as conn:
            materials._space(conn, app_id, True)
            original = materials._document(conn, app_id, row['doc_id'])
            valid = (original and current_scope(conn, app_id, workspace, file, row['doc_id'])
                     and original['object_name'] == row['source_object']
                     and materials.index_document_id(original) == row['previous_physical_id'])
            if not row['copy_on_write']:
                valid = valid and original.get('replacement_id') == row['replacement_id'] and not shared(
                    conn, app_id, row['session_id'], row['doc_id'])
            values = arguments(app_id, workspace, file, d=row['doc_id'], new=row['physical_id'],
                rid=row['replacement_id'], obj=row['object_name'], filename=row['filename'], size=row['file_size'],
                session=row['session_id'], sha=row['content_sha'])
            if not valid:
                conn.execute(text("UPDATE seenical_workspace_candidate SET status='discarding',"
                    "error_code='material_update_changed'" + WHERE), values)
                row['status'] = 'discarding'
            else:
                if not row['copy_on_write'] and runs_using(conn, app_id, row['doc_id']):
                    return 'waiting_runs'
                if row['copy_on_write']:
                    conn.execute(text('INSERT INTO seenical_material '
                        '(app_id,doc_id,message_id,filename,object_name,file_size,source_url,status) '
                        "VALUES (:a,:new,:rid,:filename,:obj,:size,'','ready')"), values)
                    conn.execute(text("UPDATE seenical_material_reference SET active=0 WHERE app_id=:a "
                        "AND owner_type='session' AND owner_id=:session AND doc_id=:d"), values)
                    conn.execute(text('INSERT INTO seenical_material_reference '
                        "(app_id,owner_type,owner_id,doc_id,active) VALUES (:a,'session',:session,:new,1)"), values)
                    materials.sync_session_plans(conn, app_id, session_id=row['session_id'])
                else:
                    conn.execute(text("UPDATE seenical_material SET index_doc_id=:new,object_name=:obj,file_size=:size,"
                        "source_url='',status='ready',error_code='' WHERE app_id=:a AND doc_id=:d AND replacement_id=:rid"), values)
                conn.execute(text("UPDATE seenical_workspace_file SET doc_id=:target,base_sha=:sha,server_sha=:sha,"
                    "source_fingerprint='',status='synced',error_code='',decision_json=NULL" + WHERE),
                    dict(values, target=row['physical_id'] if row['copy_on_write'] else row['doc_id']))
                conn.execute(text("UPDATE seenical_workspace_candidate SET status='cleanup'" + WHERE), values)
                row['status'] = 'cleanup'
    if row['status'] in {'discarding', 'cleanup'}:
        committed = row['status'] == 'cleanup'
        if not (committed and row['copy_on_write']):
            physical = row['previous_physical_id'] if committed else row['physical_id']
            cleared = materials._clear_vectors(app_id, space, physical)
            kept = materials.document_storage_usage(space['embedding_uuid'], usage_id)
            if cleared is False and kept:
                raise materials.MaterialError('material_storage_unavailable')
            materials.update_usage(app_id, space['embedding_uuid'], physical,
                                   kept if physical == usage_id else 0, clear=True)
        materials.update_usage(app_id, space['embedding_uuid'], usage_id,
                               row['new_usage'] if committed else row['old_usage'], release_only=True)
        if not committed:
            expected = 'seenical/material-updates/' + str(app_id) + '/' + row['replacement_id']
            if row['object_name'] != expected:
                raise materials.MaterialError('material_update_changed')
            files.client.remove_object(files.bucket_name, expected)
        status = 'done' if committed else 'failed'
        with materials._engine().begin() as conn:
            materials._space(conn, app_id, True)
            conn.execute(text("UPDATE seenical_material SET replacement_id='' WHERE app_id=:a AND doc_id=:d AND replacement_id=:rid"),
                dict(a=str(app_id), d=row['doc_id'], rid=row['replacement_id']))
            conn.execute(text('UPDATE seenical_workspace_candidate SET status=:status' + WHERE),
                         arguments(app_id, workspace, file, status=status))
        materials.enqueue(app_id)
        materials.materials_changed(app_id)
        return status
    return row['status']


def retry_failed(app_id, workspace, file):
    with materials._engine().begin() as conn:
        conn.execute(text("DELETE FROM seenical_workspace_candidate" + WHERE + " AND status='failed'"),
                     arguments(app_id, workspace, file))


def discard(app_id, workspace, file):
    with materials._engine().connect() as conn:
        row = load(conn, app_id, workspace, file)
    if not row or row['status'] in {'done', 'failed'}:
        return
    if row['status'] != 'cleanup':
        state(app_id, workspace, file, 'discarding', error_code='material_update_superseded')
    resume(app_id, workspace, file)
