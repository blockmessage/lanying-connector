"""Private workspaces with independently authorized Skill partitions.

Butler authorizes each scheduled cycle. A DB advisory lock serializes workspace
administration and workers across nodes, without an expiring lease or Redis Lua.
Individual file decisions remain inspectable; no remote deletion is automatic.
"""
import base64
import contextlib
import hashlib
import io
import json
import logging
import os
import time
import uuid

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import lanying_seenical_materials as materials
import lanying_writing_materials as exports
from lanying_workspace_github import Repository, SyncError, blob_sha, decision, relative_path

from lanying_workspace_store import (load, update, public_view, params, partitions, partition_view,
                                     file_rows, connections)

MAX_SCOPES = 20
MAX_CYCLE_FILES = 20
MAX_CYCLE_BYTES = 32 * 1024 * 1024


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _cipher():
    secret = os.getenv('SEENICAL_WORKSPACE_SECRET_KEY') or os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if not secret:
        raise SyncError('workspace_secret_unavailable')
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(
        ('seenical-workspace-v1:' + secret).encode()).digest()))


@contextlib.contextmanager
def locked(app_id):
    # Only one dedicated connection is held, never an open SQL transaction.
    key = 'seenical_workspace:' + hashlib.sha256(str(app_id).encode()).hexdigest()[:40]
    with materials._engine().connect() as conn:
        if conn.execute(text('SELECT GET_LOCK(:k,0)'), {'k': key}).scalar() != 1:
            raise SyncError('workspace_busy')
        try:
            yield
        finally:
            conn.execute(text('SELECT RELEASE_LOCK(:k)'), {'k': key})


def checked_scopes(app_id, actor, scopes):
    if not isinstance(scopes, list) or not 1 <= len(scopes) <= MAX_SCOPES:
        raise SyncError('invalid_workspace_scope')
    checked, seen = [], set()
    for item in scopes:
        if not isinstance(item, dict) or not item.keys() <= {'seenical_session_id', 'task_id'}:
            raise SyncError('invalid_workspace_scope')
        if item.get('task_id'):
            # A former development-only LOOP grant must not silently become
            # permission to export future conversation uploads.
            raise SyncError('workspace_scope_confirmation_required')
        session, _, knowledge = exports.scope(app_id, actor, item)
        if session in seen:
            continue
        seen.add(session)
        checked.append({'seenical_session_id': session, 'task_id': '',
                        'knowledge_base_ids': sorted({str(k['embedding_uuid']) for k in knowledge})})
    return checked


def configure(app_id, actor, data):
    if data.get('confirmed') is not True:
        raise SyncError('workspace_confirmation_required')
    workspace_id = data.get('workspace_id')
    # Administration only; workers use the workspace lock. No model/network work
    # occurs inside the material-space transaction used for binding uniqueness.
    with locked('configure:' + str(app_id)):
        old = load(app_id, workspace_id) if workspace_id else None
        if old and old['layout_version'] != 2:
            raise SyncError('workspace_layout_upgrade_required')
        repository_type = data.get('repository_type') or (old or {}).get('repository_type', 'own')
        if repository_type not in {'own', 'managed'}:
            raise SyncError('invalid_workspace_hosting_fields')
        if old and old.get('repository_type', 'own') != repository_type:
            raise SyncError('workspace_hosting_type_fixed')
        if repository_type == 'managed':
            from lanying_workspace_hosting import configure as configure_managed
            return configure_managed(app_id, actor, data, old)
        if data.get('creation_key'):
            raise SyncError('invalid_workspace_hosting_fields')
        repo_name = str(data.get('repository') or (old or {}).get('repository') or '').removeprefix('https://github.com/').removesuffix('.git')
        token = data.get('github_token')
        if not token and old:
            try:
                token = _cipher().decrypt(old['github_token'].encode()).decode()
            except InvalidToken:
                raise SyncError('github_reauthorize') from None
        repo = Repository(repo_name, data.get('branch') or (old or {}).get('branch') or 'main', token)
        repository_id = repo.verify_private()
        if not data.get('branch') and not old:
            repo.branch = repo.default_branch
        directory = relative_path(data.get('directory', (old or {}).get('directory', '')), allow_empty=True)
        if old and (repository_id, repo.branch, directory) != (old['repository_id'], old['branch'], old['directory']):
            raise SyncError('workspace_location_fixed')
        _, tree = repo.snapshot()
        root = directory + '/' if directory else ''
        if not old and any(p == root + 'seenical-workspace.json' or p == root + 'materials'
                           or p.startswith(root + 'materials/') for p in tree):
            raise SyncError('workspace_directory_not_empty')
        values = dict(a=str(app_id), w=workspace_id or uuid.uuid4().hex, repository=repo_name,
            rid=repository_id, branch=repo.branch, directory=directory,
            location=hashlib.sha256(_json([repository_id, repo.branch, directory]).encode()).hexdigest(),
            token=_cipher().encrypt(token.encode()).decode(), actor=_json(actor))
        try:
            with locked('repository:' + repository_id + ':' + repo.branch), materials._engine().begin() as conn:
                if not old:
                    locations = conn.execute(text('SELECT directory FROM seenical_workspace '
                        'WHERE repository_id=:rid AND branch=:branch'), values).scalars()
                    if any(not directory or not other or directory == other
                           or directory.startswith(other + '/') or other.startswith(directory + '/')
                           for other in locations):
                        raise SyncError('workspace_location_in_use')
                if old:
                    conn.execute(text('UPDATE seenical_workspace SET github_token=:token,actor_json=:actor '
                        'WHERE app_id=:a AND workspace_id=:w'), values)
                else:
                    conn.execute(text('INSERT INTO seenical_workspace '
                        '(app_id,workspace_id,repository,repository_id,branch,directory,location_hash,github_token,actor_json,scopes_json)'
                        " VALUES (:a,:w,:repository,:rid,:branch,:directory,:location,:token,:actor,'[]')"), values)
        except IntegrityError:
            raise SyncError('workspace_location_in_use') from None
    return public_view(app_id, values['w'])


def bind(app_id, actor, data):
    if data.get('confirmed') is not True:
        raise SyncError('workspace_confirmation_required')
    workspace = load(app_id, data.get('workspace_id'))
    if not workspace['repository_id']:
        raise SyncError('workspace_hosting_incomplete')
    if workspace['layout_version'] != 2:
        raise SyncError('workspace_layout_upgrade_required')
    identity = exports.identity(app_id, actor, {'seenical_session_id': data.get('seenical_session_id'),
                                              'skill_id': data.get('skill_id')})
    scope = checked_scopes(app_id, actor, [{'seenical_session_id': identity['seenical_session_id']}])[0]
    with locked(workspace['workspace_id']):
        repo = repository(workspace)  # Verify privacy/access before granting export.
        if not any(p['skill_id'] == identity['skill_id'] for p in partitions(app_id, workspace)):
            _, tree = repo.snapshot()
            path = prefix(workspace) + 'materials/' + identity['skill_id']
            if any(p == path or p.startswith(path + '/') for p in tree):
                raise SyncError('workspace_directory_not_empty')
        with materials._engine().begin() as conn:
            # Skill identity exists even before the first material is uploaded.
            conn.execute(text('SELECT id FROM seenical_writing_skill WHERE app_id=:a '
                'AND seenical_session_id=:session FOR UPDATE'),
                dict(a=str(app_id), session=identity['seenical_session_id'])).first()
            existing = conn.execute(text('SELECT workspace_id FROM seenical_workspace_partition '
                'WHERE app_id=:a AND skill_id=:s AND active=1 FOR UPDATE'),
                dict(a=str(app_id), s=identity['skill_id'])).scalars().all()
            if any(w != workspace['workspace_id'] for w in existing):
                raise SyncError('workspace_skill_already_bound')
            conn.execute(text('INSERT INTO seenical_workspace_partition '
                '(app_id,workspace_id,skill_id,seenical_session_id,knowledge_base_ids) VALUES (:a,:w,:s,:session,:knowledge) '
                "ON DUPLICATE KEY UPDATE active=1,paused=0,knowledge_base_ids=:knowledge,status='pending',error_code=''"),
                params(app_id, workspace, s=identity['skill_id'], session=identity['seenical_session_id'],
                       knowledge=_json(scope['knowledge_base_ids'])))
    enqueue(app_id, actor, workspace['workspace_id'])
    return public_view(app_id, workspace['workspace_id'], identity['skill_id'])


def enqueue(app_id, actor, workspace_id, changed=True):
    if changed:
        request_check(app_id, workspace_id)
    from lanying_tasks import seenical_workspace_sync_task
    seenical_workspace_sync_task.apply_async(args=[str(app_id), actor, int(time.time()), workspace_id])


def request_check(app_id, workspace_id=None):
    """Persist a change signal, never an authorization or a file operation.

    Two counters prevent a running sync from acknowledging a later event.
    Paused workspaces retain changes until the user resumes them.
    """
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace SET requested_generation=requested_generation+1,'
            "status=CASE WHEN status='synced' THEN 'pending' ELSE status END "
            'WHERE app_id=:a AND layout_version=2' + (' AND workspace_id=:w' if workspace_id else '')),
            {'a': str(app_id), 'w': workspace_id})


def notify(workspace_id):
    # The public caller supplies only an opaque locator, not an App, repository,
    # branch, actor, URL or contents. Butler reauthorizes the stored grant later.
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace SET requested_generation=requested_generation+1,'
            "status=CASE WHEN status='synced' THEN 'pending' ELSE status END "
            'WHERE workspace_id=:w'), {'w': str(workspace_id)})
    return {'accepted': True}  # Do not disclose whether a workspace exists.


def scheduled_candidates(after=''):
    if not materials.storage.is_enabled():
        return {'list': []}
    with materials._engine().begin() as conn:
        rows = conn.execute(text('SELECT app_id,workspace_id,actor_json FROM seenical_workspace '
            "WHERE layout_version=2 AND repository_id<>'' AND paused=0 AND requested_generation>completed_generation "
            'AND next_check_at<=CURRENT_TIMESTAMP AND workspace_id>:after ORDER BY workspace_id LIMIT 100'),
            {'after': str(after)}).mappings().all()
        claimed = []
        for row in rows:
            changed = conn.execute(text('UPDATE seenical_workspace '
                'SET next_check_at=DATE_ADD(CURRENT_TIMESTAMP,INTERVAL 300 SECOND) '
                'WHERE workspace_id=:w AND paused=0 AND requested_generation>completed_generation '
                'AND next_check_at<=CURRENT_TIMESTAMP'), {'w': row['workspace_id']})
            if changed.rowcount == 1:
                claimed.append({'app_id': row['app_id'], 'workspace_id': row['workspace_id'], **json.loads(row['actor_json'])})
        return {'list': claimed}


def resolve(app_id, actor, data, workspace):
    """Re-admit a current reference, or confirm content against both current hashes."""
    action = data.get('choice')
    if action not in {'server', 'github', 'delete', 'retry', 'restore', 'relink'}:
        raise SyncError('invalid_workspace_choice')
    with locked(workspace['workspace_id']):
        entries = collect(app_id, actor, workspace)
        with materials._engine().connect() as conn:
            row = conn.execute(text('SELECT * FROM seenical_workspace_file WHERE app_id=:a AND workspace_id=:w AND skill_id=:s AND path_hash=:h'),
                               params(app_id, workspace, h=data.get('path_hash'))).mappings().first()
        if not row:
            raise SyncError('workspace_file_unavailable')
        current = entries.get((row['source_type'], row['doc_id']))
        if action in {'restore', 'relink'}:
            # Explicitly re-admit an authorized, current reference, not a retained
            # original. This chooses no content and performs no remote write.
            if not current:
                raise SyncError('workspace_scope_unavailable')
            if (action == 'restore' and not row['removed']) or (
                    action == 'relink' and (row['removed']
                    or row['error_code'] != 'workspace_scope_changed')):
                raise SyncError('workspace_preview_changed')
            file_state(app_id, row, 'pending', source_id=current['source_id'],
                       removed=0, decision_json=None,
                       base_sha='' if action == 'restore' else row['base_sha'])
        else:
            if action == 'github' and row['source_type'] != 'conversation':
                raise SyncError('workspace_readonly')
            repo = repository(workspace)
            head, tree = repo.snapshot()
            remote = tree.get(prefix(workspace) + row['path'])
            server_sha = blob_sha(read_source(source(current))) if current else None
            consent = {'choice': action, 'server': server_sha,
                       'remote': remote['sha'] if remote else None}
            if (consent['server'] or '', consent['remote'] or '') != (row['server_sha'], row['remote_sha']):
                raise SyncError('workspace_preview_changed')
            with materials._engine().begin() as conn:
                conn.execute(text("UPDATE seenical_workspace_file SET decision_json=:j,status='pending',error_code='' "
                    'WHERE app_id=:a AND workspace_id=:w AND skill_id=:s AND path_hash=:h'),
                    params(app_id, workspace, j=_json(consent), h=row['path_hash']))
            if action == 'retry' and row['source_type'] == 'conversation':
                from lanying_workspace_candidate import retry_failed
                retry_failed(app_id, workspace, row)
                if current and current['status'] in {'failed', 'cleanup_failed'}:
                    materials.material_api(app_id, actor, 'retry',
                        {'seenical_session_id': row['source_id'], 'doc_id': row['doc_id']})
    enqueue(app_id, actor, workspace['workspace_id'])
    return public_view(app_id, workspace['workspace_id'], workspace.get('skill_id'))


def collect(app_id, actor, workspace):
    if not workspace:
        raise SyncError('workspace_not_configured')
    result = {}
    for scope in json.loads(workspace['scopes_json']):
        if scope.get('task_id'):
            raise SyncError('workspace_scope_confirmation_required')
        current = exports.manifest(app_id, actor, scope)
        for item in current['documents']:
            if item['source_type'] == 'knowledge_base' and item['source_id'] not in scope['knowledge_base_ids']:
                continue  # New Agent bindings do not extend previous GitHub consent.
            item['app_id'] = str(app_id)
            value = result.setdefault((item['source_type'], item['doc_id']), item)
            value.setdefault('scope_keys', []).append(scope['seenical_session_id'] + '\n' + scope.get('task_id', ''))
    if len(result) > exports.MAX_DOCUMENTS:
        raise SyncError('material_export_too_large')
    return result


def source(item):
    """Stat originals before relying on a cached SHA; not a model/index request."""
    import lanying_embedding as embedding
    import lanying_file_storage as files
    if item['source_type'] == 'conversation':
        with materials._engine().connect() as conn:
            doc = materials._document(conn, item['app_id'], item['doc_id'])
    else:
        doc = embedding.get_doc(item['source_id'], item['doc_id'])
    if not doc or not doc.get('object_name'):
        raise SyncError('material_file_unavailable')
    stat = files.client.stat_object(files.bucket_name, doc['object_name'])
    if stat.size > materials.MAX_FILE_BYTES:
        raise SyncError('material_file_too_large')
    return {'object': doc['object_name'], 'size': stat.size,
            'fingerprint': hashlib.sha256((doc['object_name'] + ':' + stat.etag).encode()).hexdigest()}


def read_source(info):
    import lanying_file_storage as files
    stream = files.open_object(info['object'])
    if stream.get('result') != 'ok':
        raise SyncError('material_file_unavailable')
    try:
        content = stream['stream'].read(materials.MAX_FILE_BYTES + 1)
    finally:
        stream['stream'].close()
    if len(content) != info['size'] or len(content) > materials.MAX_FILE_BYTES:
        raise SyncError('material_download_failed')
    return content


def repository(workspace):
    if workspace.get('repository_type') == 'managed':
        from lanying_workspace_hosting import managed_repository
        return managed_repository(workspace)
    try:
        token = _cipher().decrypt(workspace['github_token'].encode()).decode()
    except InvalidToken:
        raise SyncError('github_reauthorize') from None
    repo = Repository(workspace['repository'], workspace['branch'], token)
    repo.verify_private(workspace['repository_id'])
    return repo


def prefix(workspace):
    return workspace['directory'] + '/' if workspace['directory'] else ''


def file_state(app_id, row, status, error='', **fields):
    allowed = {'base_sha', 'source_fingerprint', 'server_sha', 'remote_sha',
               'decision_json', 'removed', 'source_id'}
    if not fields.keys() <= allowed:
        raise ValueError('invalid file state')
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace_file SET status=:status,error_code=:e'
            + ''.join(',%s=:%s' % (k, k) for k in fields)
            + ' WHERE app_id=:a AND workspace_id=:w AND skill_id=:s AND path_hash=:h'),
            params(app_id, row, **fields, h=row['path_hash'], status=status, e=error))


def mapped_files(app_id, entries, workspace):
    rows = file_rows(app_id, workspace)
    existing = {(r['source_type'], r['doc_id']) for r in rows}
    mapped_paths = {r['path'] for r in rows}
    reserved_paths = mapped_paths | {item['path'] for item in entries.values()}
    with materials._engine().begin() as conn:
        for key, item in entries.items():
            if key in existing:
                continue
            path = item['path']
            if path in mapped_paths:
                # A copy-on-write document keeps its original GitHub path. If
                # the original is restored, allocate another path once, without
                # moving the copy or reusing retained/removed file mappings.
                directory, filename = path.rsplit('/', 1)
                suffix = 2
                path = directory + '-' + str(suffix) + '/' + filename
                while path in reserved_paths:
                    suffix += 1
                    path = directory + '-' + str(suffix) + '/' + filename
            relative_path(path)
            row = dict(app_id=str(app_id), workspace_id=workspace['workspace_id'], skill_id=workspace['skill_id'],
                       path=path, path_hash=hashlib.sha256(path.encode()).hexdigest(),
                       source_type=item['source_type'], source_id=item['source_id'], doc_id=item['doc_id'],
                       base_sha='', source_fingerprint='', server_sha='', remote_sha='', error_code='',
                       removed=0, decision_json=None, status='pending')
            conn.execute(text('INSERT INTO seenical_workspace_file '
                '(app_id,workspace_id,skill_id,path_hash,path,source_type,source_id,doc_id) '
                'VALUES (:app_id,:workspace_id,:skill_id,:path_hash,:path,:source_type,:source_id,:doc_id)'), row)
            rows.append(row)
            mapped_paths.add(path)
            reserved_paths.add(path)
    return rows


def import_new(app_id, workspace, actor, session, path, content):
    """Stable message identity makes retries reuse the same document and charge."""
    import lanying_file_storage as files
    materials.authorized_conversation(app_id, actor, session)
    space = materials.ensure_space(app_id)
    identity = 'github:' + workspace['workspace_id'] + ':' + workspace['skill_id'] + ':' + hashlib.sha256(path.encode()).hexdigest()
    doc_id = space['embedding_uuid'] + '-' + hashlib.sha256(identity.encode()).hexdigest()[:40]
    object_name = 'seenical/github/' + str(app_id) + '/' + doc_id
    with materials._engine().connect() as conn:
        doc = materials._document(conn, app_id, doc_id)
    if not doc:
        if files.put_object(object_name, io.BytesIO(content), len(content)).get('result') != 'ok':
            raise SyncError('material_upload_failed')
        with materials._engine().begin() as conn:
            materials._space(conn, app_id, True)
            conn.execute(text('INSERT INTO seenical_material '
                '(app_id,doc_id,message_id,filename,source_url,object_name,file_size) '
                "VALUES (:a,:d,:m,:f,'',:o,:z)"), {'a': str(app_id), 'd': doc_id, 'm': identity,
                'f': exports.filename(path, doc_id), 'o': object_name, 'z': len(content)})
            conn.execute(text('INSERT INTO seenical_material_reference '
                "(app_id,owner_type,owner_id,doc_id,active) VALUES (:a,'session',:s,:d,1)"),
                {'a': str(app_id), 's': session, 'd': doc_id})
            materials.sync_session_plans(conn, app_id, session_id=session)
    materials.enqueue(app_id)
    return doc_id


def run(app_id, actor, authorized_at, workspace_id):
    # A backed-up queue must obtain fresh Butler authorization, not use an old
    # administrator snapshot. The scheduler will enqueue a fresh cycle.
    if not 0 <= time.time() - authorized_at <= 300:
        return
    workspace = None
    try:
        with locked(workspace_id):
            workspace = load(app_id, workspace_id)
            if workspace['paused'] or workspace['layout_version'] != 2:
                return
            update(app_id, workspace, status='syncing', error_code='')
            pending = run_workspace(app_id, actor, workspace)
            with materials._engine().begin() as conn:
                conn.execute(text('UPDATE seenical_workspace SET completed_generation=:g,'
                    'next_check_at=DATE_ADD(CURRENT_TIMESTAMP,INTERVAL 60 SECOND) WHERE app_id=:a AND workspace_id=:w'),
                    params(app_id, workspace, g=workspace['completed_generation'] if pending else workspace['requested_generation']))
    except (SyncError, materials.MaterialError) as exc:
        if workspace and str(exc) != 'workspace_busy':
            update(app_id, workspace, status='failed', error_code=str(exc))
            logging.warning('workspace sync failed | app_id:%s code:%s', app_id, exc)
    except Exception:
        if workspace:
            update(app_id, workspace, status='failed', error_code='workspace_failed')
        # No exception text: upstream clients may put credentials/body in it.
        logging.warning('workspace sync failed | app_id:%s code:workspace_failed', app_id)


def _run(app_id, actor, workspace):
    from lanying_workspace_candidate import load as replacement_load, resume, start, discard
    repo = repository(workspace)
    entries = collect(app_id, actor, workspace)
    head, tree = repo.snapshot()
    root = prefix(workspace)
    if tree.get(root + 'manifest.json', {}).get('sha', '') != workspace['manifest_sha']:
        raise SyncError('workspace_metadata_changed')
    materials.reconcile_plan_and_run_references(app_id)
    rows = mapped_files(app_id, entries, workspace)
    # Finish one partition candidate before accepting another remote version.
    # A workspace lock already serializes this path across workers and UI writes.
    for row in rows:
        if row['source_type'] != 'conversation':
            continue
        with materials._engine().connect() as conn:
            candidate = replacement_load(conn, app_id, workspace, row)
        if not candidate or candidate['status'] in {'done', 'failed'}:
            continue
        remote = tree.get(root + row['path'])
        try:
            if (row['removed'] or ('conversation', row['doc_id']) not in entries or not remote
                    or candidate['content_sha'] != remote['sha']):
                discard(app_id, workspace, row)
                file_state(app_id, row, 'pending')
            else:
                result = resume(app_id, workspace, row)
                with materials._engine().connect() as conn:
                    detail = replacement_load(conn, app_id, workspace, row) or {}
                file_state(app_id, row, 'pending' if result == 'done' else result,
                           detail.get('error_code') or 'material_update_failed' if result == 'failed' else '')
            update(app_id, workspace, status='pending')
        except Exception:
            file_state(app_id, row, 'failed', 'workspace_file_failed')
            update(app_id, workspace, status='needs_attention')
        return True  # Reload committed references and mapping on the next cycle.
    changes, successes, processed, hashed, transferred = {}, [], 0, 0, 0
    unfinished = False
    for row in rows:
        item = entries.get((row['source_type'], row['doc_id']))
        remote = tree.get(root + row['path'])
        try:
            if remote and (remote['type'], remote['mode']) != ('blob', '100644'):
                raise SyncError('github_regular_file_required')
            if row['removed']:
                continue  # Confirmed deletion is not resurrected from retained originals.
            if item and row['source_type'] == 'conversation' and not any(
                    k.split('\n')[0] == row['source_id'] for k in item.get('scope_keys', [])):
                raise SyncError('workspace_scope_changed')
            if (item and item['source_type'] == 'conversation'
                    and item.get('available') is False
                    and item['status'] in {'pending', 'indexing'}):
                # Upload retention happens asynchronously. Waiting for the source
                # is not a failed paid indexing attempt and needs no user retry.
                file_state(app_id, row, 'processing')
                continue
            info, content, server_sha = None, None, None
            if item:
                info = source(item)
                if info['fingerprint'] == row['source_fingerprint'] and row['server_sha']:
                    server_sha = row['server_sha']
                else:
                    if hashed >= MAX_CYCLE_FILES or transferred + info['size'] > MAX_CYCLE_BYTES:
                        file_state(app_id, row, 'pending')
                        continue
                    hashed += 1
                    content = read_source(info)
                    transferred += len(content)
                    server_sha = blob_sha(content)
            remote_sha = remote['sha'] if remote else None
            file_state(app_id, row, row['status'], row.get('error_code', ''),
                       server_sha=server_sha or '',
                       remote_sha=remote_sha or '',
                       source_fingerprint=info['fingerprint'] if info else '')
            choice = json.loads(row['decision_json']) if row['decision_json'] else {}
            if choice and (choice.get('server'), choice.get('remote')) != (server_sha, remote_sha):
                file_state(app_id, row, 'conflict', 'workspace_preview_changed', decision_json=None)
                continue
            if row['status'] == 'failed' and not choice:
                continue  # Paid processing is only retried explicitly.
            action = decision(row['base_sha'] or None, server_sha, remote_sha,
                              row['source_type'] == 'knowledge_base')
            if choice.get('choice') == 'server' and server_sha:
                action = 'upload'
            elif choice.get('choice') == 'github' and item and row['source_type'] == 'conversation' and remote:
                action = 'download'
            elif choice.get('choice') == 'delete' and action in {'confirm_delete_remote', 'confirm_delete_server'}:
                action = 'delete_remote' if server_sha is None else 'delete_server'
            if action == 'synced':
                status = 'synced'
                error = ''
                if item and item['source_type'] == 'conversation' and item['status'] != 'ready':
                    status = 'failed' if item['status'] in {'failed', 'cleanup_failed'} else 'processing'
                    error = item.get('error_code', '') if status == 'failed' else ''
                file_state(app_id, row, status, error, base_sha=server_sha or '', decision_json=None)
                continue
            if action not in {'upload', 'download', 'delete_remote', 'delete_server'}:
                file_state(app_id, row, action)
                continue
            if processed >= MAX_CYCLE_FILES:
                file_state(app_id, row, 'pending')
                continue
            processed += 1
            if action == 'upload':
                if content is None and transferred + info['size'] > MAX_CYCLE_BYTES:
                    file_state(app_id, row, 'pending')
                    continue
                outgoing = content if content is not None else read_source(info)
                if content is None:
                    transferred += len(outgoing)
                if blob_sha(outgoing) != server_sha:
                    raise SyncError('workspace_preview_changed')
                changes[root + row['path']] = outgoing
                successes.append((row, server_sha, False))
            elif action == 'delete_remote':
                changes[root + row['path']] = None
                successes.append((row, '', True))
            elif action == 'delete_server':
                # Reuse session removal; other sessions/LOOPs/runs keep their references.
                materials.authorized_conversation(app_id, actor, row['source_id'])
                materials.remove_reference(app_id, row['source_id'], row['doc_id'])
                file_state(app_id, row, 'removed', removed=1, base_sha='', decision_json=None)
            else:
                unfinished = True  # Cleanup may remain pending even if indexing raises.
                if transferred + remote.get('size', materials.MAX_FILE_BYTES) > MAX_CYCLE_BYTES:
                    file_state(app_id, row, 'pending')
                    continue
                incoming = repo.read(remote)
                transferred += len(incoming)
                result = start(app_id, workspace, row, incoming, remote_sha)
                if result == 'done':
                    update(app_id, workspace, status='pending')
                    return True  # Mapping/reference switch is committed; reload before exporting metadata.
                else:
                    with materials._engine().connect() as conn:
                        detail = replacement_load(conn, app_id, workspace, row) or {}
                    file_state(app_id, row, 'failed' if result == 'failed' else result,
                               detail.get('error_code') or 'material_update_failed' if result == 'failed' else '')
                    update(app_id, workspace, status='pending' if result != 'failed' else 'needs_attention')
                    return True
        except (SyncError, materials.MaterialError) as exc:
            file_state(app_id, row, 'failed', str(exc))
            if unfinished:
                update(app_id, workspace, status='needs_attention')
                return True
        except Exception:
            file_state(app_id, row, 'failed', 'workspace_file_failed')
            if unfinished:
                update(app_id, workspace, status='needs_attention')
                return True

    # Only explicitly authorized conversations admit new files.
    writable = {'conversation': s['seenical_session_id']
                for s in json.loads(workspace['scopes_json']) if not s.get('task_id')}
    known_paths = {r['path'] for r in rows}
    warnings = []
    for path, entry in tree.items():
        if not path.startswith(root) or entry['type'] == 'tree':
            continue
        local = path[len(root):]
        if local in known_paths or local in {'manifest.json', 'README.md'}:
            continue
        parts = local.split('/')
        session = writable.get(parts[0]) if len(parts) >= 2 else None
        if not session:
            if len(warnings) < 20:
                warnings.append({'path': local, 'reason': 'workspace_unmapped_file'})
            continue
        if len(entries) >= exports.MAX_DOCUMENTS:
            if len(warnings) < 20:
                warnings.append({'path': local, 'reason': 'material_export_too_large'})
            continue
        if (processed >= MAX_CYCLE_FILES
                or transferred + entry.get('size', materials.MAX_FILE_BYTES) > MAX_CYCLE_BYTES):
            if len(warnings) < 20:
                warnings.append({'path': local, 'reason': 'workspace_pending_file'})
            continue
        processed += 1
        try:
            relative_path(local)
            content = repo.read(entry)
            transferred += len(content)
            doc_id = import_new(app_id, workspace, actor, session, local, content)
        except (SyncError, materials.MaterialError) as exc:
            if len(warnings) < 20:
                warnings.append({'path': local, 'reason': str(exc)})
            continue
        except Exception:
            if len(warnings) < 20:
                warnings.append({'path': local, 'reason': 'workspace_file_failed'})
            continue
        item = dict(path=local, source_type='conversation', source_id=session, doc_id=doc_id,
                    filename=exports.filename(local, doc_id), read_only=False, available=True,
                    file_size=len(content), status='pending', source_name=session, app_id=str(app_id),
                    scope_keys=[session + '\n'])
        # Keep the user's path; moving it automatically would be surprising.
        created = mapped_files(app_id, {(item['source_type'], doc_id): item}, workspace)
        new_row = next(r for r in created if r['doc_id'] == doc_id and r['source_type'] == 'conversation')
        file_state(app_id, new_row, 'processing', base_sha=entry['sha'], server_sha=entry['sha'])
        entries[('conversation', doc_id)] = item

    # Regenerate only server-owned metadata. No prompts, API tokens or Skill code.
    latest_rows = mapped_files(app_id, entries, workspace)
    included = []
    for row in latest_rows:
        item = entries.get((row['source_type'], row['doc_id']))
        if not item or row['removed']:
            continue
        path = root + row['path']
        present = ((path in changes and changes[path] is not None)
                   or (path in tree and path not in changes))
        # Export only file metadata; App identity and scope bookkeeping stay server-side.
        exported = {key: item[key] for key in (
            'source_type', 'source_id', 'source_name', 'doc_id', 'filename', 'read_only',
            'file_size', 'status', 'error_code', 'available') if key in item}
        included.append(dict(exported, path=row['path'], local_status='ready' if present else 'unavailable'))
    manifest = {'schema_version': 2, 'source': 'github',
                'workspace_id': workspace['workspace_id'], 'skill_id': workspace['skill_id'],
                'seenical_session_id': workspace['seenical_session_id'], 'documents': included}
    payloads = {'manifest.json': _json(manifest).encode()}
    if tree.get(root + 'manifest.json', {}).get('sha') != blob_sha(payloads['manifest.json']):
        changes[root + 'manifest.json'] = payloads['manifest.json']
    if root + 'README.md' not in tree:
        changes[root + 'README.md'] = (
            '# Reference materials\n\nConversation edits update only this conversation; shared documents are copied '
            'on write and may require additional knowledge storage. Knowledge-base copies are read-only. '
            'Removal and conflicts require Console confirmation. Only manifest entries are writing references.\n'
            '会话文件修改仅用于当前会话；共享资料修改时独立复制并检查容量。关联企业知识库只读。\n').encode()
    if changes:
        repo.verify_private(workspace['repository_id'])
        repo.commit(head, changes)
    # Commit could have succeeded on GitHub even if the response was lost. In
    # that case we stop and expose metadata repair, not replay an overwrite.
    update(app_id, workspace, manifest_sha=blob_sha(payloads['manifest.json']))
    for row, sha, removed in successes:
        status, error = 'synced', ''
        item = entries.get((row['source_type'], row['doc_id']))
        if item and item['source_type'] == 'conversation' and item['status'] != 'ready':
            status = 'failed' if item['status'] in {'failed', 'cleanup_failed'} else 'processing'
            error = item.get('error_code', '') if status == 'failed' else ''
        file_state(app_id, row, 'removed' if removed else status, error, base_sha=sha, removed=int(removed),
                   decision_json=None)
    states = [r['status'] for r in file_rows(app_id, workspace) if not r['removed']]
    status = 'synced' if not warnings and all(s == 'synced' for s in states) else 'needs_attention'
    update(app_id, workspace, status=status, error_code=warnings[0]['reason'] if warnings else '', warnings_json=_json(warnings))
    return unfinished or any(s in {'pending', 'processing', 'waiting_runs'} for s in states) or any(
        warning['reason'] == 'workspace_pending_file' for warning in warnings)


def workspace_config(app_id, workspace, repo, tree, repair=False):
    """Preserve unrelated modules but never accept edited material authority."""
    path = prefix(workspace) + 'seenical-workspace.json'
    entry = tree.get(path)
    try:
        config = json.loads(repo.read(entry)) if entry else {}
    except (ValueError, TypeError):
        raise SyncError('workspace_metadata_changed') from None
    if not isinstance(config, dict):
        raise SyncError('workspace_metadata_changed')
    if repair:
        # Explicit repair changes only server-owned identity/material metadata.
        # Invalid JSON or invalid unrelated modules still require manual recovery.
        modules = config.get('modules', {})
        if not isinstance(modules, dict):
            raise SyncError('workspace_metadata_changed')
        modules['materials'] = {'path': 'materials', 'partitions': {
            p['skill_id']: p['skill_id'] + '/manifest.json' for p in partitions(app_id, workspace)}}
        config.update(schema_version=2, workspace_id=workspace['workspace_id'], modules=modules)
        config.pop('app_id', None)
    if entry:
        if (config.get('schema_version') != 2 or config.get('workspace_id') != workspace['workspace_id']
                or ('app_id' in config and config['app_id'] != str(app_id))):
            raise SyncError('workspace_metadata_changed')
        modules = config.get('modules')
        if not isinstance(modules, dict):
            raise SyncError('workspace_metadata_changed')
        for name, module in modules.items():
            if not isinstance(module, dict):
                raise SyncError('workspace_metadata_changed')
            location = relative_path(module.get('path'))
            if name == 'materials':
                if location != 'materials':
                    raise SyncError('workspace_metadata_changed')
            elif location == 'materials' or location.startswith('materials/') or 'materials'.startswith(location + '/'):
                raise SyncError('workspace_metadata_changed')
        locations = [m['path'] for m in modules.values()]
        if any(a == b or a.startswith(b + '/') or b.startswith(a + '/')
               for i, a in enumerate(locations) for b in locations[i + 1:]):
            raise SyncError('workspace_metadata_changed')
    elif workspace['config_sha'] and not repair:
        raise SyncError('workspace_metadata_changed')
    elif not repair:
        config = {'schema_version': 2, 'workspace_id': workspace['workspace_id'], 'modules': {}}
    # Accept matching legacy metadata, but remove App IDs on the next normal sync.
    config.pop('app_id', None)
    return config


def run_workspace(app_id, actor, workspace):
    from lanying_workspace_workflow import ensure_workflow
    repo = repository(workspace)
    head, tree = repo.snapshot()
    root = prefix(workspace)
    if root + 'seenical-workspace.json' not in tree and not workspace['config_sha']:
        if any(p == root + 'materials' or p.startswith(root + 'materials/') for p in tree):
            raise SyncError('workspace_metadata_changed')
    config = workspace_config(app_id, workspace, repo, tree)
    bindings = partitions(app_id, workspace)
    # Never accept index edits that could redirect a local Skill to another scope.
    current_index = (config.get('modules', {}).get('materials') or {}).get('partitions', {})
    allowed_index = {p['skill_id']: p['skill_id'] + '/manifest.json' for p in bindings}
    if not isinstance(current_index, dict) or any(allowed_index.get(k) != v for k, v in current_index.items()):
        raise SyncError('workspace_metadata_changed')
    head, tree = ensure_workflow(repo, workspace, head, tree)
    pending, errors = False, []
    for binding in bindings:
        if not binding['active'] or binding['paused']:
            continue
        partition = partition_view(workspace, binding)
        try:
            pending = _run(app_id, actor, partition) or pending
        except (SyncError, materials.MaterialError) as exc:
            # Without a valid scope/manifest do not activate unfinished inputs.
            # Already committed updates only finish their idempotent cleanup.
            from lanying_workspace_candidate import discard
            for row in file_rows(app_id, partition):
                discard(app_id, partition, row)
            update(app_id, partition, status='failed', error_code=str(exc))
            errors.append(str(exc))
    # Re-read after partition commits; root metadata never overwrites user modules.
    head, tree = repo.snapshot()
    config = workspace_config(app_id, workspace, repo, tree)
    config['modules']['materials'] = {'path': 'materials', 'partitions': allowed_index}
    content = _json(config).encode()
    path = prefix(workspace) + 'seenical-workspace.json'
    changes = {}
    if tree.get(path, {}).get('sha') != blob_sha(content):
        changes[path] = content
    if prefix(workspace) + 'README.md' not in tree:
        changes[prefix(workspace) + 'README.md'] = (
            '# Seenical workspace\n\nSee seenical-workspace.json for material partitions. '
            'Skills and websites may coexist outside materials; they are not synchronized by material sync. '
            'Do not commit credentials. Each partition contains its own README and manifest.\n').encode()
    if changes:
        repo.verify_private(workspace['repository_id'])
        repo.commit(head, changes)
    states = [p['status'] for p in partitions(app_id, workspace) if p['active'] and not p['paused']]
    status = 'needs_attention' if errors or any(v != 'synced' for v in states) else 'synced'
    update(app_id, workspace, config_sha=blob_sha(content), status=status,
           error_code=errors[0] if errors else '')
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace SET last_synced_at=CURRENT_TIMESTAMP '
            'WHERE app_id=:a AND workspace_id=:w'), params(app_id, workspace))
    return pending


def operation(app_id, actor, name, data):
    if name == 'list':
        from lanying_workspace_hosting import available
        return {'list': connections(app_id), 'managed_available': available()}
    if name == 'configure':
        return configure(app_id, actor, data)
    if name == 'bind':
        return bind(app_id, actor, data)
    workspace = load(app_id, data.get('workspace_id'))
    skill = data.get('skill_id')
    if name == 'status':
        return public_view(app_id, workspace['workspace_id'], skill)
    if not workspace['repository_id']:
        raise SyncError('workspace_hosting_incomplete')
    if name == 'invite':
        from lanying_workspace_hosting import invite
        with locked(workspace['workspace_id']):
            return invite(app_id, workspace, data)
    if workspace['layout_version'] != 2:
        raise SyncError('workspace_layout_upgrade_required')
    partition = None
    if skill:
        binding = next((p for p in partitions(app_id, workspace) if p['skill_id'] == skill), None)
        if not binding or not binding['active']:
            raise SyncError('workspace_scope_unavailable')
        partition = partition_view(workspace, binding)
        if name not in {'unbind', 'pause'}:
            collect(app_id, actor, partition)
    if name == 'resolve':
        if not partition:
            raise SyncError('workspace_scope_unavailable')
        return resolve(app_id, actor, data, partition)
    if name in {'check', 'dispatch'}:
        enqueue(app_id, actor, workspace['workspace_id'], changed=name == 'check')
    elif name in {'pause', 'unbind', 'repair_metadata'}:
        if name == 'pause' and not isinstance(data.get('paused'), bool):
            raise SyncError('invalid_workspace_pause')
        if name in {'unbind', 'repair_metadata'} and data.get('confirmed') is not True:
            raise SyncError('workspace_confirmation_required')
        with locked(workspace['workspace_id']):
            if name == 'pause':
                update(app_id, partition or workspace, paused=int(data['paused']),
                       status='paused' if data['paused'] else 'pending')
            elif name == 'unbind':
                if not partition:
                    raise SyncError('workspace_scope_unavailable')
                from lanying_workspace_candidate import discard
                for row in file_rows(app_id, partition):
                    discard(app_id, partition, row)
                with materials._engine().begin() as conn:
                    materials._space(conn, app_id, True)
                    conn.execute(text("UPDATE seenical_workspace_partition SET active=0,paused=1,status='paused' "
                        'WHERE app_id=:a AND workspace_id=:w AND skill_id=:s'), params(app_id, partition))
            else:
                repo = repository(workspace)
                head, tree = repo.snapshot()
                config = workspace_config(app_id, workspace, repo, tree, repair=True)
                if not partition:
                    raise SyncError('workspace_scope_unavailable')
                content = _json(config).encode()
                repo.commit(head, {prefix(workspace) + 'seenical-workspace.json': content})
                update(app_id, workspace, config_sha=blob_sha(content))
                update(app_id, partition, manifest_sha=tree.get(prefix(partition) + 'manifest.json', {}).get('sha', ''))
        if name != 'pause' or not data['paused']:
            enqueue(app_id, actor, workspace['workspace_id'])
    else:
        raise SyncError('workspace_operation_invalid')
    return public_view(app_id, workspace['workspace_id'], skill)
