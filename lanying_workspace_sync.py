"""One private material workspace per App; no Skill/configuration synchronization.

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


def load(app_id):
    with materials._engine().connect() as conn:
        row = conn.execute(text('SELECT * FROM seenical_workspace WHERE app_id=:a'),
                           {'a': str(app_id)}).mappings().first()
    return dict(row) if row else None


def update(app_id, **fields):
    allowed = {'paused', 'status', 'error_code', 'manifest_sha', 'config_sha', 'warnings_json'}
    if not fields or not fields.keys() <= allowed:
        raise ValueError('invalid workspace update')
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace SET ' + ','.join(k + '=:' + k for k in fields)
                          + ' WHERE app_id=:a'), dict(fields, a=str(app_id)))


def public_view(app_id):
    row = load(app_id)
    if not row:
        return {'configured': False, 'scopes': [], 'files': []}
    result = {k: row[k] for k in ['workspace_id', 'repository', 'branch', 'directory',
                                  'paused', 'status', 'error_code', 'last_synced_at']}
    result['configured'] = True
    result['warnings'] = json.loads(row.get('warnings_json') or '[]')
    result['scopes'] = json.loads(row['scopes_json'])
    with materials._engine().connect() as conn:
        result['files'] = [dict(r) for r in conn.execute(text('SELECT path_hash,path,source_type,'
            'doc_id,status,error_code,removed FROM seenical_workspace_file WHERE app_id=:a '
            'ORDER BY path'), {'a': str(app_id)}).mappings()]
    return result


def checked_scopes(app_id, actor, scopes):
    if not isinstance(scopes, list) or not 1 <= len(scopes) <= MAX_SCOPES:
        raise SyncError('invalid_workspace_scope')
    checked, seen = [], set()
    for item in scopes:
        if not isinstance(item, dict) or not item.keys() <= {'seenical_session_id', 'task_id'}:
            raise SyncError('invalid_workspace_scope')
        session, ids, knowledge = exports.scope(app_id, actor, item)
        task = str(item.get('task_id') or '')
        if (session, task) in seen:
            continue
        seen.add((session, task))
        checked.append({'seenical_session_id': session, 'task_id': task,
                        'knowledge_base_ids': sorted({str(k['embedding_uuid']) for k in knowledge})})
    return checked


def configure(app_id, actor, data):
    if data.get('confirmed') is not True:
        raise SyncError('workspace_confirmation_required')
    with locked(app_id):
        old = load(app_id)
        repo_name = str(data.get('repository') or '').removeprefix('https://github.com/').removesuffix('.git')
        branch = data.get('branch') or 'main'
        directory = relative_path(data.get('directory', 'seenical-workspace'), allow_empty=True)
        token = data.get('github_token')
        if not token and old:
            try:
                token = _cipher().decrypt(old['github_token'].encode()).decode()
            except InvalidToken:
                raise SyncError('github_reauthorize') from None
        repo = Repository(repo_name, branch, token)
        repository_id = repo.verify_private()
        # Moving a previously populated workspace would need a separate migration.
        # Keep it explicit rather than silently copying data to a second repo.
        if old and (repository_id, branch, directory) != (old['repository_id'], old['branch'], old['directory']):
            raise SyncError('workspace_location_fixed')
        scopes = checked_scopes(app_id, actor, data.get('scopes'))
        head, tree = repo.snapshot()  # Require an initialized branch; no implicit repo creation.
        root = directory + '/' if directory else ''
        if not old and any(p.startswith(root) for p in tree if
                           p in {root + 'seenical-workspace.json', root + 'materials/manifest.json'}
                           or p.startswith(root + 'materials/')):
            raise SyncError('workspace_directory_not_empty')
        values = dict(a=str(app_id), w=old['workspace_id'] if old else uuid.uuid4().hex,
                      repository=repo_name, rid=repository_id, branch=branch, directory=directory,
                      location=hashlib.sha256(_json([repository_id, branch, directory]).encode()).hexdigest(),
                      token=_cipher().encrypt(token.encode()).decode(), actor=_json(actor), scopes=_json(scopes))
        try:
            with materials._engine().begin() as conn:
                if old:
                    conn.execute(text('UPDATE seenical_workspace SET github_token=:token,actor_json=:actor,'
                        "scopes_json=:scopes,paused=0,status='pending',error_code='' WHERE app_id=:a"), values)
                else:
                    conn.execute(text('INSERT INTO seenical_workspace '
                        '(app_id,workspace_id,repository,repository_id,branch,directory,location_hash,github_token,actor_json,scopes_json)'
                        ' VALUES (:a,:w,:repository,:rid,:branch,:directory,:location,:token,:actor,:scopes)'), values)
        except IntegrityError:
            raise SyncError('workspace_location_in_use') from None
    enqueue(app_id, actor)
    return public_view(app_id)


def enqueue(app_id, actor, changed=True):
    if changed:
        request_check(app_id)
    from lanying_tasks import seenical_workspace_sync_task
    seenical_workspace_sync_task.apply_async(args=[str(app_id), actor, int(time.time())])


def request_check(app_id):
    """Persist a change signal, never an authorization or a file operation.

    Two counters prevent a running sync from acknowledging a later event.
    Paused workspaces retain changes until the user resumes them.
    """
    with materials._engine().begin() as conn:
        conn.execute(text('UPDATE seenical_workspace SET requested_generation=requested_generation+1,'
            "status=CASE WHEN status='synced' THEN 'pending' ELSE status END "
            'WHERE app_id=:a'), {'a': str(app_id)})


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
        rows = conn.execute(text('SELECT app_id,actor_json FROM seenical_workspace '
            'WHERE paused=0 AND requested_generation>completed_generation '
            'AND next_check_at<=CURRENT_TIMESTAMP AND app_id>:after ORDER BY app_id LIMIT 100'),
            {'after': str(after)}).mappings().all()
        claimed = []
        for row in rows:
            changed = conn.execute(text('UPDATE seenical_workspace '
                'SET next_check_at=DATE_ADD(CURRENT_TIMESTAMP,INTERVAL 300 SECOND) '
                'WHERE app_id=:a AND paused=0 AND requested_generation>completed_generation '
                'AND next_check_at<=CURRENT_TIMESTAMP'), {'a': row['app_id']})
            if changed.rowcount == 1:
                claimed.append({'app_id': row['app_id'], **json.loads(row['actor_json'])})
        return {'list': claimed}


def resolve(app_id, actor, data):
    """Re-admit a current reference, or confirm content against both current hashes."""
    action = data.get('choice')
    if action not in {'server', 'github', 'delete', 'retry', 'restore', 'relink'}:
        raise SyncError('invalid_workspace_choice')
    with locked(app_id):
        workspace = load(app_id)
        entries = collect(app_id, actor, workspace)
        with materials._engine().connect() as conn:
            row = conn.execute(text('SELECT * FROM seenical_workspace_file WHERE app_id=:a AND path_hash=:h'),
                               {'a': str(app_id), 'h': data.get('path_hash')}).mappings().first()
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
                    'WHERE app_id=:a AND path_hash=:h'), {'j': _json(consent), 'a': str(app_id), 'h': row['path_hash']})
            if action == 'retry' and row['source_type'] == 'conversation':
                from lanying_material_replacement import retry_failed
                retry_failed(app_id, row['doc_id'])
                if current and current['status'] in {'failed', 'cleanup_failed'}:
                    materials.material_api(app_id, actor, 'retry',
                        {'seenical_session_id': row['source_id'], 'doc_id': row['doc_id']})
    enqueue(app_id, actor)
    return public_view(app_id)


def collect(app_id, actor, workspace):
    if not workspace:
        raise SyncError('workspace_not_configured')
    result = {}
    for scope in json.loads(workspace['scopes_json']):
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
        conn.execute(text('UPDATE seenical_workspace_file SET status=:s,error_code=:e'
            + ''.join(',%s=:%s' % (k, k) for k in fields)
            + ' WHERE app_id=:a AND path_hash=:h'),
            dict(fields, a=str(app_id), h=row['path_hash'], s=status, e=error))


def mapped_files(app_id, entries):
    with materials._engine().begin() as conn:
        rows = [dict(r) for r in conn.execute(text('SELECT * FROM seenical_workspace_file WHERE app_id=:a'),
                                             {'a': str(app_id)}).mappings()]
        existing = {(r['source_type'], r['doc_id']) for r in rows}
        for key, item in entries.items():
            if key in existing:
                continue
            row = dict(a=str(app_id), path=item['path'], path_hash=hashlib.sha256(item['path'].encode()).hexdigest(),
                       source_type=item['source_type'], source_id=item['source_id'], doc_id=item['doc_id'],
                       base_sha='', source_fingerprint='', server_sha='', removed=0, decision_json=None, status='pending')
            conn.execute(text('INSERT INTO seenical_workspace_file '
                '(app_id,path_hash,path,source_type,source_id,doc_id) '
                'VALUES (:a,:path_hash,:path,:source_type,:source_id,:doc_id)'), row)
            rows.append(row)
    return rows


def import_new(app_id, workspace, actor, session, path, content):
    """Stable message identity makes retries reuse the same document and charge."""
    import lanying_file_storage as files
    materials.authorized_conversation(app_id, actor, session)
    space = materials.ensure_space(app_id)
    identity = 'github:' + workspace['workspace_id'] + ':' + hashlib.sha256(path.encode()).hexdigest()
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
    materials.enqueue(app_id)
    return doc_id


def run(app_id, actor, authorized_at):
    # A backed-up queue must obtain fresh Butler authorization, not use an old
    # administrator snapshot. The scheduler will enqueue a fresh cycle.
    if not 0 <= time.time() - authorized_at <= 300:
        return
    try:
        with locked(app_id):
            workspace = load(app_id)
            if not workspace or workspace['paused']:
                return
            update(app_id, status='syncing', error_code='')
            pending = _run(app_id, actor, workspace)
            with materials._engine().begin() as conn:
                conn.execute(text('UPDATE seenical_workspace SET completed_generation=:g,'
                    'next_check_at=DATE_ADD(CURRENT_TIMESTAMP,INTERVAL 60 SECOND) WHERE app_id=:a'),
                    {'a': str(app_id), 'g': workspace['completed_generation'] if pending
                     else workspace['requested_generation']})
    except (SyncError, materials.MaterialError) as exc:
        if str(exc) != 'workspace_busy':
            update(app_id, status='failed', error_code=str(exc))
            logging.warning('workspace sync failed | app_id:%s code:%s', app_id, exc)
    except Exception:
        update(app_id, status='failed', error_code='workspace_failed')
        # No exception text: upstream clients may put credentials/body in it.
        logging.warning('workspace sync failed | app_id:%s code:workspace_failed', app_id)


def _run(app_id, actor, workspace):
    from lanying_material_replacement import load as replacement_load, resume, start, discard
    repo = repository(workspace)
    try:
        entries = collect(app_id, actor, workspace)
    except materials.MaterialError:
        # A deleted conversation/plan can no longer authorize a candidate.
        # Release only staged updates (or finish an already committed cleanup),
        # never replace or delete the active source on this failure path.
        with materials._engine().connect() as conn:
            candidates = conn.execute(text('SELECT doc_id FROM seenical_material_replacement '
                "WHERE app_id=:a AND status NOT IN ('done','failed')"), {'a': str(app_id)}).scalars().all()
        for doc_id in candidates:
            discard(app_id, doc_id)
        raise
    head, tree = repo.snapshot()
    root = prefix(workspace)
    # Install before any paid processing. A missing Workflows permission must
    # not leave imported/indexed materials behind a failed GitHub commit.
    from lanying_workspace_workflow import ensure_workflow
    head, tree = ensure_workflow(repo, workspace, head, tree)
    for name, key in [('seenical-workspace.json', 'config_sha'), ('materials/manifest.json', 'manifest_sha')]:
        actual = tree.get(root + name, {}).get('sha', '')
        if actual != workspace[key]:
            raise SyncError('workspace_metadata_changed')
    materials.reconcile_plan_and_run_references(app_id)
    rows = mapped_files(app_id, entries)
    changes, successes, processed, hashed, transferred = {}, [], 0, 0, 0
    for row in rows:
        item = entries.get((row['source_type'], row['doc_id']))
        remote = tree.get(root + row['path'])
        try:
            if remote and (remote['type'], remote['mode']) != ('blob', '100644'):
                raise SyncError('github_regular_file_required')
            if row['source_type'] == 'conversation':
                with materials._engine().connect() as conn:
                    pending = replacement_load(conn, app_id, row['doc_id'])
                if pending and pending['status'] not in {'done', 'failed'}:
                    if row['removed'] or not item or not remote or pending['content_sha'] != remote['sha']:
                        discard(app_id, row['doc_id'])
                    else:
                        outcome = resume(app_id, row['doc_id'])
                        if outcome == 'waiting_runs':
                            file_state(app_id, row, 'waiting_runs')
                            continue
                        if outcome == 'failed':
                            file_state(app_id, row, 'failed', 'material_update_failed')
                            continue
                        if outcome == 'done':
                            row['status'] = 'pending'  # Cleanup recovered; compare the final content again.
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
                with materials._engine().connect() as conn:
                    pending = replacement_load(conn, app_id, row['doc_id'])
                if pending and pending['status'] not in {'done', 'failed'}:
                    result = resume(app_id, row['doc_id'])
                    if pending['content_sha'] != remote_sha:
                        # Finish the already-started operation, then inspect the
                        # newer GitHub edit next cycle. Never overwrite GitHub.
                        file_state(app_id, row, 'pending')
                        continue
                else:
                    if transferred + remote.get('size', materials.MAX_FILE_BYTES) > MAX_CYCLE_BYTES:
                        file_state(app_id, row, 'pending')
                        continue
                    incoming = repo.read(remote)
                    transferred += len(incoming)
                    result = start(app_id, row['doc_id'], incoming, remote_sha)
                if result == 'done':
                    file_state(app_id, row, 'synced', base_sha=remote_sha, source_fingerprint='',
                               server_sha=remote_sha, decision_json=None)
                else:
                    file_state(app_id, row, 'failed' if result == 'failed' else result,
                               'material_update_failed' if result == 'failed' else '')
        except (SyncError, materials.MaterialError) as exc:
            file_state(app_id, row, 'failed', str(exc))
        except Exception:
            file_state(app_id, row, 'failed', 'workspace_file_failed')

    # Only whole-session grants admit new files; LOOP-only scopes never do.
    writable = {exports.component(s['seenical_session_id']): s['seenical_session_id']
                for s in json.loads(workspace['scopes_json']) if not s.get('task_id')}
    known_paths = {r['path'] for r in rows}
    warnings = []
    for path, entry in tree.items():
        if not path.startswith(root + 'materials/') or entry['type'] == 'tree':
            continue
        local = path[len(root):]
        if local in known_paths or local == 'materials/manifest.json':
            continue
        parts = local.split('/')
        session = writable.get(parts[2]) if len(parts) >= 4 and parts[1] == 'conversations' else None
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
        created = mapped_files(app_id, {(item['source_type'], doc_id): item})
        new_row = next(r for r in created if r['doc_id'] == doc_id and r['source_type'] == 'conversation')
        file_state(app_id, new_row, 'processing', base_sha=entry['sha'], server_sha=entry['sha'])
        entries[('conversation', doc_id)] = item

    # Regenerate only server-owned metadata. No prompts, API tokens or Skill code.
    latest_rows = mapped_files(app_id, entries)
    included = []
    for row in latest_rows:
        item = entries.get((row['source_type'], row['doc_id']))
        if not item or row['removed']:
            continue
        path = root + row['path']
        present = ((path in changes and changes[path] is not None)
                   or (path in tree and path not in changes))
        included.append(dict(item, path=row['path'], local_status='ready' if present else 'unavailable'))
    config = {'schema_version': 1, 'workspace_id': workspace['workspace_id'],
              'app_id': str(app_id), 'modules': {'materials': 'materials/manifest.json'}}
    manifest = {'schema_version': 1, 'source': 'github', 'app_id': str(app_id),
                'workspace_id': workspace['workspace_id'], 'documents': included,
                'scope_keys': [s['seenical_session_id'] + '\n' + s.get('task_id', '')
                               for s in json.loads(workspace['scopes_json'])]}
    payloads = {'seenical-workspace.json': _json(config).encode(),
                'materials/manifest.json': _json(manifest).encode()}
    for path, content in payloads.items():
        if tree.get(root + path, {}).get('sha') != blob_sha(content):
            changes[root + path] = content
    if root + 'README.md' not in tree:
        changes[root + 'README.md'] = (
            '# Seenical materials\n\nKnowledge-base copies are read-only. Conversation files sync back '
            'only inside authorized directories. Deletions and conflicts require Console confirmation. '
            'Keep generated articles, Skills, configuration and credentials outside materials/.\n\n'
            '公共知识库为只读副本；授权会话目录中的资料自动同步。删除和冲突需在控制台确认。'
            '不要把文章、技能、配置或凭据放入资料目录。\n').encode()
    if changes:
        repo.verify_private(workspace['repository_id'])
        repo.commit(head, changes)
    # Commit could have succeeded on GitHub even if the response was lost. In
    # that case we stop and expose metadata repair, not replay an overwrite.
    update(app_id, manifest_sha=blob_sha(payloads['materials/manifest.json']),
           config_sha=blob_sha(payloads['seenical-workspace.json']), warnings_json=_json(warnings))
    for row, sha, removed in successes:
        status, error = 'synced', ''
        item = entries.get((row['source_type'], row['doc_id']))
        if item and item['source_type'] == 'conversation' and item['status'] != 'ready':
            status = 'failed' if item['status'] in {'failed', 'cleanup_failed'} else 'processing'
            error = item.get('error_code', '') if status == 'failed' else ''
        file_state(app_id, row, 'removed' if removed else status, error, base_sha=sha, removed=int(removed),
                   decision_json=None)
    with materials._engine().begin() as conn:
        states = conn.execute(text('SELECT status FROM seenical_workspace_file WHERE app_id=:a AND removed=0'),
                              {'a': str(app_id)}).scalars().all()
        unfinished_replacement = conn.execute(text('SELECT 1 FROM seenical_material_replacement '
            "WHERE app_id=:a AND status NOT IN ('done','failed') LIMIT 1"),
            {'a': str(app_id)}).first() is not None
        status = 'synced' if not warnings and all(s == 'synced' for s in states) else 'needs_attention'
        conn.execute(text('UPDATE seenical_workspace SET status=:s,error_code=\'\','
            'last_synced_at=CURRENT_TIMESTAMP WHERE app_id=:a'), {'a': str(app_id), 's': status})
    return unfinished_replacement or any(s in {'pending', 'processing', 'waiting_runs'} for s in states) or any(
        warning['reason'] == 'workspace_pending_file' for warning in warnings)


def operation(app_id, actor, name, data):
    if name == 'status':
        return public_view(app_id)
    if name == 'configure':
        return configure(app_id, actor, data)
    if name == 'resolve':
        return resolve(app_id, actor, data)
    if name == 'check':
        enqueue(app_id, actor)
    elif name == 'dispatch':
        enqueue(app_id, actor, changed=False)
    elif name == 'pause':
        if not isinstance(data.get('paused'), bool):
            raise SyncError('invalid_workspace_pause')
        with locked(app_id):
            update(app_id, paused=int(data['paused']), status='paused' if data['paused'] else 'pending')
        if not data['paused']:
            enqueue(app_id, actor)
    elif name == 'repair_metadata':
        if data.get('confirmed') is not True:
            raise SyncError('workspace_confirmation_required')
        with locked(app_id):
            workspace = load(app_id)
            collect(app_id, actor, workspace)
            repo = repository(workspace)
            head, tree = repo.snapshot()
            update(app_id, config_sha=tree.get(prefix(workspace) + 'seenical-workspace.json', {}).get('sha', ''),
                   manifest_sha=tree.get(prefix(workspace) + 'materials/manifest.json', {}).get('sha', ''))
        enqueue(app_id, actor)
    else:
        raise SyncError('workspace_operation_invalid')
    return public_view(app_id)
