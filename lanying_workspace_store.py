"""Workspace/partition persistence. Every locator is scoped to its authenticated App."""
import json
from sqlalchemy import text
import lanying_seenical_materials as materials
from lanying_workspace_github import SyncError


def params(app_id, workspace, **extra):
    result = dict(a=str(app_id), w=workspace['workspace_id'], s=workspace.get('skill_id', ''))
    result.update(extra)
    return result


def load(app_id, workspace_id):
    with materials._engine().connect() as conn:
        row = conn.execute(text('SELECT * FROM seenical_workspace WHERE app_id=:a AND workspace_id=:w'),
                           {'a': str(app_id), 'w': str(workspace_id)}).mappings().first()
    if not row:
        raise SyncError('workspace_not_configured')
    return dict(row)


def update(app_id, workspace, **fields):
    allowed = {'paused', 'status', 'error_code', 'manifest_sha', 'config_sha', 'warnings_json'}
    if not fields or not fields.keys() <= allowed:
        raise ValueError('invalid workspace update')
    partition = bool(workspace.get('skill_id'))
    if partition:
        fields = {k: v for k, v in fields.items() if k in {'paused', 'status', 'error_code', 'manifest_sha', 'warnings_json'}}
    if fields:
        table = 'seenical_workspace_partition' if partition else 'seenical_workspace'
        with materials._engine().begin() as conn:
            conn.execute(text('UPDATE ' + table + ' SET ' + ','.join(k + '=:' + k for k in fields)
                + ' WHERE app_id=:a AND workspace_id=:w' + (' AND skill_id=:s' if partition else '')),
                params(app_id, workspace, **fields))


def partitions(app_id, workspace):
    with materials._engine().connect() as conn:
        return [dict(r) for r in conn.execute(text('SELECT * FROM seenical_workspace_partition '
            'WHERE app_id=:a AND workspace_id=:w ORDER BY skill_id'), params(app_id, workspace)).mappings()]


def partition_view(workspace, binding):
    value = dict(workspace)
    value.update(binding)
    value['directory'] = (workspace['directory'] + '/' if workspace['directory'] else '') + 'materials/' + binding['skill_id']
    value['scopes_json'] = json.dumps([{'seenical_session_id': binding['seenical_session_id'],
        'skill_id': binding['skill_id'], 'knowledge_base_ids': json.loads(binding['knowledge_base_ids'])}])
    return value


def file_rows(app_id, workspace):
    with materials._engine().connect() as conn:
        return [dict(r) for r in conn.execute(text('SELECT * FROM seenical_workspace_file '
            'WHERE app_id=:a AND workspace_id=:w AND skill_id=:s ORDER BY path'), params(app_id, workspace)).mappings()]


def connections(app_id):
    with materials._engine().connect() as conn:
        rows = [dict(r) for r in conn.execute(text('SELECT workspace_id,repository,branch,directory,'
            'repository_type,layout_version,paused,status,error_code FROM seenical_workspace WHERE app_id=:a ORDER BY created_at'),
            {'a': str(app_id)}).mappings()]
        bindings = [dict(r) for r in conn.execute(text('SELECT workspace_id,skill_id,seenical_session_id,active,paused,status,error_code FROM '
            'seenical_workspace_partition WHERE app_id=:a'), {'a': str(app_id)}).mappings()]
    for row in rows:
        row['partitions'] = [b for b in bindings if b['workspace_id'] == row['workspace_id']]
    return rows


def public_view(app_id, workspace_id, skill_id=None):
    row = load(app_id, workspace_id)
    result = {k: row[k] for k in ['workspace_id', 'repository', 'branch', 'directory', 'layout_version',
        'repository_type', 'paused', 'status', 'error_code', 'last_synced_at']}
    result.update(configured=bool(row['repository_id']), files=[], warnings=json.loads(row.get('warnings_json') or '[]'))
    bindings = partitions(app_id, row)
    result['partitions'] = [{k: p[k] for k in ('skill_id', 'seenical_session_id', 'active', 'paused', 'status', 'error_code')}
                            for p in bindings]
    if skill_id:
        binding = next((p for p in bindings if p['skill_id'] == skill_id), None)
        result['binding'] = next((p for p in result['partitions'] if p['skill_id'] == skill_id), None)
        if binding:
            result['warnings'] = json.loads(binding.get('warnings_json') or '[]')
            result['files'] = [{k: r[k] for k in ('path_hash', 'path', 'source_type', 'doc_id', 'status', 'error_code', 'removed')}
                               for r in file_rows(app_id, binding)]
    return result
