"""Private material repositories owned by the platform, never website repositories.

A connection is reserved before contacting GitHub. A retry uses its server-generated
name and verifies its marker; no caller-selected repository receives platform rights.
"""
import hashlib
import json
import os
import re
import uuid

from sqlalchemy import text

import lanying_seenical_materials as materials
from lanying_workspace_github import Repository, SyncError
from lanying_workspace_store import load, public_view, update


def settings():
    org = os.getenv('GITHUB_HOSTING_ORG', '')
    token = os.getenv('GITHUB_HOSTING_TOKEN', '')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{0,38}', org) or not token:
        raise SyncError('workspace_hosting_unavailable')
    return org, token


def available():
    try:
        settings()
        return True
    except SyncError:
        return False


def managed_repository(workspace, verify=True):
    org, token = settings()
    expected = org + '/seenical-materials-' + workspace['workspace_id']
    if workspace.get('repository_type') != 'managed' or workspace['repository'] != expected:
        raise SyncError('workspace_hosting_identity_changed')
    repo = Repository(expected, workspace['branch'], token)
    if verify:
        if not workspace['repository_id']:
            raise SyncError('workspace_hosting_incomplete')
        repo.verify_private(workspace['repository_id'])
    return repo


def configure(app_id, actor, data, old):
    # Called under the existing per-App configuration lock.
    if any(data.get(k) for k in ('repository', 'branch', 'directory', 'github_token')):
        raise SyncError('invalid_workspace_hosting_fields')
    org, _ = settings()
    if not old:
        key = data.get('creation_key', '')
        if not isinstance(key, str) or not re.fullmatch('[a-f0-9]{32}', key):
            raise SyncError('invalid_workspace_creation_key')
        with materials._engine().connect() as conn:
            existing = conn.execute(text('SELECT workspace_id FROM seenical_workspace '
                'WHERE app_id=:a AND creation_key=:k'), {'a': str(app_id), 'k': key}).scalar()
        if existing:
            old = load(app_id, existing)
        else:
            wid = uuid.uuid4().hex
            name = org + '/seenical-materials-' + wid
            with materials._engine().begin() as conn:
                conn.execute(text('INSERT INTO seenical_workspace '
                    '(app_id,workspace_id,repository_type,creation_key,repository,repository_id,branch,directory,'
                    'location_hash,github_token,actor_json,scopes_json,paused,status) '
                    "VALUES (:a,:w,'managed',:k,:r,'','main','',:h,'',:actor,'[]',1,'provisioning')"),
                    dict(a=str(app_id), w=wid, k=key, r=name,
                         h=hashlib.sha256(('managed:' + wid).encode()).hexdigest(),
                         actor=json.dumps(actor)))
            old = load(app_id, wid)
    if old.get('repository_type') != 'managed':
        raise SyncError('workspace_hosting_type_fixed')
    if old['repository_id']:
        with materials._engine().begin() as conn:
            conn.execute(text('UPDATE seenical_workspace SET actor_json=:actor WHERE app_id=:a AND workspace_id=:w'),
                         dict(actor=json.dumps(actor), a=str(app_id), w=old['workspace_id']))
        return public_view(app_id, old['workspace_id'])  # Do not unpause on a duplicate submission.
    try:
        repo = managed_repository(old, verify=False)
        marker = 'Seenical material workspace ' + old['workspace_id']
        info = repo.call('GET', accepted=(200, 404))
        if info.get('http_status') == 404:
            # A fixed API endpoint; name, owner and privacy are exclusively server-owned.
            creator = Repository(repo.repository, 'main', settings()[1])
            creator.base = 'https://api.github.com/orgs/' + org + '/repos'
            info = creator.call('POST', body={
                'name': old['repository'].split('/')[1], 'description': marker,
                'private': True, 'auto_init': True, 'has_issues': False,
                'has_projects': False, 'has_wiki': False})
        if (info.get('full_name', '').lower() != old['repository'].lower()
                or info.get('description') != marker):
            raise SyncError('workspace_hosting_identity_changed')
        rid = repo.verify_private()
        repo.branch = repo.default_branch
        repo.snapshot()  # Wait for the initial commit; manual retry keeps this same repository.
        location = hashlib.sha256(json.dumps([rid, repo.branch, ''],
            ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        with materials._engine().begin() as conn:
            conn.execute(text('UPDATE seenical_workspace SET repository_id=:rid,branch=:b,location_hash=:h,'
                "actor_json=:actor,paused=0,status='pending',error_code='' WHERE app_id=:a AND workspace_id=:w"),
                dict(rid=rid, b=repo.branch, h=location, actor=json.dumps(actor), a=str(app_id), w=old['workspace_id']))
    except SyncError as exc:
        # Keep the connection visible and retryable, even if GitHub created it before timing out.
        update(app_id, old, paused=1, status='provisioning', error_code=str(exc))
    return public_view(app_id, old['workspace_id'])


def invite(app_id, workspace, data):
    if data.get('confirmed') is not True:
        raise SyncError('workspace_confirmation_required')
    username = data.get('github_username', '')
    if not isinstance(username, str) or not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?', username):
        raise SyncError('invalid_github_username')
    repo = managed_repository(workspace)
    result = repo.call('PUT', '/collaborators/' + username,
                       {'permission': 'push'}, accepted=(201, 204))
    view = public_view(app_id, workspace['workspace_id'])
    view['invitation'] = {'username': username,
                          'status': 'active' if result.get('http_status') == 204 else 'pending'}
    return view
