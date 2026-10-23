"""Bounded GitHub transport. No arbitrary hosts, redirects, shell or force push."""
import base64
import hashlib
import json
import re
from urllib.parse import quote

import requests


class SyncError(ValueError):
    pass


MAX_FILE_BYTES = 30 * 1024 * 1024
MAX_TREE_ENTRIES = 10000


def relative_path(value, allow_empty=False):
    if not isinstance(value, str) or len(value.encode('utf-8')) > 768:
        raise SyncError('invalid_workspace_path')
    if value == '' and allow_empty:
        return value
    if (not value or '\\' in value or any(ord(c) < 32 for c in value)
            or any(part in {'', '.', '..', '.git'} for part in value.split('/'))):
        raise SyncError('invalid_workspace_path')
    return value


def blob_sha(content):
    return hashlib.sha1(b'blob ' + str(len(content)).encode() + b'\0' + content).hexdigest()


def decision(base, server, remote, read_only=False):
    """Hashes, or None for absent. Never infer removal from an unreadable source."""
    if server == remote:
        return 'synced'
    if base is None:
        if remote is None:
            return 'upload'
        return 'readonly_changed' if read_only else 'conflict'
    if server is None:
        return 'delete_remote' if remote == base else 'confirm_delete_remote'
    if remote is None:
        return 'readonly_changed' if read_only else 'confirm_delete_server'
    if remote == base:
        return 'upload'
    if read_only:
        return 'readonly_changed'
    return 'download' if server == base else 'conflict'


class Repository:
    def __init__(self, repository, branch, token):
        if not isinstance(repository, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
            raise SyncError('invalid_github_repository')
        if any(p in {'.', '..'} for p in repository.split('/')):
            raise SyncError('invalid_github_repository')
        if (not isinstance(branch, str) or not branch or len(branch) > 128
                or any(ord(c) < 33 for c in branch) or '..' in branch):
            raise SyncError('invalid_github_branch')
        if not isinstance(token, str) or not token or len(token) > 512 or any(ord(c) < 33 for c in token):
            raise SyncError('invalid_github_token')
        self.repository, self.branch = repository, branch
        self.base = 'https://api.github.com/repos/' + repository
        self.headers = {'Authorization': 'Bearer ' + token,
                        'Accept': 'application/vnd.github+json',
                        'X-GitHub-Api-Version': '2022-11-28'}

    def call(self, method, path='', body=None, max_bytes=4 * 1024 * 1024,
             accepted=(200, 201)):
        # Error messages deliberately exclude GitHub response bodies and tokens.
        try:
            with requests.request(method, self.base + path, headers=self.headers, json=body,
                                  timeout=(10, 90), allow_redirects=False, stream=True) as response:
                if response.status_code not in accepted:
                    code = ('github_access_denied' if response.status_code in (401, 403, 404)
                            else 'github_changed' if response.status_code in (409, 422)
                            else 'github_unavailable')
                    raise SyncError(code)
                if response.status_code in (204, 404):
                    return {'http_status': response.status_code}
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > max_bytes:
                        raise SyncError('github_response_too_large')
                    chunks.append(chunk)
                value = json.loads(b''.join(chunks))
                if not isinstance(value, dict):
                    raise SyncError('github_invalid_response')
                return value
        except (requests.RequestException, ValueError) as exc:
            if isinstance(exc, SyncError):
                raise
            raise SyncError('github_unavailable') from None

    def verify_private(self, repository_id=None):
        info = self.call('GET')
        if info.get('private') is not True or info.get('archived') or info.get('disabled'):
            raise SyncError('github_private_required')
        if info.get('permissions', {}).get('push') is not True:
            raise SyncError('github_write_required')
        if repository_id and str(info.get('id')) != str(repository_id):
            raise SyncError('github_repository_changed')
        self.default_branch = info.get('default_branch') or 'main'
        return str(info['id'])

    def snapshot(self):
        head = self.call('GET', '/git/ref/heads/' + quote(self.branch, safe=''))['object']['sha']
        commit = self.call('GET', '/git/commits/' + head)
        tree = self.call('GET', '/git/trees/' + commit['tree']['sha'] + '?recursive=1')
        if tree.get('truncated') or len(tree.get('tree', [])) > MAX_TREE_ENTRIES:
            raise SyncError('github_tree_too_large')
        return head, {v['path']: v for v in tree['tree']}

    def read(self, entry):
        if entry.get('type') != 'blob' or entry.get('mode') != '100644':
            raise SyncError('github_regular_file_required')
        if int(entry.get('size', 0)) > MAX_FILE_BYTES:
            raise SyncError('material_file_too_large')
        sha = entry.get('sha', '')
        if not re.fullmatch(r'[0-9a-f]{40}', sha):
            raise SyncError('github_invalid_response')
        result = self.call('GET', '/git/blobs/' + sha, max_bytes=MAX_FILE_BYTES * 2)
        try:
            if result.get('encoding') != 'base64':
                raise ValueError()
            content = base64.b64decode(''.join(result['content'].split()), validate=True)
        except (ValueError, KeyError):
            raise SyncError('github_invalid_response') from None
        if len(content) > MAX_FILE_BYTES or blob_sha(content) != sha:
            raise SyncError('github_invalid_response')
        if content.startswith(b'version https://git-lfs.github.com/spec/'):
            raise SyncError('github_lfs_unsupported')
        return content

    def commit(self, head, changes):
        """changes maps checked repo-relative paths to bytes or None (confirmed removal).

        Build on exactly the inspected head. A concurrent push fails the final
        non-force ref update; next cycle inspects the new tree before deciding.
        """
        if not changes:
            return head
        tree = []
        for path, content in changes.items():
            relative_path(path)
            sha = None
            if content is not None:
                if not isinstance(content, bytes) or len(content) > MAX_FILE_BYTES:
                    raise SyncError('material_file_too_large')
                sha = self.call('POST', '/git/blobs', {
                    'encoding': 'base64', 'content': base64.b64encode(content).decode('ascii')})['sha']
            tree.append({'path': path, 'type': 'blob', 'mode': '100644', 'sha': sha})
        parent = self.call('GET', '/git/commits/' + head)
        new_tree = self.call('POST', '/git/trees', {'base_tree': parent['tree']['sha'], 'tree': tree})
        commit = self.call('POST', '/git/commits', {
            'message': 'Sync Seenical reference materials',
            'tree': new_tree['sha'], 'parents': [head]})
        self.call('PATCH', '/git/refs/heads/' + quote(self.branch, safe=''),
                  {'sha': commit['sha'], 'force': False})
        return commit['sha']
