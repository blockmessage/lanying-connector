"""Server-generated, notification-only GitHub workflow; never executes repo files."""
import json
import os
import re
from urllib.parse import urlsplit

from lanying_workspace_github import SyncError


def workflow(workspace):
    identifier = workspace['workspace_id']
    if not re.fullmatch(r'[a-f0-9]{32}', identifier):
        raise SyncError('invalid_workspace_id')
    base = os.getenv('SEENICAL_WORKSPACE_NOTIFY_BASE_URL', 'https://butler.seenical.ai').rstrip('/')
    parsed = urlsplit(base)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or any(c.isspace() for c in base)):
        raise SyncError('workspace_notify_url_invalid')
    # YAML quoting alone does not disable GitHub expression interpolation.
    values = [base, workspace['branch'], workspace['directory']]
    if any('${{' in value for value in values):
        raise SyncError('workspace_workflow_expression_invalid')
    literal = lambda value: re.sub(r'([*?+!\[\]\\])', r'\\\1', value)
    root = literal(workspace['directory']) + '/' if workspace['directory'] else ''
    path = '.github/workflows/seenical-materials-' + identifier + '.yml'
    paths = [root + 'materials/**', root + 'seenical-workspace.json', path]
    url = base + '/seenical/workspaces/' + identifier + '/notify'
    content = f'''# Created by Seenical. Existing workflows are not overwritten by sync.
name: Notify Seenical materials
on:
  push:
    branches: {json.dumps([literal(workspace['branch'])])}
    paths: {json.dumps(paths)}
  workflow_dispatch:
permissions: {{}}
jobs:
  notify:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - name: Notify
        env:
          NOTIFY_URL: {json.dumps(url)}
        run: |
          status=$(curl --fail --silent --show-error --proto '=https' \\
            --connect-timeout 10 --max-time 20 \\
            --retry 3 --retry-delay 60 --retry-max-time 240 \\
            --output /dev/null --write-out '%{{http_code}}' \\
            --request POST "$NOTIFY_URL")
          test "$status" = '202'
'''
    return path, content.encode('utf-8')


def ensure_workflow(repo, workspace, head, tree):
    path, content = workflow(workspace)
    if path in tree:
        if (tree[path]['type'], tree[path]['mode']) != ('blob', '100644'):
            raise SyncError('github_regular_file_required')
        return head, tree  # Preserve user edits, never overwrite an existing workflow.
    repo.verify_private(workspace['repository_id'])
    try:
        repo.commit(head, {path: content})
    except SyncError as exc:
        if str(exc) == 'github_access_denied':
            raise SyncError('github_workflow_write_required') from None
        raise
    return repo.snapshot()
