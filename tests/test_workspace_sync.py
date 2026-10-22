import contextlib
import copy
import json
import unittest
from unittest import mock

from sqlalchemy import create_engine, text, event

import lanying_workspace_sync as sync
from lanying_workspace_github import blob_sha, SyncError

WORKSPACE_ID = '0123456789abcdef0123456789abcdef'


class ScopeConsentTest(unittest.TestCase):
    def test_conversation_grant_follows_files_but_not_new_knowledge_bindings(self):
        with mock.patch.object(sync.exports, 'scope', return_value=('s', ['old'], [])):
            scopes = sync.checked_scopes('a', {}, [{'seenical_session_id': 's'}])
        self.assertNotIn('document_ids', scopes[0])
        docs = [{'source_type': 'conversation', 'source_id': 's', 'doc_id': value}
                for value in ['old', 'new']]
        docs.append({'source_type': 'knowledge_base', 'source_id': 'new-kb', 'doc_id': 'private'})
        with mock.patch.object(sync.exports, 'manifest', side_effect=lambda *args: {'documents': copy.deepcopy(docs)}):
            current = sync.collect('a', {}, {'scopes_json': json.dumps(scopes)})
            self.assertEqual(set(current), {('conversation', 'old'), ('conversation', 'new')})
            self.assertEqual(current[('conversation', 'new')]['scope_keys'], ['s\n'])

    def test_development_loop_grants_require_new_conversation_consent(self):
        scope = {'seenical_session_id': 's', 'task_id': 'p'}
        with self.assertRaisesRegex(SyncError, 'workspace_scope_confirmation_required'):
            sync.checked_scopes('a', {}, [scope])
        for extra in ({}, {'document_ids': ['old']}):
            with self.assertRaisesRegex(SyncError, 'workspace_scope_confirmation_required'):
                sync.collect('a', {}, {'scopes_json': json.dumps([dict(scope, **extra)])})


class MemoryRepository:
    def __init__(self):
        self.files = {}
        self.writes = []

    def snapshot(self):
        return 'head', {path: {'path': path, 'sha': blob_sha(content), 'type': 'blob',
                              'mode': '100644', 'size': len(content)}
                        for path, content in self.files.items()}

    def verify_private(self, *args):
        self.default_branch = 'main'
        return '1'

    def read(self, entry):
        return self.files[entry['path']]

    def commit(self, head, changes):
        self.writes.append(copy.deepcopy(changes))
        for path, content in changes.items():
            if content is None:
                self.files.pop(path, None)
            else:
                self.files[path] = content
        return 'next'


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        @event.listens_for(self.engine, 'before_cursor_execute', retval=True)
        def sqlite_dates(conn, cursor, statement, parameters, context, executemany):
            statement = statement.replace(' FOR UPDATE', '')
            statement = statement.replace('ON DUPLICATE KEY UPDATE active=1,paused=0,knowledge_base_ids=?,',
                'ON CONFLICT(workspace_id,skill_id) DO UPDATE SET active=1,paused=0,knowledge_base_ids=?,')
            for seconds in (60, 300):
                statement = statement.replace(
                    f'DATE_ADD(CURRENT_TIMESTAMP,INTERVAL {seconds} SECOND)',
                    f"datetime('now','+{seconds} seconds')")
            return statement, parameters
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE seenical_workspace (app_id TEXT,workspace_id TEXT PRIMARY KEY,layout_version INTEGER DEFAULT 2,created_at TEXT,"
                "repository_type TEXT DEFAULT 'own',creation_key TEXT,"
                "repository TEXT,repository_id TEXT,branch TEXT,directory TEXT,location_hash TEXT,github_token TEXT,actor_json TEXT,"
                "scopes_json TEXT,paused INTEGER DEFAULT 0,status TEXT DEFAULT 'pending',error_code TEXT DEFAULT '',"
                "manifest_sha TEXT DEFAULT '',config_sha TEXT DEFAULT '',last_synced_at TEXT,warnings_json TEXT,"
                "next_check_at TEXT DEFAULT CURRENT_TIMESTAMP,requested_generation INTEGER DEFAULT 1,"
                "completed_generation INTEGER DEFAULT 0,UNIQUE(app_id,creation_key))"))
            conn.execute(text("CREATE TABLE seenical_workspace_file (app_id TEXT,workspace_id TEXT,skill_id TEXT,path_hash TEXT,path TEXT,"
                "source_type TEXT,source_id TEXT,doc_id TEXT,base_sha TEXT DEFAULT '',source_fingerprint TEXT DEFAULT '',"
                "server_sha TEXT DEFAULT '',remote_sha TEXT DEFAULT '',status TEXT DEFAULT 'pending',"
                "error_code TEXT DEFAULT '',decision_json TEXT,removed INTEGER DEFAULT 0,"
                "PRIMARY KEY(workspace_id,skill_id,path_hash),UNIQUE(workspace_id,skill_id,source_type,doc_id))"))
            conn.execute(text('CREATE TABLE seenical_material_replacement '
                '(app_id TEXT,doc_id TEXT,status TEXT,content_sha TEXT)'))
            conn.execute(text("INSERT INTO seenical_workspace (app_id,workspace_id,repository,repository_id,"
                "branch,directory,github_token,actor_json,scopes_json) VALUES "
                "('a',:w,'owner/repo','1','main','workspace','encrypted-secret','{}',:scopes)"),
                {'w': WORKSPACE_ID, 'scopes': json.dumps([{'seenical_session_id': 's', 'task_id': '', 'knowledge_base_ids': ['kb']}])})
            conn.execute(text("CREATE TABLE seenical_workspace_partition (app_id TEXT,workspace_id TEXT,skill_id TEXT,"
                "seenical_session_id TEXT,knowledge_base_ids TEXT,active INTEGER DEFAULT 1,paused INTEGER DEFAULT 0,"
                "manifest_sha TEXT DEFAULT '',status TEXT DEFAULT 'pending',error_code TEXT DEFAULT '',warnings_json TEXT,"
                "PRIMARY KEY(workspace_id,skill_id))"))
            conn.execute(text("INSERT INTO seenical_workspace_partition "
                "(app_id,workspace_id,skill_id,seenical_session_id,knowledge_base_ids) VALUES ('a',:w,'writing-1','s','[\"kb\"]')"),
                {'w': WORKSPACE_ID})
            conn.execute(text('CREATE TABLE seenical_writing_skill (id INTEGER,app_id TEXT,seenical_session_id TEXT)'))
            conn.execute(text("INSERT INTO seenical_writing_skill VALUES (1,'a','s')"))
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.repo = MemoryRepository()
        self.data = {'d': b'original', 'k': b'knowledge'}
        self.entries = {
            ('conversation', 'd'): {'source_type': 'conversation', 'source_id': 's', 'doc_id': 'd',
                'path': 'conversation/d/file.txt', 'status': 'ready', 'filename': 'file.txt',
                'read_only': False, 'scope_keys': ['s\n'], 'app_id': 'a'},
            ('knowledge_base', 'k'): {'source_type': 'knowledge_base', 'source_id': 'kb', 'doc_id': 'k',
                'path': 'knowledge-bases/kb/k/k.txt', 'status': 'finish', 'filename': 'k.txt',
                'read_only': True, 'scope_keys': ['s\n'], 'app_id': 'a'}}
        self.stack.enter_context(mock.patch.object(sync.materials, '_engine', return_value=self.engine))
        self.stack.enter_context(mock.patch.object(sync, 'repository', return_value=self.repo))
        self.stack.enter_context(mock.patch.object(sync, 'collect', side_effect=lambda *_: copy.deepcopy(self.entries)))
        self.stack.enter_context(mock.patch.object(sync, 'source', side_effect=self.source))
        self.stack.enter_context(mock.patch.object(sync, 'read_source', side_effect=lambda info: self.data[info['object']]))
        self.stack.enter_context(mock.patch.object(sync, 'enqueue'))
        self.stack.enter_context(mock.patch.object(sync, 'locked', side_effect=lambda _: contextlib.nullcontext()))
        self.stack.enter_context(mock.patch.object(sync.materials, 'reconcile_plan_and_run_references'))
        self.stack.enter_context(mock.patch.object(sync.materials, 'authorized_conversation'))
        self.remove = self.stack.enter_context(mock.patch.object(sync.materials, 'remove_reference'))
        self.stack.enter_context(mock.patch('lanying_workspace_candidate.load', return_value=None))
        self.start = self.stack.enter_context(mock.patch('lanying_workspace_candidate.start', side_effect=self.replace))
        self.import_new = self.stack.enter_context(mock.patch.object(sync, 'import_new', return_value='new'))

    def source(self, item):
        content = self.data[item['doc_id']]
        return {'object': item['doc_id'], 'size': len(content), 'fingerprint': blob_sha(content)}

    def replace(self, app_id, workspace, file, content, sha):
        self.data[file['doc_id']] = content
        sync.file_state(app_id, file, 'synced', base_sha=sha, server_sha=sha, source_fingerprint='', decision_json=None)
        return 'done'

    def run_cycle(self):
        sync.run_workspace('a', {}, sync.load('a', WORKSPACE_ID))

    def partition(self):
        workspace = sync.load('a', WORKSPACE_ID)
        return sync.partition_view(workspace, sync.partitions('a', workspace)[0])

    def row(self, doc_id):
        with self.engine.connect() as conn:
            return dict(conn.execute(text('SELECT * FROM seenical_workspace_file WHERE doc_id=:d'),
                                     {'d': doc_id}).mappings().one())

    def remote_path(self, key):
        return 'workspace/materials/writing-1/' + self.entries[key]['path']

    def test_first_export_and_no_changes_do_not_rewrite_files(self):
        self.run_cycle()
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], b'original')
        self.assertEqual(self.repo.files[self.remote_path(('knowledge_base', 'k'))], b'knowledge')
        self.assertEqual(self.row('d')['base_sha'], blob_sha(b'original'))
        first = copy.deepcopy(self.repo.files)
        self.run_cycle()
        self.assertEqual(first, self.repo.files)
        self.assertEqual(len(self.repo.writes), 3)  # Workflow installation, then materials.
        view = sync.public_view('a', WORKSPACE_ID, 'writing-1')
        self.assertNotIn('github_token', view)
        self.assertNotIn('actor_json', view)

    def test_multiple_skill_partitions_keep_mapping_and_pause_independent(self):
        with self.engine.begin() as conn:
            conn.execute(text("INSERT INTO seenical_workspace_partition "
                "(app_id,workspace_id,skill_id,seenical_session_id,knowledge_base_ids) "
                "VALUES ('a',:w,'writing-2','s2','[]')"), {'w': WORKSPACE_ID})
        self.run_cycle()
        first = sync.public_view('a', WORKSPACE_ID, 'writing-1')
        second = sync.public_view('a', WORKSPACE_ID, 'writing-2')
        self.assertEqual(len(first['files']), 2)
        self.assertEqual(len(second['files']), 2)
        original_path = 'workspace/materials/writing-1/conversation/d/file.txt'
        self.repo.files[original_path] = b'first edit'
        sync.operation('a', {}, 'pause', dict(workspace_id=WORKSPACE_ID, skill_id='writing-1', paused=True))
        self.run_cycle()
        self.start.assert_not_called()
        self.assertEqual(sync.public_view('a', WORKSPACE_ID, 'writing-2')['binding']['status'], 'synced')
        self.assertEqual(self.repo.files[original_path], b'first edit')

    def test_file_state_updates_only_the_selected_app_workspace_and_skill(self):
        self.run_cycle()
        original = self.row('d')
        siblings = [('a', WORKSPACE_ID, 'writing-2'),
                    ('a', 'other-workspace', 'writing-1'),
                    ('foreign-app', 'foreign-workspace', 'writing-1')]
        for app_id, workspace_id, skill_id in siblings:
            sync.mapped_files(app_id, {('conversation', 'd'): self.entries[('conversation', 'd')]},
                              dict(workspace_id=workspace_id, skill_id=skill_id))
        with self.engine.connect() as conn:
            before = [dict(r) for r in conn.execute(text('SELECT * FROM seenical_workspace_file '
                'WHERE path_hash=:h ORDER BY app_id,workspace_id,skill_id'),
                {'h': original['path_hash']}).mappings()]
        sync.file_state('a', original, 'removed', 'test-error', removed=1, base_sha='new-base',
                        server_sha='new-server', remote_sha='new-remote', source_fingerprint='new-source',
                        source_id='new-session', decision_json='{}')
        with self.engine.connect() as conn:
            after = [dict(r) for r in conn.execute(text('SELECT * FROM seenical_workspace_file '
                'WHERE path_hash=:h ORDER BY app_id,workspace_id,skill_id'),
                {'h': original['path_hash']}).mappings()]
        changed = 0
        for old, new in zip(before, after):
            if (old['app_id'], old['workspace_id'], old['skill_id']) == ('a', WORKSPACE_ID, 'writing-1'):
                self.assertEqual((new['status'], new['removed'], new['base_sha']), ('removed', 1, 'new-base'))
                changed += 1
            else:
                self.assertEqual(old, new)
        self.assertEqual(changed, 1)

    def test_restored_original_after_copy_gets_an_independent_stable_path(self):
        self.run_cycle()
        original = copy.deepcopy(self.entries[('conversation', 'd')])
        retained_path = self.row('d')['path']
        # Model the mapping committed by a successful copy-on-write update:
        # the new document keeps the old path; the original remains restorable.
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_workspace_file SET doc_id='copy',base_sha=:sha,"
                "server_sha=:sha,source_fingerprint='' WHERE doc_id='d'"), {'sha': blob_sha(b'edited')})
        self.data['copy'] = b'edited'
        self.repo.files['workspace/materials/writing-1/' + retained_path] = b'edited'
        self.entries[('conversation', 'copy')] = dict(original, doc_id='copy', path='conversation/copy/file.txt')
        self.run_cycle()
        restored_path = self.row('d')['path']
        self.assertNotEqual(restored_path, retained_path)
        self.assertEqual(self.row('copy')['path'], retained_path)
        self.assertEqual(self.repo.files['workspace/materials/writing-1/' + retained_path], b'edited')
        self.assertEqual(self.repo.files['workspace/materials/writing-1/' + restored_path], b'original')
        manifest = json.loads(self.repo.files['workspace/materials/writing-1/manifest.json'])
        self.assertEqual({d['doc_id']: d['path'] for d in manifest['documents'] if d['source_type'] == 'conversation'},
                         {'copy': retained_path, 'd': restored_path})
        snapshot = copy.deepcopy(self.repo.files)
        self.run_cycle()
        self.assertEqual(self.repo.files, snapshot)
        self.assertEqual(self.row('d')['path'], restored_path)
        self.import_new.assert_not_called()
        self.start.assert_not_called()

    def test_restored_path_does_not_reuse_retained_or_other_incoming_paths(self):
        self.run_cycle()
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_workspace_file SET doc_id='copy',removed=1 WHERE doc_id='d'"))
        original = self.entries[('conversation', 'd')]
        reserved = dict(original, doc_id='other', path='conversation/d-2/file.txt')
        rows = sync.mapped_files('a', {('conversation', 'd'): original, ('conversation', 'other'): reserved}, self.partition())
        paths = {r['doc_id']: r['path'] for r in rows}
        self.assertEqual(paths['copy'], 'conversation/d/file.txt')
        self.assertEqual(paths['other'], 'conversation/d-2/file.txt')
        self.assertEqual(paths['d'], 'conversation/d-3/file.txt')
        repeated = sync.mapped_files('a', {('conversation', 'd'): original}, self.partition())
        self.assertEqual(sorted(repeated, key=lambda r: r['path']), sorted(rows, key=lambda r: r['path']))

    def test_metadata_preserves_other_modules_readme_and_workflows(self):
        self.repo.files['workspace/README.md'] = b'user readme'
        self.repo.files['.github/workflows/build.yml'] = b'user workflow'
        self.run_cycle()
        path = 'workspace/seenical-workspace.json'
        config = json.loads(self.repo.files[path])
        config['modules'].update(sites={'path': 'sites', 'description': 'user config'}, skills={'path': 'skills'})
        self.repo.files[path] = json.dumps(config).encode()
        self.run_cycle()
        self.assertEqual(json.loads(self.repo.files[path])['modules']['sites'], config['modules']['sites'])
        self.assertEqual(self.repo.files['workspace/README.md'], b'user readme')
        self.assertEqual(self.repo.files['.github/workflows/build.yml'], b'user workflow')
        self.assertFalse(any(p.startswith('workspace/skills/') for p in self.repo.files))
        config['modules']['sites']['path'] = 'materials/private'
        self.repo.files[path] = json.dumps(config).encode()
        with self.assertRaisesRegex(SyncError, 'metadata_changed'):
            self.run_cycle()

    def test_unbind_retains_files_and_allows_same_location_reconnect(self):
        self.run_cycle()
        snapshot = copy.deepcopy(self.repo.files)
        with mock.patch.object(sync.materials, '_space'), mock.patch('lanying_workspace_candidate.discard'):
            result = sync.operation('a', {}, 'unbind', dict(workspace_id=WORKSPACE_ID, skill_id='writing-1', confirmed=True))
        self.assertFalse(result['binding']['active'])
        self.assertEqual(snapshot, self.repo.files)
        with mock.patch.object(sync.exports, 'identity', return_value={'skill_id': 'writing-1', 'seenical_session_id': 's'}), \
                mock.patch.object(sync, 'checked_scopes', return_value=[{'knowledge_base_ids': ['kb']}]):
            sync.bind('a', {}, dict(workspace_id=WORKSPACE_ID, skill_id='writing-1', seenical_session_id='s', confirmed=True))
        self.repo.files[self.remote_path(('conversation', 'd'))] = b'edit while disconnected'
        self.run_cycle()
        self.assertEqual(self.data['d'], b'edit while disconnected')
        self.import_new.assert_not_called()

    def test_binding_another_workspace_requires_disconnect(self):
        other = 'f' * 32
        with self.engine.begin() as conn:
            conn.execute(text("INSERT INTO seenical_workspace(app_id,workspace_id,repository,repository_id,directory,actor_json) "
                "VALUES ('a',:w,'owner/second','2','','{}')"), {'w': other})
        with mock.patch.object(sync.exports, 'identity', return_value={'skill_id': 'writing-1', 'seenical_session_id': 's'}), \
                mock.patch.object(sync, 'checked_scopes', return_value=[{'knowledge_base_ids': []}]):
            with self.assertRaisesRegex(SyncError, 'workspace_skill_already_bound'):
                sync.bind('a', {}, dict(workspace_id=other, skill_id='writing-1', seenical_session_id='s', confirmed=True))
        self.assertEqual(len(sync.connections('a')), 2)
        with self.assertRaisesRegex(SyncError, 'not_configured'):
            sync.operation('foreign', {}, 'status', dict(workspace_id=WORKSPACE_ID))

    def test_failed_root_metadata_reports_safe_state(self):
        self.repo.files['workspace/seenical-workspace.json'] = b'not JSON'
        sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        self.assertEqual(sync.load('a', WORKSPACE_ID)['error_code'], 'workspace_metadata_changed')

    def test_processing_material_export_does_not_claim_index_ready(self):
        self.entries[('conversation', 'd')]['status'] = 'pending'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'processing')
        manifest = json.loads(self.repo.files['workspace/materials/writing-1/manifest.json'])
        self.assertEqual(manifest['seenical_session_id'], 's')

    def test_source_retention_wait_resumes_without_manual_retry(self):
        item = self.entries[('conversation', 'd')]
        item.update(status='indexing', available=False)
        real_source = self.source
        def source(value):
            if value['doc_id'] == 'd' and not item['available']:
                raise AssertionError('Do not read an original that is not stored yet')
            return real_source(value)
        with mock.patch.object(sync, 'source', side_effect=source):
            self.run_cycle()
            self.assertEqual(self.row('d')['status'], 'processing')
            self.assertNotIn(self.remote_path(('conversation', 'd')), self.repo.files)
            item.update(status='ready', available=True)
            self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'synced')
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], b'original')
        self.start.assert_not_called()

    def test_failed_paid_update_still_requires_retry_and_keeps_reason(self):
        self.run_cycle()
        self.repo.files[self.remote_path(('conversation', 'd'))] = b'new'
        self.start.side_effect = None
        self.start.return_value = 'failed'
        self.run_cycle()
        self.run_cycle()
        self.start.assert_called_once()
        self.assertEqual(self.row('d')['status'], 'failed')
        self.assertEqual(self.row('d')['error_code'], 'material_update_failed')

    def test_bounded_initial_export_keeps_pending_files_in_manifest(self):
        with mock.patch.object(sync, 'MAX_CYCLE_FILES', 1):
            self.run_cycle()
        manifest = json.loads(self.repo.files['workspace/materials/writing-1/manifest.json'])
        self.assertEqual(len(manifest['documents']), 2)
        self.assertEqual(sum(d['local_status'] == 'unavailable' for d in manifest['documents']), 1)
        self.run_cycle()
        manifest = json.loads(self.repo.files['workspace/materials/writing-1/manifest.json'])
        self.assertTrue(all(d['local_status'] == 'ready' for d in manifest['documents']))

    def test_invalid_new_file_does_not_block_other_exports(self):
        self.run_cycle()
        self.repo.files['workspace/materials/writing-1/conversation/bad.txt'] = b'bad'
        self.import_new.side_effect = SyncError('material_unsupported_format')
        self.run_cycle()
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], b'original')
        self.assertEqual(sync.public_view('a', WORKSPACE_ID, 'writing-1')['warnings'][0]['reason'], 'material_unsupported_format')

    def test_unregistered_material_directory_is_not_adopted_on_first_sync(self):
        self.repo.files['workspace/materials/writing-1/conversation/private.txt'] = b'private'
        with self.assertRaisesRegex(SyncError, 'metadata_changed'):
            self.run_cycle()
        self.import_new.assert_not_called()
        self.assertEqual(self.repo.writes, [])

    def test_new_connection_cannot_overlap_registered_workspaces(self):
        self.repo.branch = 'main'
        with mock.patch.object(sync, 'Repository', return_value=self.repo), mock.patch.object(sync, '_cipher') as cipher:
            cipher.return_value.encrypt.return_value = b'encrypted'
            for directory in ['', 'workspace', 'workspace/nested']:
                with self.assertRaisesRegex(SyncError, 'location_in_use'):
                    sync.configure('a', {}, dict(repository='owner/repo', github_token='token', directory=directory, confirmed=True))
            result = sync.configure('a', {}, dict(repository='owner/repo', github_token='token', directory='separate', confirmed=True))
        self.assertEqual(result['layout_version'], 2)
        self.assertEqual(result['branch'], 'main')
        self.assertEqual(len(sync.connections('a')), 2)

    def test_explicit_metadata_repair_preserves_files_and_other_modules(self):
        self.run_cycle()
        path = 'workspace/seenical-workspace.json'
        config = json.loads(self.repo.files[path])
        config['modules']['sites'] = {'path': 'sites', 'note': 'keep'}
        config['modules']['materials']['partitions']['writing-1'] = 'writing-2/manifest.json'
        self.repo.files[path] = json.dumps(config).encode()
        original = self.repo.files[self.remote_path(('conversation', 'd'))]
        sync.operation('a', {}, 'repair_metadata', dict(workspace_id=WORKSPACE_ID, skill_id='writing-1', confirmed=True))
        self.run_cycle()
        repaired = json.loads(self.repo.files[path])
        self.assertEqual(repaired['modules']['sites'], {'path': 'sites', 'note': 'keep'})
        self.assertEqual(repaired['modules']['materials']['partitions']['writing-1'], 'writing-1/manifest.json')
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], original)

    def test_public_edit_does_not_backwrite_or_overwrite_user_edit(self):
        self.run_cycle()
        path = self.remote_path(('knowledge_base', 'k'))
        self.repo.files[path] = b'user edit'
        self.run_cycle()
        self.assertEqual(self.row('k')['status'], 'readonly_changed')
        self.assertEqual(self.data['k'], b'knowledge')
        self.assertEqual(self.repo.files[path], b'user edit')
        self.start.assert_not_called()

    def test_conversation_edit_updates_same_document_without_looping_upload(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        self.repo.files[path] = b'user edit'
        self.run_cycle()
        self.assertEqual(self.data['d'], b'user edit')
        self.start.assert_called_once_with('a', mock.ANY, mock.ANY, b'user edit', blob_sha(b'user edit'))
        self.assertEqual(self.start.call_args.args[2]['doc_id'], 'd')
        self.run_cycle()
        self.assertEqual(self.start.call_count, 1)
        self.assertEqual(self.row('d')['status'], 'synced')

    def test_waiting_runs_does_not_claim_update_success(self):
        self.run_cycle()
        self.repo.files[self.remote_path(('conversation', 'd'))] = b'new'
        self.start.side_effect = None
        self.start.return_value = 'waiting_runs'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'waiting_runs')
        self.assertEqual(self.data['d'], b'original')
        self.assertEqual(self.row('d')['base_sha'], blob_sha(b'original'))

    def test_two_sided_conflict_requires_fresh_confirmation(self):
        self.run_cycle()
        self.data['d'] = b'server'
        path = self.remote_path(('conversation', 'd'))
        self.repo.files[path] = b'github'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'conflict')
        self.start.assert_not_called()
        self.repo.files[path] = b'changed again'
        with self.assertRaisesRegex(SyncError, 'preview_changed'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'server'}, self.partition())
        self.run_cycle()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'server'}, self.partition())
        self.run_cycle()
        self.assertEqual(self.repo.files[path], b'server')

    def test_remote_delete_waits_for_confirmation_and_retained_original_is_not_reexported(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        del self.repo.files[path]
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'confirm_delete_server')
        self.remove.assert_not_called()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'}, self.partition())
        self.run_cycle()
        self.remove.assert_called_once_with('a', 's', 'd')
        self.run_cycle()
        self.assertNotIn(path, self.repo.files)
        self.assertEqual(self.row('d')['removed'], 1)
        self.assertEqual(self.data['d'], b'original')

    def test_server_removal_never_automatically_deletes_git_file(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        del self.entries[('conversation', 'd')]
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'confirm_delete_remote')
        self.assertIn(path, self.repo.files)
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'}, self.partition())
        self.run_cycle()
        self.assertNotIn(path, self.repo.files)

    def remove_remote_and_confirm(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        del self.repo.files[path]
        self.run_cycle()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'}, self.partition())
        self.run_cycle()
        return path

    def test_explicit_restore_reexports_but_retained_reference_alone_does_not(self):
        path = self.remove_remote_and_confirm()
        self.run_cycle()
        self.assertNotIn(path, self.repo.files)
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'}, self.partition())
        self.assertEqual(self.row('d')['removed'], 0)
        self.assertNotIn(path, self.repo.files)  # No inline content write.
        self.run_cycle()
        self.assertEqual(self.repo.files[path], b'original')
        self.assertEqual(self.row('d')['status'], 'synced')

    def test_restore_cannot_reactivate_an_original_outside_current_scope(self):
        self.remove_remote_and_confirm()
        del self.entries[('conversation', 'd')]
        with self.assertRaisesRegex(SyncError, 'workspace_scope_unavailable'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'}, self.partition())
        self.assertEqual(self.row('d')['removed'], 1)
        self.start.assert_not_called()

    def test_restore_preserves_remote_edits_as_a_conflict(self):
        path = self.remove_remote_and_confirm()
        self.repo.files[path] = b'new github content'
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'}, self.partition())
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'conflict')
        self.assertEqual(self.repo.files[path], b'new github content')
        self.assertEqual(self.data['d'], b'original')
        self.start.assert_not_called()

    def test_relink_shared_document_to_remaining_scope_preserves_path_and_conflicts(self):
        self.entries[('conversation', 'd')]['scope_keys'] = ['s\n', 'other\n']
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        self.entries[('conversation', 'd')].update(source_id='other', scope_keys=['other\n'])
        self.run_cycle()
        self.assertEqual(self.row('d')['error_code'], 'workspace_scope_changed')
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'}, self.partition())
        self.assertEqual(self.row('d')['source_id'], 'other')
        self.assertEqual('workspace/materials/writing-1/' + self.row('d')['path'], path)
        # Relinking selects no content; concurrent edits still require confirmation.
        self.data['d'] = b'server edit'
        self.repo.files[path] = b'github edit'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'conflict')
        self.start.assert_not_called()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'github'}, self.partition())
        self.run_cycle()
        self.assertEqual(self.data['d'], b'github edit')
        self.assertEqual(self.row('d')['status'], 'synced')
        self.remove.assert_not_called()

    def test_relink_rechecks_authorized_scope_and_current_state(self):
        self.run_cycle()
        with self.assertRaisesRegex(SyncError, 'workspace_preview_changed'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'}, self.partition())
        self.entries[('conversation', 'd')]['scope_keys'] = ['other\n']
        self.run_cycle()
        del self.entries[('conversation', 'd')]
        with self.assertRaisesRegex(SyncError, 'workspace_scope_unavailable'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'}, self.partition())
        self.assertEqual(self.row('d')['source_id'], 's')

    def test_new_session_file_imports_but_new_public_or_outside_file_does_not(self):
        self.run_cycle()
        self.repo.files['workspace/materials/writing-1/conversation/new.txt'] = b'new'
        self.repo.files['workspace/materials/writing-1/knowledge-bases/kb/new.txt'] = b'no'
        self.repo.files['outside.txt'] = b'no'
        self.run_cycle()
        self.import_new.assert_called_once()
        self.assertEqual(self.import_new.call_args.args[3], 's')
        self.assertEqual(self.import_new.call_args.args[4], 'conversation/new.txt')
        self.assertEqual(sync.public_view('a', WORKSPACE_ID, 'writing-1')['warnings'][0]['reason'], 'workspace_unmapped_file')
        self.assertEqual(self.row('new')['status'], 'processing')

    def test_paused_partition_does_not_import_new_session_files(self):
        self.run_cycle()
        with self.engine.begin() as conn:
            conn.execute(text('UPDATE seenical_workspace_partition SET paused=1'))
        self.repo.files['workspace/materials/writing-1/conversation/new.txt'] = b'new'
        self.run_cycle()
        self.import_new.assert_not_called()

    def test_corrupted_manifest_never_controls_import_or_deletion(self):
        self.run_cycle()
        self.repo.files['workspace/materials/writing-1/manifest.json'] = b'{"documents":[]}'
        self.run_cycle()
        self.assertEqual(sync.public_view('a', WORKSPACE_ID, 'writing-1')['binding']['error_code'], 'workspace_metadata_changed')
        self.remove.assert_not_called()
        self.start.assert_not_called()

    def test_export_failure_is_not_treated_as_server_deletion(self):
        self.run_cycle()
        with mock.patch.object(sync, 'source', side_effect=SyncError('material_file_unavailable')):
            self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'failed')
        self.remove.assert_not_called()
        self.assertIn(self.remote_path(('conversation', 'd')), self.repo.files)

    def test_expired_queued_authorization_does_not_start_sync(self):
        with mock.patch.object(sync, '_run') as run:
            sync.run('a', {}, 0, WORKSPACE_ID)
        run.assert_not_called()

    def test_idle_workspace_is_not_a_scheduled_candidate(self):
        with self.engine.begin() as conn:
            conn.execute(text('UPDATE seenical_workspace SET completed_generation=requested_generation'))
        with mock.patch.object(sync.materials.storage, 'is_enabled', return_value=True):
            self.assertEqual(sync.scheduled_candidates(), {'list': []})
            sync.notify(WORKSPACE_ID)
            self.assertEqual(sync.scheduled_candidates()['list'], [{'app_id': 'a', 'workspace_id': WORKSPACE_ID}])
            self.assertEqual(sync.scheduled_candidates(), {'list': []})

    def test_unknown_notify_does_not_create_workspace_or_disclose_existence(self):
        before = sync.load('a', WORKSPACE_ID)['requested_generation']
        self.assertEqual(sync.notify('missing'), sync.notify(WORKSPACE_ID))
        self.assertEqual(sync.load('a', WORKSPACE_ID)['requested_generation'], before + 1)
        with self.assertRaises(SyncError):
            sync.load('missing', WORKSPACE_ID)

    def test_new_event_during_sync_is_not_acknowledged(self):
        with mock.patch.object(sync, '_run', side_effect=lambda *_: sync.request_check('a')):
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        row = sync.load('a', WORKSPACE_ID)
        self.assertEqual(row['completed_generation'], 1)
        self.assertEqual(row['requested_generation'], 2)

    def test_finished_sync_becomes_idle_but_pending_work_keeps_retrying(self):
        with mock.patch.object(sync, '_run', return_value=True):
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        self.assertEqual(sync.load('a', WORKSPACE_ID)['completed_generation'], 0)
        with mock.patch.object(sync, '_run', return_value=False):
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        self.assertEqual(sync.load('a', WORKSPACE_ID)['completed_generation'], 1)

    def test_paused_workspace_retains_event_until_resume(self):
        sync.update('a', sync.load('a', WORKSPACE_ID), paused=1)
        sync.notify(WORKSPACE_ID)
        with mock.patch.object(sync.materials.storage, 'is_enabled', return_value=True):
            self.assertEqual(sync.scheduled_candidates()['list'], [])
            sync.update('a', sync.load('a', WORKSPACE_ID), paused=0)
            self.assertEqual(len(sync.scheduled_candidates()['list']), 1)

    def test_file_conflict_does_not_keep_polling(self):
        self.run_cycle()
        self.data['d'] = b'server edit'
        self.repo.files[self.remote_path(('conversation', 'd'))] = b'github edit'
        self.assertFalse(sync._run('a', {}, self.partition()))
        self.assertEqual(self.row('d')['status'], 'conflict')

    def test_workflow_permission_failure_precedes_material_operations(self):
        with mock.patch.object(self.repo, 'commit', side_effect=SyncError('github_access_denied')):
            with self.assertRaisesRegex(SyncError, 'github_workflow_write_required'):
                self.run_cycle()
        self.start.assert_not_called()
        self.import_new.assert_not_called()
        self.remove.assert_not_called()
        self.assertEqual(self.repo.files, {})

    def test_cleanup_failure_remains_scheduled_then_recovers_without_reindexing(self):
        self.run_cycle()
        sha = self.row('d')['base_sha']
        with self.engine.begin() as conn:
            conn.execute(text("INSERT INTO seenical_material_replacement VALUES ('a','d','cleanup',:sha)"), {'sha': sha})
        pending = {'status': 'cleanup', 'content_sha': sha}
        with mock.patch('lanying_workspace_candidate.load', return_value=pending), \
                mock.patch('lanying_workspace_candidate.resume', side_effect=RuntimeError('temporary storage failure')):
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        self.assertEqual(self.row('d')['status'], 'failed')
        self.assertEqual(sync.load('a', WORKSPACE_ID)['completed_generation'], 0)
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_workspace SET next_check_at='2000-01-01'"))
        with mock.patch.object(sync.materials.storage, 'is_enabled', return_value=True):
            self.assertEqual(sync.scheduled_candidates()['list'], [{'app_id': 'a', 'workspace_id': WORKSPACE_ID}])
        def finish(*_):
            pending['status'] = 'done'
            sync.file_state('a', self.row('d'), 'synced')
            return 'done'
        with mock.patch('lanying_workspace_candidate.load', return_value=pending), \
                mock.patch('lanying_workspace_candidate.resume', side_effect=finish):
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
            sync.run('a', {}, sync.time.time(), WORKSPACE_ID)
        self.assertEqual(self.row('d')['status'], 'synced')
        self.assertEqual(sync.load('a', WORKSPACE_ID)['completed_generation'], 1)
        self.start.assert_not_called()


if __name__ == '__main__':
    unittest.main()
