import contextlib
import copy
import json
import unittest
from unittest import mock

from sqlalchemy import create_engine, text

import lanying_workspace_sync as sync
from lanying_workspace_github import blob_sha, SyncError


class MemoryRepository:
    def __init__(self):
        self.files = {}
        self.writes = []

    def snapshot(self):
        return 'head', {path: {'path': path, 'sha': blob_sha(content), 'type': 'blob',
                              'mode': '100644', 'size': len(content)}
                        for path, content in self.files.items()}

    def verify_private(self, *args):
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
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE seenical_workspace (app_id TEXT PRIMARY KEY,workspace_id TEXT,"
                "repository TEXT,repository_id TEXT,branch TEXT,directory TEXT,github_token TEXT,actor_json TEXT,"
                "scopes_json TEXT,paused INTEGER DEFAULT 0,status TEXT DEFAULT 'pending',error_code TEXT DEFAULT '',"
                "manifest_sha TEXT DEFAULT '',config_sha TEXT DEFAULT '',last_synced_at TEXT,warnings_json TEXT)"))
            conn.execute(text("CREATE TABLE seenical_workspace_file (app_id TEXT,path_hash TEXT,path TEXT,"
                "source_type TEXT,source_id TEXT,doc_id TEXT,base_sha TEXT DEFAULT '',source_fingerprint TEXT DEFAULT '',"
                "server_sha TEXT DEFAULT '',remote_sha TEXT DEFAULT '',status TEXT DEFAULT 'pending',"
                "error_code TEXT DEFAULT '',decision_json TEXT,removed INTEGER DEFAULT 0,"
                "PRIMARY KEY(app_id,path_hash),UNIQUE(app_id,source_type,doc_id))"))
            conn.execute(text("INSERT INTO seenical_workspace (app_id,workspace_id,repository,repository_id,"
                "branch,directory,github_token,actor_json,scopes_json) VALUES "
                "('a','w','owner/repo','1','main','workspace','encrypted-secret','{}',:scopes)"),
                {'scopes': json.dumps([{'seenical_session_id': 's', 'task_id': '', 'knowledge_base_ids': ['kb']}])})
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.repo = MemoryRepository()
        self.data = {'d': b'original', 'k': b'knowledge'}
        self.entries = {
            ('conversation', 'd'): {'source_type': 'conversation', 'source_id': 's', 'doc_id': 'd',
                'path': 'materials/conversations/s/d/file.txt', 'status': 'ready', 'filename': 'file.txt',
                'read_only': False, 'scope_keys': ['s\n'], 'app_id': 'a'},
            ('knowledge_base', 'k'): {'source_type': 'knowledge_base', 'source_id': 'kb', 'doc_id': 'k',
                'path': 'materials/knowledge-bases/kb/k/k.txt', 'status': 'finish', 'filename': 'k.txt',
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
        self.stack.enter_context(mock.patch('lanying_material_replacement.load', return_value=None))
        self.start = self.stack.enter_context(mock.patch('lanying_material_replacement.start', side_effect=self.replace))
        self.import_new = self.stack.enter_context(mock.patch.object(sync, 'import_new', return_value='new'))

    def source(self, item):
        content = self.data[item['doc_id']]
        return {'object': item['doc_id'], 'size': len(content), 'fingerprint': blob_sha(content)}

    def replace(self, app_id, doc_id, content, sha):
        self.data[doc_id] = content
        return 'done'

    def run_cycle(self):
        sync._run('a', {}, sync.load('a'))

    def row(self, doc_id):
        with self.engine.connect() as conn:
            return dict(conn.execute(text('SELECT * FROM seenical_workspace_file WHERE doc_id=:d'),
                                     {'d': doc_id}).mappings().one())

    def remote_path(self, key):
        return 'workspace/' + self.entries[key]['path']

    def test_first_export_and_no_changes_do_not_rewrite_files(self):
        self.run_cycle()
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], b'original')
        self.assertEqual(self.repo.files[self.remote_path(('knowledge_base', 'k'))], b'knowledge')
        self.assertEqual(self.row('d')['base_sha'], blob_sha(b'original'))
        first = copy.deepcopy(self.repo.files)
        self.run_cycle()
        self.assertEqual(first, self.repo.files)
        self.assertEqual(len(self.repo.writes), 1)
        view = sync.public_view('a')
        self.assertNotIn('github_token', view)
        self.assertNotIn('actor_json', view)

    def test_processing_material_export_does_not_claim_index_ready(self):
        self.entries[('conversation', 'd')]['status'] = 'pending'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'processing')
        manifest = json.loads(self.repo.files['workspace/materials/manifest.json'])
        self.assertIn('s\n', manifest['scope_keys'])

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
        manifest = json.loads(self.repo.files['workspace/materials/manifest.json'])
        self.assertEqual(len(manifest['documents']), 2)
        self.assertEqual(sum(d['local_status'] == 'unavailable' for d in manifest['documents']), 1)
        self.run_cycle()
        manifest = json.loads(self.repo.files['workspace/materials/manifest.json'])
        self.assertTrue(all(d['local_status'] == 'ready' for d in manifest['documents']))

    def test_invalid_new_file_does_not_block_other_exports(self):
        self.repo.files['workspace/materials/conversations/s/bad.txt'] = b'bad'
        self.import_new.side_effect = SyncError('material_unsupported_format')
        self.run_cycle()
        self.assertEqual(self.repo.files[self.remote_path(('conversation', 'd'))], b'original')
        self.assertEqual(sync.public_view('a')['warnings'][0]['reason'], 'material_unsupported_format')

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
        self.start.assert_called_once_with('a', 'd', b'user edit', blob_sha(b'user edit'))
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
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'server'})
        self.run_cycle()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'server'})
        self.run_cycle()
        self.assertEqual(self.repo.files[path], b'server')

    def test_remote_delete_waits_for_confirmation_and_retained_original_is_not_reexported(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        del self.repo.files[path]
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'confirm_delete_server')
        self.remove.assert_not_called()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'})
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
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'})
        self.run_cycle()
        self.assertNotIn(path, self.repo.files)

    def remove_remote_and_confirm(self):
        self.run_cycle()
        path = self.remote_path(('conversation', 'd'))
        del self.repo.files[path]
        self.run_cycle()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'delete'})
        self.run_cycle()
        return path

    def test_explicit_restore_reexports_but_retained_reference_alone_does_not(self):
        path = self.remove_remote_and_confirm()
        self.run_cycle()
        self.assertNotIn(path, self.repo.files)
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'})
        self.assertEqual(self.row('d')['removed'], 0)
        self.assertNotIn(path, self.repo.files)  # No inline content write.
        self.run_cycle()
        self.assertEqual(self.repo.files[path], b'original')
        self.assertEqual(self.row('d')['status'], 'synced')

    def test_restore_cannot_reactivate_an_original_outside_current_scope(self):
        self.remove_remote_and_confirm()
        del self.entries[('conversation', 'd')]
        with self.assertRaisesRegex(SyncError, 'workspace_scope_unavailable'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'})
        self.assertEqual(self.row('d')['removed'], 1)
        self.start.assert_not_called()

    def test_restore_preserves_remote_edits_as_a_conflict(self):
        path = self.remove_remote_and_confirm()
        self.repo.files[path] = b'new github content'
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'restore'})
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
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'})
        self.assertEqual(self.row('d')['source_id'], 'other')
        self.assertEqual('workspace/' + self.row('d')['path'], path)
        # Relinking selects no content; concurrent edits still require confirmation.
        self.data['d'] = b'server edit'
        self.repo.files[path] = b'github edit'
        self.run_cycle()
        self.assertEqual(self.row('d')['status'], 'conflict')
        self.start.assert_not_called()
        sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'github'})
        self.run_cycle()
        self.assertEqual(self.data['d'], b'github edit')
        self.assertEqual(self.row('d')['status'], 'synced')
        self.remove.assert_not_called()

    def test_relink_rechecks_authorized_scope_and_current_state(self):
        self.run_cycle()
        with self.assertRaisesRegex(SyncError, 'workspace_preview_changed'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'})
        self.entries[('conversation', 'd')]['scope_keys'] = ['other\n']
        self.run_cycle()
        del self.entries[('conversation', 'd')]
        with self.assertRaisesRegex(SyncError, 'workspace_scope_unavailable'):
            sync.resolve('a', {}, {'path_hash': self.row('d')['path_hash'], 'choice': 'relink'})
        self.assertEqual(self.row('d')['source_id'], 's')

    def test_new_session_file_imports_but_new_public_or_outside_file_does_not(self):
        self.run_cycle()
        self.repo.files['workspace/materials/conversations/s/new.txt'] = b'new'
        self.repo.files['workspace/materials/knowledge-bases/kb/new.txt'] = b'no'
        self.repo.files['outside.txt'] = b'no'
        self.run_cycle()
        self.import_new.assert_called_once()
        self.assertEqual(self.import_new.call_args.args[3], 's')
        self.assertEqual(self.import_new.call_args.args[4], 'materials/conversations/s/new.txt')
        self.assertEqual(sync.public_view('a')['warnings'][0]['reason'], 'workspace_unmapped_file')
        self.assertEqual(self.row('new')['status'], 'processing')

    def test_loop_only_scope_does_not_import_new_session_files(self):
        self.run_cycle()
        with self.engine.begin() as conn:
            conn.execute(text('UPDATE seenical_workspace SET scopes_json=:s'),
                         {'s': json.dumps([{'seenical_session_id': 's', 'task_id': 'p'}])})
        self.repo.files['workspace/materials/conversations/s/new.txt'] = b'new'
        self.run_cycle()
        self.import_new.assert_not_called()

    def test_corrupted_manifest_never_controls_import_or_deletion(self):
        self.run_cycle()
        self.repo.files['workspace/materials/manifest.json'] = b'{"documents":[]}'
        with self.assertRaisesRegex(SyncError, 'metadata_changed'):
            self.run_cycle()
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
            sync.run('a', {}, 0)
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
