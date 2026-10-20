import base64
import unittest
from unittest import mock

from lanying_workspace_github import Repository, SyncError, relative_path, decision, blob_sha


class GithubTest(unittest.TestCase):
    def test_reject_urls_credentials_traversal(self):
        for value in ['https://evil.test/repo', '../repo', 'owner/repo/../../x', 'owner@host/repo']:
            with self.assertRaises(SyncError):
                Repository(value, 'main', 'secret')
        for path in ['../file', '/file', 'a//b', 'a/./b', 'a\\b', '.git/hooks/x', 'a\nb']:
            with self.assertRaises(SyncError):
                relative_path(path)

    def test_decisions_never_auto_delete_or_write_public_library(self):
        for server, remote in [('old', 'new'), ('new', 'other'), ('old', None)]:
            self.assertEqual(decision('old', server, remote, True), 'readonly_changed')
        self.assertEqual(decision('old', 'old', None), 'confirm_delete_server')
        self.assertEqual(decision('old', None, 'old'), 'confirm_delete_remote')
        self.assertEqual(decision('old', 'new', 'other'), 'conflict')
        self.assertEqual(decision('old', 'old', 'new'), 'download')
        self.assertEqual(decision('old', 'new', 'old'), 'upload')
        self.assertEqual(decision(None, 'new', 'other'), 'conflict')
        self.assertEqual(decision(None, 'same', 'same'), 'synced')

    def test_snapshot_rejects_truncated_tree_instead_of_assuming_removals(self):
        repo = Repository('owner/repo', 'main', 'secret')
        with mock.patch.object(repo, 'call', side_effect=[
            {'object': {'sha': 'a'}}, {'tree': {'sha': 'b'}}, {'truncated': True, 'tree': []}]):
            with self.assertRaisesRegex(SyncError, 'tree_too_large'):
                repo.snapshot()

    def test_private_and_permission_required(self):
        repo = Repository('owner/repo', 'main', 'secret')
        for data in [{'private': False}, {'private': True, 'permissions': {'push': False}},
                     {'private': True, 'id': 2, 'permissions': {'push': True}}]:
            with mock.patch.object(repo, 'call', return_value=data), self.assertRaises(SyncError):
                repo.verify_private('1')

    def test_blob_integrity_and_symlink_lfs_rejected(self):
        repo = Repository('owner/repo', 'main', 'secret')
        content = b'hello'
        entry = {'type': 'blob', 'mode': '100644', 'size': 5, 'sha': blob_sha(content)}
        with mock.patch.object(repo, 'call', return_value={'encoding': 'base64', 'content': base64.b64encode(content).decode()}):
            self.assertEqual(repo.read(entry), content)
            with self.assertRaises(SyncError):
                repo.read(dict(entry, mode='120000'))
            with self.assertRaises(SyncError):
                repo.read(dict(entry, sha='a' * 40))

    def test_commit_retains_base_tree_and_never_forces_ref(self):
        repo = Repository('owner/repo', 'main', 'secret')
        with mock.patch.object(repo, 'call', side_effect=[
            {'sha': 'blob'}, {'tree': {'sha': 'oldtree'}}, {'sha': 'newtree'},
            {'sha': 'newcommit'}, {}]) as call:
            self.assertEqual(repo.commit('oldcommit', {'workspace/a.txt': b'a'}), 'newcommit')
        self.assertEqual(call.call_args_list[2].args[2]['base_tree'], 'oldtree')
        self.assertEqual(call.call_args_list[3].args[2]['parents'], ['oldcommit'])
        self.assertEqual(call.call_args_list[4].args[2], {'sha': 'newcommit', 'force': False})

    def test_transport_does_not_follow_redirects_or_echo_response(self):
        repo = Repository('owner/repo', 'main', 'secret')
        response = mock.MagicMock(status_code=302)
        response.__enter__.return_value = response
        with mock.patch('lanying_workspace_github.requests.request', return_value=response) as call:
            with self.assertRaisesRegex(SyncError, '^github_unavailable$'):
                repo.call('GET')
            self.assertFalse(call.call_args.kwargs['allow_redirects'])


if __name__ == '__main__':
    unittest.main()
