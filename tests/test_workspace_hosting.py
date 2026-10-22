import copy
import os
import unittest
from unittest import mock

from sqlalchemy import text
import lanying_workspace_hosting as hosting
import lanying_workspace_sync as sync
from lanying_workspace_github import Repository, SyncError
import test_workspace_sync as fixtures

SYNC_REPOSITORY = sync.repository


class Platform:
    def __init__(self):
        self.repos = {}
        self.creations = []
        self.invites = []
        self.timeout_after_create = False
        self.invite_error = False
        self.accepted = False

    def repository(self, name, branch, token):
        platform = self
        class Repo:
            repository = name
            base = ''

            def call(self, method, path='', body=None, accepted=(200, 201)):
                if method == 'GET':
                    return copy.deepcopy(platform.repos.get(name, {'http_status': 404}))
                if method == 'POST':
                    platform.creations.append((name, token, copy.deepcopy(body)))
                    platform.repos[name] = dict(full_name=name, description=body['description'],
                        private=body['private'], id='42', permissions={'push': True}, default_branch='main')
                    if platform.timeout_after_create:
                        platform.timeout_after_create = False
                        raise SyncError('github_unavailable')
                    return copy.deepcopy(platform.repos[name])
                if method == 'PUT':
                    platform.invites.append((name, path, body))
                    if platform.invite_error:
                        raise SyncError('github_access_denied')
                    return {'http_status': 204} if platform.accepted else {'id': 1}

            def verify_private(self, repository_id=None):
                info = platform.repos[name]
                if not info['private'] or (repository_id and info['id'] != repository_id):
                    raise SyncError('github_private_required')
                self.default_branch = info['default_branch']
                return info['id']

            def snapshot(self):
                return 'head', {'README.md': {}}
        return Repo()


class HostingTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.WorkspaceTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.platform = Platform()
        self.fixture.stack.enter_context(mock.patch.dict(os.environ,
            GITHUB_HOSTING_ORG='platform', GITHUB_HOSTING_TOKEN='platform-secret'))
        self.fixture.stack.enter_context(mock.patch.object(hosting, 'Repository', side_effect=self.platform.repository))
        self.data = dict(repository_type='managed', creation_key='a' * 32, confirmed=True)

    def create(self, **extra):
        return sync.configure('a', {'actor_subject_id': 'admin'}, dict(self.data, **extra))

    def test_creation_is_private_and_idempotent_without_stored_platform_token(self):
        first = self.create()
        again = self.create()
        self.assertTrue(first['configured'])
        self.assertEqual(first['workspace_id'], again['workspace_id'])
        self.assertEqual(len(self.platform.creations), 1)
        name, token, body = self.platform.creations[0]
        self.assertEqual(name, 'platform/seenical-materials-' + first['workspace_id'])
        self.assertEqual(token, 'platform-secret')
        self.assertIs(body['private'], True)
        self.assertIs(body['auto_init'], True)
        row = sync.load('a', first['workspace_id'])
        self.assertEqual(row['github_token'], '')
        self.assertNotIn('platform-secret', str(first))
        self.assertNotIn('github_token', str(sync.connections('a')))
        self.assertNotIn('creation_key', str(first))
        sync.update('a', row, paused=1)
        self.assertEqual(self.create()['paused'], 1, 'Retry does not undo a later pause')

    def test_timeout_after_creation_reuses_reserved_connection_and_remote_repository(self):
        self.platform.timeout_after_create = True
        failed = self.create()
        self.assertFalse(failed['configured'])
        self.assertEqual(failed['error_code'], 'github_unavailable')
        self.assertEqual(failed['paused'], 1)
        for operation, data in [('pause', {'paused': False}), ('check', {}), ('bind', {'confirmed': True})]:
            with self.assertRaisesRegex(SyncError, 'hosting_incomplete'):
                sync.operation('a', {}, operation, dict(workspace_id=failed['workspace_id'], **data))
        complete = self.create()
        self.assertTrue(complete['configured'])
        self.assertEqual(complete['workspace_id'], failed['workspace_id'])
        self.assertEqual(len(self.platform.creations), 1)

    def test_pending_creation_refuses_unrelated_or_public_remote_repository(self):
        self.platform.timeout_after_create = True
        pending = self.create()
        name = pending['repository']
        original = copy.deepcopy(self.platform.repos[name])
        self.platform.repos[name]['description'] = 'somebody else'
        self.assertEqual(self.create()['error_code'], 'workspace_hosting_identity_changed')
        self.platform.repos[name] = dict(original, private=False)
        self.assertEqual(self.create()['error_code'], 'github_private_required')
        self.assertEqual(len(self.platform.creations), 1)

    def test_cannot_choose_platform_repository_or_change_connection_type(self):
        for field in ('repository', 'branch', 'directory', 'github_token'):
            with self.assertRaisesRegex(SyncError, 'hosting_fields'):
                self.create(**{field: 'arbitrary'})
        with self.assertRaisesRegex(SyncError, 'confirmation_required'):
            self.create(confirmed=False)
        with self.assertRaisesRegex(SyncError, 'creation_key'):
            self.create(creation_key='../other')
        with self.assertRaisesRegex(SyncError, 'hosting_type_fixed'):
            self.create(workspace_id=fixtures.WORKSPACE_ID)
        self.assertEqual(self.platform.creations, [])

    def test_no_cross_app_access_even_with_same_creation_key(self):
        first = self.create()
        with self.assertRaisesRegex(SyncError, 'not_configured'):
            sync.configure('other', {}, dict(self.data, workspace_id=first['workspace_id']))
        second = sync.configure('other', {}, self.data)
        self.assertNotEqual(first['workspace_id'], second['workspace_id'])
        self.assertNotEqual(first['repository'], second['repository'])

    def test_platform_credentials_only_for_server_assigned_managed_repository(self):
        view = self.create()
        row = sync.load('a', view['workspace_id'])
        for bad in [dict(row, repository='platform/another-customer'), dict(row, repository_type='own')]:
            with self.assertRaisesRegex(SyncError, 'identity_changed'):
                hosting.managed_repository(bad)
        with mock.patch.dict(os.environ, GITHUB_HOSTING_ORG='new-owner'):
            with self.assertRaisesRegex(SyncError, 'identity_changed'):
                hosting.managed_repository(row)
        self.platform.repos[row['repository']]['id'] = '999'
        with self.assertRaises(SyncError):
            hosting.managed_repository(row)

    def test_sync_uses_current_platform_token_but_own_connections_still_decrypt_their_token(self):
        view = self.create()
        row = sync.load('a', view['workspace_id'])
        with mock.patch.object(sync, '_cipher') as cipher, mock.patch.object(hosting, 'Repository') as constructor:
            with mock.patch.dict(os.environ, GITHUB_HOSTING_TOKEN='rotated-platform-token'):
                SYNC_REPOSITORY(row)
            constructor.assert_called_once_with(row['repository'], row['branch'], 'rotated-platform-token')
            cipher.assert_not_called()
        with mock.patch.object(sync, '_cipher') as cipher, mock.patch.object(sync, 'Repository') as constructor:
            cipher.return_value.decrypt.return_value = b'own-user-token'
            SYNC_REPOSITORY(sync.load('a', fixtures.WORKSPACE_ID))
            constructor.assert_called_once_with('owner/repo', 'main', 'own-user-token')

    def test_reconfirm_updates_authorizing_actor_without_resuming_a_paused_workspace(self):
        view = self.create()
        sync.update('a', sync.load('a', view['workspace_id']), paused=1)
        result = sync.configure('a', {'actor_subject_id': 'new-admin'},
            dict(workspace_id=view['workspace_id'], repository_type='managed', confirmed=True))
        self.assertEqual(result['paused'], 1)
        self.assertIn('new-admin', sync.load('a', view['workspace_id'])['actor_json'])
        self.assertEqual(len(self.platform.creations), 1)

    def test_invites_explicit_repo_wide_write_without_changing_sync_on_failure(self):
        view = self.create()
        args = dict(workspace_id=view['workspace_id'], github_username='octocat', confirmed=True)
        result = sync.operation('a', {}, 'invite', args)
        self.assertEqual(result['invitation']['status'], 'pending')
        self.assertEqual(self.platform.invites[-1][2], {'permission': 'push'})
        self.platform.accepted = True
        self.assertEqual(sync.operation('a', {}, 'invite', args)['invitation']['status'], 'active')
        before = sync.load('a', view['workspace_id'])
        self.platform.invite_error = True
        with self.assertRaises(SyncError):
            sync.operation('a', {}, 'invite', args)
        self.assertEqual(before, sync.load('a', view['workspace_id']))
        for bad in [dict(args, confirmed=False), dict(args, github_username='../admin'),
                    dict(args, workspace_id=fixtures.WORKSPACE_ID)]:
            with self.assertRaises(SyncError):
                sync.operation('a', {}, 'invite', bad)
        with self.assertRaisesRegex(SyncError, 'not_configured'):
            sync.operation('other', {}, 'invite', args)

    def test_unconfigured_platform_is_not_offered_and_creates_no_record(self):
        with mock.patch.dict(os.environ, GITHUB_HOSTING_TOKEN=''):
            self.assertFalse(sync.operation('a', {}, 'list', {})['managed_available'])
            with self.assertRaisesRegex(SyncError, 'hosting_unavailable'):
                self.create()
        self.assertEqual(len(sync.connections('a')), 1)


class HostingTransportTest(unittest.TestCase):
    def test_empty_success_and_explicit_not_found_are_handled_without_body_or_credentials(self):
        repo = Repository('owner/repo', 'main', 'secret')
        for status in (204, 404):
            response = mock.MagicMock(status_code=status)
            response.__enter__.return_value = response
            with mock.patch('lanying_workspace_github.requests.request', return_value=response) as request:
                result = repo.call('GET', accepted=(status,))
                self.assertEqual(result, {'http_status': status})
                self.assertFalse(request.call_args.kwargs['allow_redirects'])
                response.iter_content.assert_not_called()
