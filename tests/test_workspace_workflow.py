import os
import unittest
from unittest import mock

import yaml

from lanying_workspace_github import SyncError
from lanying_workspace_workflow import workflow, ensure_workflow


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.workspace = dict(workspace_id='0123456789abcdef0123456789abcdef',
                              repository_id='1', branch='feature/docs', directory='my docs')

    def test_notification_only_yaml_and_escaped_filters(self):
        path, content = workflow(self.workspace)
        data = yaml.load(content, Loader=yaml.BaseLoader)
        self.assertEqual(data['on']['push']['branches'], ['feature/docs'])
        self.assertEqual(data['on']['push']['paths'],
                         ['my docs/materials/**', 'my docs/seenical-workspace.json', path])
        self.assertEqual(data['permissions'], {})
        step = data['jobs']['notify']['steps'][0]
        self.assertEqual(step['env']['NOTIFY_URL'], 'https://butler.seenical.ai/seenical/workspaces/'
                         + self.workspace['workspace_id'] + '/notify')
        self.assertIn('--retry 3', step['run'])
        self.assertIn('test "$status" = \'202\'', step['run'])
        self.assertNotIn('--location', step['run'])
        self.assertNotIn('secrets.', content.decode())
        with mock.patch.dict(os.environ, SEENICAL_WORKSPACE_NOTIFY_BASE_URL='https://preview.example/api/'):
            _, content = workflow(dict(self.workspace, branch='a+b', directory='a[1]'))
        data = yaml.load(content, Loader=yaml.BaseLoader)
        self.assertEqual(data['on']['push']['branches'], ['a\\+b'])
        self.assertEqual(data['on']['push']['paths'][0], 'a\\[1\\]/materials/**')
        self.assertTrue(data['jobs']['notify']['steps'][0]['env']['NOTIFY_URL'].startswith('https://preview.example/api/'))

    def test_reject_invalid_urls_and_expression_interpolation(self):
        for url in ['http://host', 'https://user:password@host', 'https://host?token=x', 'https://host/#x', 'https://']:
            with mock.patch.dict(os.environ, SEENICAL_WORKSPACE_NOTIFY_BASE_URL=url), self.assertRaises(SyncError):
                workflow(self.workspace)
        for field in ('branch', 'directory'):
            with self.assertRaisesRegex(SyncError, 'expression_invalid'):
                workflow(dict(self.workspace, **{field: '${{secrets.TOKEN}}'}))

    def test_existing_workflow_is_preserved_and_missing_one_is_created(self):
        path, content = workflow(self.workspace)
        repo = mock.Mock()
        tree = {path: dict(type='blob', mode='100644', sha='user-edited')}
        self.assertEqual(ensure_workflow(repo, self.workspace, 'head', tree), ('head', tree))
        repo.commit.assert_not_called()
        repo.snapshot.return_value = ('new-head', tree)
        self.assertEqual(ensure_workflow(repo, self.workspace, 'head', {}), ('new-head', tree))
        repo.commit.assert_called_once_with('head', {path: content})

    def test_missing_permission_is_actionable_and_symlink_is_not_replaced(self):
        repo = mock.Mock()
        repo.commit.side_effect = SyncError('github_access_denied')
        with self.assertRaisesRegex(SyncError, 'github_workflow_write_required'):
            ensure_workflow(repo, self.workspace, 'head', {})
        path, _ = workflow(self.workspace)
        with self.assertRaisesRegex(SyncError, 'github_regular_file_required'):
            ensure_workflow(repo, self.workspace, 'head', {path: dict(type='blob', mode='120000')})
