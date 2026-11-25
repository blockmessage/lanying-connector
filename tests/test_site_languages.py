import json
import unittest
from unittest.mock import Mock, patch

import lanying_site_languages as languages


class SiteLanguagesTest(unittest.TestCase):
    def test_homepage_changes_without_reassigning_either_language(self):
        settings = languages.normalize({'default_language': 'en', 'overrides': {'en': {'title': 'Guide'}}}, 'zh-hans')
        layout = languages.layout_for('zh-hans', settings, 'zh-hans')
        self.assertEqual('en', layout['default_language'])
        self.assertEqual('/', layout['languages']['zh-hans']['public_path'])
        self.assertEqual('/en/', layout['languages']['en']['public_path'])
        self.assertEqual('zh-hans', languages.release_language(layout, {'article_language': 'zh-hans'}, 'zh-hans'))
        self.assertEqual('zh-hans', languages.release_language(layout, {}, 'zh-hans'))

    def test_publish_uses_batch_language_and_validates_destination(self):
        layout = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        with self.assertRaisesRegex(ValueError, 'not enabled'):
            languages.release_language(layout, {'article_language': 'en'}, 'zh-hans')
        self.assertEqual('', languages.release_language(None, {'article_language': 'en'}, 'zh-hans'))
        self.assertEqual('/en/articles', languages.scoped_directory('/en', '/articles'))
        with self.assertRaisesRegex(ValueError, 'inside'):
            languages.scoped_directory('/en', '../zh-hans/articles')

    def test_legacy_directory_collision_stops_before_writing(self):
        repo = Mock()
        repo.read.side_effect = [None, b'make refine release_finish']
        repo.json.return_value = {'language': 'zh-hans'}
        repo.request.return_value = {'tree': [{'path': 'en/assets', 'type': 'tree'}]}
        with self.assertRaisesRegex(ValueError, 'directory'):
            languages.legacy_upgrade_entries(repo, 'base', 'zh-hans', 'master', 'master')
        repo.blob.assert_not_called()

    @patch.object(languages, 'Repository')
    def test_legacy_upgrade_moves_content_and_keeps_gitbook_tooling(self, template):
        repo = Mock()
        repo.read.side_effect = [None, b'make refine release_finish']
        repo.json.return_value = {'language': 'zh-hans'}
        repo.request.return_value = {'tree': [
            {'path': name, 'sha': f'sha-{name}', 'mode': '100644', 'type': 'blob'}
            for name in ['README.md', 'SUMMARY.md', 'articles/guide.md', 'assets/logo.png', 'scripts/helper.py']
        ]}
        template.return_value.read.return_value = b'template'
        changes = languages.legacy_upgrade_entries(repo, 'base', 'zh-hans', 'master', 'master')
        self.assertIsNone(changes['articles/guide.md']['sha'])
        self.assertEqual('sha-articles/guide.md', changes['zh-hans/articles/guide.md']['sha'])
        self.assertEqual('sha-assets/logo.png', changes['zh-hans/assets/logo.png']['sha'])
        self.assertNotIn('scripts/helper.py', changes)

    def test_new_english_site_replaces_only_template_seed(self):
        repo = Mock()
        repo.read.return_value = None
        book = {'pluginsConfig': {'edit-link': {'base': 'github'}, 'lanying-grow-ai': {}}}
        repo.json.side_effect = lambda path, *_args, **_opts: book if path == 'book.json' else None
        repo.request.return_value = {'tree': [
            {'path': 'zh-hans/README.md', 'mode': '100644', 'type': 'blob', 'sha': 'readme'},
            {'path': 'zh-hans/assets/logo.png', 'mode': '100644', 'type': 'blob', 'sha': 'logo'},
        ]}
        blobs = {}
        def blob(data):
            key = str(len(blobs))
            blobs[key] = data
            return key
        repo.blob.side_effect = blob

        current = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        site = {'language': 'en', 'name': 'English docs', 'site_language_seed_pending': 'on'}
        languages.sync(repo, 'master', '', 'seed', current, site, lambda _cfg, src: src, Mock())
        entries = repo.commit.call_args.args[1]
        layout = json.loads(blobs[entries['site-layout.json']['sha']])
        self.assertEqual(['en'], list(layout['languages']))
        self.assertEqual('en', languages.release_language(layout, {'article_language': 'en'}, 'en'))
        self.assertEqual('logo', entries['en/assets/logo.png']['sha'])
        self.assertIsNone(entries['zh-hans/README.md']['sha'])

    def test_concurrent_save_rejection_does_not_change_git_refs(self):
        repo = Mock()
        repo.read.return_value = b'old file'
        book = {'pluginsConfig': {'edit-link': {'base': 'github'}, 'lanying-grow-ai': {}}}
        repo.json.side_effect = lambda path, *_args, **_opts: book if path == 'book.json' else None
        current = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        site = {'language': 'zh-hans', 'name': 'Docs'}
        before_publish = Mock(side_effect=ValueError('site revision changed'))
        with self.assertRaisesRegex(ValueError, 'site revision changed'):
            languages.sync(repo, 'master', '', 'base', current, site,
                           lambda _cfg, src: src, Mock(), before_publish=before_publish)
        repo.commit.assert_not_called()

    def test_closed_or_deleted_upgrade_no_longer_counts_as_open(self):
        repo = languages.Repository('https://example.invalid', {})
        with patch.object(repo, 'request', side_effect=[{'state': 'open'}, {'state': 'closed'}, None]) as request:
            url = 'https://github.com/example/repo/pull/5'
            self.assertTrue(repo.upgrade_is_open(url))
            self.assertFalse(repo.upgrade_is_open(url))
            self.assertFalse(repo.upgrade_is_open(url))
            self.assertFalse(repo.upgrade_is_open(''))
            request.assert_called_with('GET', 'pulls/5', missing_ok=True)

    def test_retry_reuses_an_open_upgrade_pr(self):
        repo = languages.Repository('https://api.github.test/repos/owner/site', {})
        head = 'seenical-language-upgrade-retry'
        url = 'https://github.test/owner/site/pull/5'
        with patch.object(repo, 'request', side_effect=[{'object': {'sha': 'commit'}}, [{'html_url': url}]]) as request:
            self.assertEqual(url, repo.commit('new-parent', {}, 'master', head))
            self.assertEqual(2, request.call_count)
            request.assert_called_with('GET', 'pulls', params={
                'state': 'open', 'head': f'owner:{head}', 'base': 'master',
            })

    def test_retry_creates_pr_from_existing_upgrade_branch(self):
        repo = languages.Repository('https://api.github.test/repos/owner/site', {})
        head = 'seenical-language-upgrade-retry'
        url = 'https://github.test/owner/site/pull/5'
        with patch.object(repo, 'request', side_effect=[{'object': {'sha': 'commit'}}, [], {'html_url': url}]) as request:
            self.assertEqual(url, repo.commit('parent', {}, 'master', head))
            self.assertEqual(3, request.call_count)
            self.assertEqual(('POST', 'pulls'), request.call_args.args)
            self.assertEqual(head, request.call_args.kwargs['json']['head'])
