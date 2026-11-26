import json
import unittest
from unittest.mock import Mock, patch

import lanying_site_languages as languages


class SiteLanguagesTest(unittest.TestCase):
    def test_saves_use_the_repository_plugin_without_renaming_legacy_keys(self):
        for key in ('seenical', 'lanying-grow-ai'):
            with self.subTest(key=key):
                book = {'plugins': [key + '@^1.0.0'], 'pluginsConfig': {
                    'edit-link': {'base': 'github'}, key: {'sitemap_hostname': 'https://customer.example'}}}
                repo = Mock()
                books = {'book.json': book, 'en/book.json': {'pluginsConfig': {key: {'custom': 'EN'}}}}
                repo.json.side_effect = lambda path, *_args, **_opts: books.get(path)
                repo.read.return_value = b'# Home'
                repo.blob.side_effect = lambda data: data
                settings = {'default_language': 'en', 'overrides': {'en': {'footer_note': 'English'}}}
                layout = languages.layout_for('zh-hans', settings)
                site = {'name': 'Docs', 'language': 'zh-hans', 'site_languages': settings, 'footer_note': '中文'}
                languages.sync(repo, 'main', '', 'head', layout, site, lambda _, config, changed: config, None)
                entries = repo.commit.call_args.args[1]
                for code, footer in [('en', 'English'), ('zh-hans', '中文')]:
                    plugins = json.loads(entries[code + '/book.json']['sha'])['pluginsConfig']
                    self.assertEqual(footer, plugins[key]['footer_note'])
                    if code == 'en':
                        self.assertEqual('EN', plugins[key]['custom'])
                    self.assertNotIn('seenical' if key == 'lanying-grow-ai' else 'lanying-grow-ai', plugins)

    def test_title_and_keywords_preserve_per_language_repository_overrides(self):
        from test_grow_ai_patch import load_grow_ai
        transform = load_grow_ai().transform_site_to_book_json
        key = 'lanying-grow-ai'
        book = {'plugins': [key], 'pluginsConfig': {key: {'footer_note': 'handwritten'}}}
        local = {'title': 'Old', 'pluginsConfig': {key: {'footer_note': 'local'},
                 'logo': {'logo': 'custom.png'}, 'meta': {'data': [{'name': 'robots', 'content': 'noindex'}]}}}
        before = {'name': 'Site', 'language': 'zh-hans', 'site_languages': {
            'default_language': 'zh-hans', 'overrides': {'en': {'title': 'Old'}}}}
        site = {**before, 'site_languages': {'default_language': 'en',
                'overrides': {'en': {'title': 'Updated', 'meta_keywords': 'docs'}}}}
        repo = Mock()
        repo.json.side_effect = lambda name, *_a, **_kw: {'book.json': book, 'en/book.json': local}.get(name)
        repo.read.return_value = b'# Home'
        repo.blob.side_effect = lambda data: data
        languages.sync(repo, 'master', '', 'head', languages.layout_for('zh-hans', before['site_languages']), site,
                       lambda s, b, changes: transform(s, b, 'org', 'site', 'master', changes), None,
                       previous=languages.book_sync_state(before))
        entries = repo.commit.call_args.args[1]
        self.assertEqual({'en/book.json', 'site-layout.json'}, set(entries))
        updated = json.loads(entries['en/book.json']['sha'])
        self.assertEqual('Updated', updated['title'])
        self.assertEqual(local['pluginsConfig']['logo'], updated['pluginsConfig']['logo'])
        self.assertEqual(local['pluginsConfig'][key], updated['pluginsConfig'][key])
        self.assertEqual([{'name': 'robots', 'content': 'noindex'}, {'name': 'keywords', 'content': 'docs'}],
                         updated['pluginsConfig']['meta']['data'])

    def test_admin_site_name_does_not_replace_localized_titles(self):
        key = 'lanying-grow-ai'
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {'title': ''}}}
        old = {'name': 'Old name', 'language': 'zh-hans', 'site_languages': settings, 'title': ''}
        site = {**old, 'name': 'New name'}
        repo = Mock()
        books = {'book.json': {'pluginsConfig': {key: {}}},
                 'zh-hans/book.json': {'title': '中文标题'}, 'en/book.json': {'title': 'English title'}}
        repo.json.side_effect = lambda path, *_args, **_kwargs: books.get(path)
        repo.read.return_value = b'# Home'
        repo.blob.side_effect = lambda data: data
        languages.sync(repo, 'master', '', 'head', languages.layout_for('zh-hans', settings), site,
                       lambda _settings, book, _fields: book, None, previous=languages.book_sync_state(old))
        self.assertEqual({}, repo.commit.call_args.args[1])

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

    def test_secondary_stylesheet_imports_shared_css_without_replacing_local_css(self):
        stylesheet = 'assets/styles/website.css'
        for primary, added, legacy, existing_css in [
                ('zh-hans', 'en', '', None), ('en', 'zh-hans', 'en', None),
                ('zh-hans', 'en', '', b'custom')]:
            with self.subTest(primary=primary, legacy=legacy, existing_css=existing_css):
                settings = {'default_language': added, 'overrides': {added: {}}}
                site = {'language': primary, 'site_languages': settings, 'name': 'Docs'}
                current = languages.layout_for(primary, settings, legacy)
                shared = f'{legacy}/{stylesheet}' if legacy else stylesheet
                local = f'{added}/{stylesheet}'
                book = {'styles': {'website': stylesheet}, 'pluginsConfig': {
                    'edit-link': {'base': 'github'}, 'lanying-grow-ai': {}}}
                repo = Mock()
                files = {shared: b'body {}', local: existing_css,
                         f'{added}/README.md': b'# Home', f'{primary}/README.md': b'# Home'}
                repo.read.side_effect = lambda p, *_args, **_opts: files.get(p)
                repo.json.side_effect = lambda p, *_args, **_opts: book if p == 'book.json' else None
                repo.blob.side_effect = lambda data: data
                languages.sync(repo, 'master', '', 'parent', current, site, lambda _, config, changed: config, None)
                entries = repo.commit.call_args.args[1]
                if existing_css is None:
                    self.assertEqual(f'@import url("../../../{shared}");\n'.encode(), entries[local]['sha'])
                else:
                    self.assertNotIn(local, entries)

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
        languages.sync(repo, 'master', '', 'seed', current, site, lambda _cfg, src, changed: src, Mock())
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
                           lambda _cfg, src, changed: src, Mock(), before_publish=before_publish)
        repo.commit.assert_not_called()

    def test_repository_ref_wait_is_bounded_and_only_retries_ref_reads(self):
        repo = languages.Repository('https://api.github.test/repos/org/site', {})
        cases = [
            ('GET', 'git/ref/heads/master', 409, True, 4),
            ('GET', 'git/ref/heads/master', 409, False, 1),
            ('GET', 'git/ref/heads/master', 403, True, 1),
            ('PATCH', 'git/refs/heads/master', 409, True, 1),
        ]
        for method, path, status, wait_for_ref, attempts in cases:
            with self.subTest(method=method, status=status, wait_for_ref=wait_for_ref), \
                    patch.object(languages.requests, 'request', return_value=Mock(status_code=status, ok=False)) as request, \
                    patch.object(languages.time, 'sleep') as sleep:
                with self.assertRaisesRegex(ValueError, f'HTTP {status}'):
                    repo.request(method, path, wait_for_ref=wait_for_ref)
                self.assertEqual(attempts, request.call_count)
                self.assertEqual(attempts - 1, sleep.call_count)

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

    def test_upgrade_digest_covers_tracking_and_localized_settings(self):
        saved = {'language': 'zh-hans', 'site_languages': {'default_language': 'en'},
                 'baidu_token': 'old', 'google_token': 'old'}
        digest = languages.upgrade_digest(saved)
        for field in ('baidu_token', 'google_token', 'site_languages'):
            with self.subTest(field=field):
                self.assertNotEqual(digest, languages.upgrade_digest({**saved, field: 'changed'}))
        self.assertEqual(digest, languages.upgrade_digest({**saved, 'agent_tools_revision': 2}))

    def test_pending_upgrade_can_be_found_before_receipt_is_saved(self):
        repo = languages.Repository('https://api.github.test/repos/owner/site', {})
        site = {'language_upgrade_head': 'pending-upgrade'}
        with patch.object(repo, 'request', return_value=[{'html_url': 'https://github.test/owner/site/pull/1'}]) as request:
            self.assertTrue(repo.has_open_upgrade(site, 'master'))
            request.assert_called_once_with('GET', 'pulls', params={
                'head': 'owner:pending-upgrade', 'base': 'master', 'state': 'open'})
        with patch.object(repo, 'request', return_value=[]):
            self.assertFalse(repo.has_open_upgrade(site, 'master'))
