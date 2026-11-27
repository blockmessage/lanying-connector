import hashlib
import json
import unittest
from unittest.mock import Mock, patch

import lanying_site_languages as languages


def set_repository_files(repo, files):
    repo.read.side_effect = lambda path, *args, **kwargs: files.get(path)
    repo.request.return_value = {'tree': [
        languages.entry(path, content) for path, content in files.items() if content is not None
    ]}


class SiteLanguagesTest(unittest.TestCase):
    def test_combined_layout_save_keeps_manifest_fields_and_batch_language(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        site = {'language': 'zh-hans', 'site_languages': settings, 'site_template': 'seenical'}
        layout = languages.layout_for('zh-hans', settings, 'zh-hans')
        manifest = {k: v for k, v in layout.items() if k != 'version'}
        manifest.update(template='seenical', repository_version=1, extra='keep')
        repo = Mock()
        books = {'book.json': {'plugins': ['seenical@1.0.1'], 'pluginsConfig': {'seenical': {}}}}
        repo.json.side_effect = lambda path, *_args, **_kwargs: books.get(path)
        set_repository_files(repo, {'en/README.md': b'# Home', 'zh-hans/README.md': b'# Home'})
        repo.blob.side_effect = lambda data: data
        with patch.object(languages, 'read_repository_manifest', return_value=manifest):
            current, path, _ = languages.read_site_layout(repo, 'head', site)
        new_site = {**site, 'site_languages': {**settings, 'default_language': 'en'}}
        languages.sync(repo, 'main', '', 'head', current, new_site, lambda s, b, c: b, None,
                       previous=languages.book_sync_state(site), manifest=manifest, layout_path=path)
        entries = repo.commit.call_args.args[1]
        self.assertNotIn('site-layout.json', entries)
        updated = json.loads(entries['seenical.json']['sha'])
        self.assertEqual('keep', updated['extra'])
        self.assertNotIn('version', updated)
        self.assertEqual('seenical', updated['template'])
        self.assertEqual(1, updated['repository_version'])
        with patch.object(languages, 'read_repository_manifest', return_value=updated):
            current, path, _ = languages.read_site_layout(repo, 'head', site)
        self.assertEqual('/', current['languages']['en']['public_path'])
        self.assertEqual('zh-hans', languages.release_language(current, {'article_language': 'zh-hans'}, 'en'))
        books['site-layout.json'] = layout
        with patch.object(languages, 'read_repository_manifest', return_value=updated):
            self.assertEqual((layout, 'site-layout.json', updated), languages.read_site_layout(repo, 'head', site))

    def test_patch_only_replaces_the_known_deployment_workflow(self):
        path = '.github/workflows/build.yml'
        old = b'push:\n      - master\npull_request:\n      - master\nold callback\n'
        new = old.replace(b'old callback', b'wait for deployment')
        repo, put = Mock(), Mock()
        with patch.object(languages, 'LEGACY_WORKFLOW_SHA256', hashlib.sha256(old).hexdigest()), \
                patch.object(languages, 'Repository') as official:
            official.return_value.read.return_value = new
            languages.patch_deployment_workflow(repo, old.replace(b'master', b'main'), 'main', put)
            official.return_value.read.assert_called_once_with(path, languages.WAIT_WORKFLOW_REF)
            put.assert_called_once_with(path, new.replace(b'master', b'main'))
            put.reset_mock()
            for content in (new, old + b'custom step', None):
                languages.patch_deployment_workflow(repo, content, 'main', put)
            put.assert_not_called()

    @patch.object(languages, 'LEGACY_LAYOUT_BUILDERS', {hashlib.sha256(b'old renderer\ndef main():\nold reader').hexdigest()})
    @patch.object(languages, 'TEMPLATE_MAKEFILE_SHA256', hashlib.sha256(b'make').hexdigest())
    def test_repository_patch_can_be_reverted_independently_of_settings(self):
        from test_grow_ai_patch import load_grow_ai
        transform = load_grow_ai().transform_site_to_book_json
        site = {'name': 'Docs', 'language': 'zh-hans', 'footer_note': 'New note', 'site_template': 'seenical'}
        old_site = {**site, 'footer_note': 'Old note'}
        layout = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        del layout['legacy_root_language']  # First-generation sites keep articles below /zh-hans/.
        layout['extra'] = {'keep': True}
        script_path = 'scripts/language_layout.py'
        files = {
            'book.json': json.dumps({'plugins': ['other@2', 'seenical@1.0.0'],
                                     'pluginsConfig': {'seenical': {'keep': 'yes'}, 'other': {'key': 2}}}).encode(),
            'zh-hans/book.json': b'{"title": "Original title"}',
            'zh-hans/README.md': b'# My home',
            'site-layout.json': json.dumps(layout).encode(),
            '.bookignore': b'/private',
            'Makefile': b'make',
            script_path: b'old renderer\ndef main():\nold reader',
        }
        repo = Mock()
        set_repository_files(repo, files)
        repo.json.side_effect = lambda path, *args, **kwargs: json.loads(files[path]) if path in files else None
        repo.blob.side_effect = lambda data: data
        def sync(previous):
            current, path, manifest = languages.read_site_layout(repo, 'head', site)
            with patch.object(languages, 'Repository') as template:
                template.return_value.read.return_value = b'new renderer\ndef main():\ncombined reader'
                languages.sync(repo, 'master', '', 'head', current, site,
                               lambda settings, book, changed: transform(settings, book, 'org', 'docs', 'master', changed),
                               None, manifest=manifest, layout_path=path, previous=languages.book_sync_state(previous))
            call = repo.commit.call_args
            settings = {k: entry['sha'] for k, entry in call.args[1].items()}
            upgrade = {k: entry['sha'] for k, entry in call.kwargs['patch_entries'].items()}
            return settings, upgrade

        original_local = files['zh-hans/book.json']
        files['zh-hans/book.json'] = b'{"plugins": ["-seenical"]}'
        self.assertEqual(({}, {}), sync(site), 'Language-specific plugin selections opt out of enrollment')
        files['zh-hans/book.json'] = original_local

        repo.commit.side_effect = ValueError('branch advanced')
        with self.assertRaisesRegex(ValueError, 'branch advanced'):
            sync(old_site)
        self.assertNotIn('seenical.json', files)
        repo.commit.side_effect = None
        settings, upgrade = sync(old_site)
        self.assertEqual({'book.json', 'zh-hans/book.json'}, set(settings))
        self.assertEqual({'.bookignore', 'seenical.json', 'book.json', 'site-layout.json', script_path}, set(upgrade))
        configured = json.loads(settings['book.json'])
        self.assertIn('seenical@1.0.0', configured['plugins'])
        root = json.loads(upgrade['book.json'])
        self.assertEqual(['other@2', 'seenical@1.0.1'], root['plugins'])
        self.assertEqual({'key': 2}, root['pluginsConfig']['other'])
        self.assertEqual('yes', root['pluginsConfig']['seenical']['keep'])
        self.assertEqual('New note', root['pluginsConfig']['seenical']['footer_note'])
        self.assertEqual(configured, {**root, 'plugins': configured['plugins']})
        self.assertEqual('Original title', json.loads(settings['zh-hans/book.json'])['title'])
        self.assertEqual(b'/private\n/seenical.json\n', upgrade['.bookignore'])
        expected_layout = {key: value for key, value in layout.items() if key != 'version'}
        self.assertEqual({'template': 'seenical', 'repository_version': 1, **expected_layout},
                         json.loads(upgrade['seenical.json']))
        self.assertIsNone(upgrade['site-layout.json'])
        self.assertEqual(b'old renderer\ndef main():\ncombined reader', upgrade[script_path])
        files.update(settings)
        before_patch = dict(files)
        files.update(upgrade)
        del files['site-layout.json']
        repo.blob.reset_mock()
        self.assertEqual(({}, {}), sync(site))
        repo.blob.assert_not_called()
        files.clear()
        files.update(before_patch)
        settings, reapplied = sync(site)
        self.assertEqual({}, settings)
        self.assertEqual(upgrade, reapplied)
        files.update(reapplied)
        del files['site-layout.json']
        self.assertEqual(({}, {}), sync(site))

        # A language change and the migration in the same push must also survive a patch revert.
        files.clear()
        files.update(before_patch)
        files['en/README.md'] = b'# Home'
        files['en/book.json'] = b'{}'
        files['seenical.json'] = b'{"template":"seenical","repository_version":1}'
        layout['languages']['en'] = {'source_dir': 'en', 'public_path': '/en/'}
        files['site-layout.json'] = json.dumps(layout).encode()
        site['site_languages'] = {'default_language': 'en', 'overrides': {'en': {}}}
        settings, upgrade = sync({**site, 'site_languages': {'default_language': 'zh-hans', 'overrides': {'en': {}}}})
        configured_layout = json.loads(settings['site-layout.json'])
        migrated = json.loads(upgrade['seenical.json'])
        self.assertEqual('en', migrated['default_language'])
        self.assertEqual('/', migrated['languages']['en']['public_path'])
        self.assertEqual({k: v for k, v in configured_layout.items() if k != 'version'},
                         {k: v for k, v in migrated.items() if k not in ('repository_version', 'template')})

    @patch.object(languages, 'COMBINED_LAYOUT_BUILDER', hashlib.sha256(b'combined reader').hexdigest())
    @patch.object(languages, 'TEMPLATE_MAKEFILE_SHA256', hashlib.sha256(b'make').hexdigest())
    def test_repository_patch_preserves_custom_and_already_updated_plugin_pins(self):
        repo = Mock()
        files = {'.bookignore': b'/seenical.json\n', 'scripts/language_layout.py': b'combined reader', 'Makefile': b'make'}
        set_repository_files(repo, files)
        layout = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        repo.blob.side_effect = lambda content: content
        for plugins in (['seenical@^1.0.0'], ['-seenical'], ['seenical@1.0.2'], ['seenical@file:custom'],
                        ['seenical@1.0.0', '-seenical'], ['lanying-grow-ai@1.0.0']):
            book = {'plugins': plugins}
            self.assertEqual({}, languages.repository_patch_v1(repo, 'head', book, {}, 'main', layout))
        repo.read.assert_not_called()
        repo.blob.assert_not_called()
        book = {'plugins': ['seenical@1.0.1']}
        changes = languages.repository_patch_v1(repo, 'head', book, {'extra': True}, 'main', layout)
        self.assertEqual({'seenical.json', 'site-layout.json'}, set(changes))
        self.assertTrue(json.loads(changes['seenical.json']['sha'])['extra'])
        for path in ('scripts/language_layout.py', 'Makefile', '.github/workflows/build.yml'):
            with self.subTest(path=path), patch.dict(files, {path: b'custom build using site-layout.json'}):
                self.assertEqual({}, languages.repository_patch_v1(repo, 'head', book, {}, 'main', layout))

    @patch.object(languages, 'COMBINED_LAYOUT_BUILDER', hashlib.sha256(b'combined reader').hexdigest())
    @patch.object(languages, 'TEMPLATE_MAKEFILE_SHA256', hashlib.sha256(b'make').hexdigest())
    def test_multilingual_growai_migrates_layout_once_and_keeps_all_content(self):
        site = {'language': 'zh-hans', 'name': 'Docs',
                'site_languages': {'default_language': 'zh-hans', 'overrides': {'en': {}}}}
        layout = languages.layout_for('zh-hans', site['site_languages'], 'zh-hans')
        layout['extra'] = {'keep': True}
        files = {
            'site-layout.json': languages.json_bytes(layout),
            'book.json': b'{"plugins":["lanying-grow-ai@^1.0.13"],"pluginsConfig":{"lanying-grow-ai":{}}}',
            'en/book.json': b'{"title":"Custom EN"}', 'zh-hans/book.json': b'{"title":"Custom ZH"}',
            'en/README.md': b'# Home', 'zh-hans/README.md': b'# Docs',
            'en/wiki/article.md': b'EN', 'zh-hans/wiki/article.md': b'ZH',
            '.bookignore': b'/site-layout.json\n/seenical.json\n',
            'scripts/language_layout.py': b'combined reader', 'Makefile': b'make',
            '.github/workflows/build.yml': b'standard build',
        }
        repo = Mock()
        set_repository_files(repo, files)
        repo.json.side_effect = lambda path, *a, **kw: json.loads(files[path]) if path in files else None
        repo.blob.side_effect = lambda data: data

        def save():
            current, path, manifest = languages.read_site_layout(repo, 'head', site)
            languages.sync(repo, 'master', '', 'head', current, site, lambda _s, book, _c: book, None,
                           manifest=manifest, layout_path=path, previous=languages.book_sync_state(site))
            self.assertEqual({}, repo.commit.call_args.args[1])
            return repo.commit.call_args.kwargs['patch_entries']

        for path, content in (('scripts/language_layout.py', b'legacy reader'),
                              ('seenical.json', b'customer file'),
                              ('.github/workflows/build.yml', b'--config site-layout.json')):
            with self.subTest(path=path), patch.dict(files, {path: content}):
                self.assertEqual({}, save())

        entries = save()
        self.assertEqual({'site-layout.json', 'seenical.json'}, set(entries))
        self.assertIsNone(entries['site-layout.json']['sha'])
        manifest = json.loads(entries['seenical.json']['sha'])
        self.assertEqual({**{k: v for k, v in layout.items() if k != 'version'},
                          'template': 'seenical', 'repository_version': 1}, manifest)
        del files['site-layout.json']
        files['seenical.json'] = entries['seenical.json']['sha']
        repo.blob.reset_mock()
        self.assertEqual({}, save())
        repo.blob.assert_not_called()
        migrated, path, _ = languages.read_site_layout(repo, 'head', site)
        self.assertEqual('seenical.json', path)
        self.assertEqual(layout['languages'], migrated['languages'])
        self.assertEqual('en', languages.release_language(migrated, {'article_language': 'en'}, 'zh-hans'))

    def test_repository_manifest_rejects_unknown_or_invalid_versions(self):
        repo = Mock()
        for raw in (b'not json', b'null', b'{}', b'{"repository_version": true}', b'{"repository_version": -1}',
                    b'{"template": "seenical", "repository_version": 2}', b'{"repository_version": 1}',
                    b'{"template": "other", "repository_version": 0}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                repo.read.return_value = raw
                languages.read_repository_manifest(repo, 'head')

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
                    self.assertEqual('github', plugins['edit-link']['base'])
                    if code == 'en':
                        self.assertEqual('EN', plugins[key]['custom'])
                    self.assertNotIn('seenical' if key == 'lanying-grow-ai' else 'lanying-grow-ai', plugins)

    def test_save_repairs_generated_edit_link_without_overwriting_custom_link(self):
        base = 'https://github.com/org/site/blob/master'
        root = {'pluginsConfig': {'seenical': {}, 'edit-link': {'base': base}}}
        books = {
            'book.json': root,
            'zh-hans/book.json': {'pluginsConfig': {'edit-link': {'base': base + '/zh-hans', 'label': '编辑'}}},
            'en/book.json': {'pluginsConfig': {'edit-link': {'base': 'https://github.com/org/other/blob/main'}}},
        }
        repo = Mock()
        repo.json.side_effect = lambda path, *_args, **_kwargs: books.get(path)
        repo.read.return_value = b'# Home'
        repo.blob.side_effect = lambda content: content
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        site = {'name': 'Docs', 'language': 'zh-hans', 'site_languages': settings}
        languages.sync(repo, 'master', '', 'head', languages.layout_for('zh-hans', settings), site,
                       lambda _site, book, _fields: book, None, previous=languages.book_sync_state(site))

        entries = repo.commit.call_args.args[1]
        self.assertEqual({'zh-hans/book.json'}, set(entries))
        update = json.loads(entries['zh-hans/book.json']['sha'])
        self.assertEqual({'base': base, 'label': '编辑'}, update['pluginsConfig']['edit-link'])

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
        set_repository_files(repo, {'zh-hans/README.md': b'# Home', 'en/README.md': b'# Home'})
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

    def test_switch_default_language_reassigns_both_paths_and_preserves_source(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        current = languages.layout_for('zh-hans', settings, 'zh-hans')
        config = {'plugins': ['versions-select@^0.1.1', 'seenical'], 'pluginsConfig': {
            'seenical': {}, 'versions': {'type': 'languages', 'options': []}}}
        repo = Mock()
        repo.json.side_effect = lambda path, *_args, **_kwargs: config if path == 'book.json' else None
        files = {'zh-hans/README.md': b'# Chinese', 'en/README.md': b'# English',
                 'zh-hans/assets/logo.png': b'shared logo', 'zh-hans/assets/favicon.ico': b'shared icon',
                 'en/assets/favicon.ico': b'custom English icon'}
        set_repository_files(repo, files)
        repo.blob.side_effect = lambda data: data

        for default, other in (('en', 'zh-hans'), ('zh-hans', 'en')):
            with self.subTest(default=default):
                site = {'language': 'zh-hans', 'name': 'Docs',
                        'site_languages': {**settings, 'default_language': default}}
                languages.sync(repo, 'master', '', 'head', current, site,
                               lambda _site, book, _fields: book, None)
                changes = repo.commit.call_args.args[1]
                current = json.loads(changes['site-layout.json']['sha'])
                self.assertEqual(default, current['default_language'])
                self.assertEqual(default, current['legacy_root_language'])
                self.assertEqual({'source_dir': default, 'public_path': '/'}, current['languages'][default])
                self.assertEqual({'source_dir': other, 'public_path': f'/{other}/'}, current['languages'][other])
                self.assertEqual('zh-hans', languages.release_language(current, {'article_language': 'zh-hans'}, 'en'))
                self.assertEqual('en', languages.release_language(current, {'article_language': 'en'}, 'zh-hans'))
                for code in ('zh-hans', 'en'):
                    self.assertNotIn(f'{code}/README.md', changes)
                    options = json.loads(changes[f'{code}/book.json']['sha'])['pluginsConfig']['versions']['options']
                    self.assertEqual(['/', f'/{other}/'], [option['value'] for option in options])
                if default == 'en':
                    self.assertEqual(b'shared logo', changes['en/assets/logo.png']['sha'])
                    self.assertNotIn('en/assets/favicon.ico', changes)

    def test_old_site_receives_local_assets_when_primary_language_is_promoted_first(self):
        before = {'default_language': 'en', 'overrides': {'en': {}}}
        current = languages.layout_for('zh-hans', before)
        site = {'language': 'zh-hans', 'name': 'Docs',
                'site_languages': {**before, 'default_language': 'zh-hans'}}
        stylesheet = 'assets/styles/website.css'
        repo = Mock()
        config = {'pluginsConfig': {'seenical': {}, 'logo': {'logo': '/assets/logo.png'}}}
        repo.json.side_effect = lambda path, *_args, **_kwargs: config if path == 'book.json' else None
        files = {'zh-hans/README.md': b'# Chinese', 'en/README.md': b'# English',
                 'assets/logo.png': b'logo', 'zh-hans/assets/logo.png': b'logo',
                 'assets/favicon.ico': b'old icon', 'zh-hans/assets/favicon.ico': b'old icon',
                 'en/assets/favicon.ico': b'English icon',
                 'assets/apple-touch-icon.png': b'shared custom icon',
                 stylesheet: b'body {}', f'zh-hans/{stylesheet}': b'body {}',
                 f'en/{stylesheet}': b'@import url("../../../assets/styles/website.css");\n',
                 'assets/fonts/custom.woff2': b'custom font'}
        set_repository_files(repo, files)
        repo.blob.side_effect = lambda data: data

        languages.sync(repo, 'master', '', 'head', current, site, lambda _site, book, _fields: book, None)
        changes = repo.commit.call_args.args[1]
        for asset in ('assets/logo.png', 'assets/favicon.ico', stylesheet):
            self.assertIsNone(changes[asset]['sha'])
            self.assertNotIn(f'zh-hans/{asset}', changes)
        self.assertEqual(b'logo', changes['en/assets/logo.png']['sha'])
        self.assertNotIn('en/assets/favicon.ico', changes)
        self.assertIsNone(changes['assets/apple-touch-icon.png']['sha'])
        self.assertEqual(b'shared custom icon', changes['en/assets/apple-touch-icon.png']['sha'])
        self.assertEqual(b'shared custom icon', changes['zh-hans/assets/apple-touch-icon.png']['sha'])
        self.assertEqual(b'body {}', changes[f'en/{stylesheet}']['sha'])
        self.assertIsNone(changes['assets/fonts/custom.woff2']['sha'])
        self.assertEqual(b'custom font', changes['en/assets/fonts/custom.woff2']['sha'])

    def test_materializes_existing_primary_css_import_without_replacing_custom_assets(self):
        before = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        layout = languages.layout_for('zh-hans', before, 'zh-hans')
        site = {'language': 'zh-hans', 'name': 'Docs', 'site_languages': {**before, 'default_language': 'en'}}
        repo = Mock()
        stylesheet = 'assets/styles/website.css'
        files = {'zh-hans/README.md': b'# Chinese', 'en/README.md': b'# English',
                 f'zh-hans/{stylesheet}': b'p {color:red}',
                 f'en/{stylesheet}': b'@import url("../../../zh-hans/assets/styles/website.css");\n',
                 'zh-hans/assets/fonts/body.woff2': b'source font',
                 'en/assets/logo.png': b'local logo'}
        set_repository_files(repo, files)
        repo.json.side_effect = lambda path, *_args, **_kwargs: {'pluginsConfig': {'seenical': {}}} if path == 'book.json' else None
        repo.blob.side_effect = lambda data: data
        languages.sync(repo, 'master', '', 'head', layout, site, lambda _site, book, _fields: book, None)
        changes = repo.commit.call_args.args[1]
        self.assertEqual(b'p {color:red}', changes[f'en/{stylesheet}']['sha'])
        self.assertEqual(b'source font', changes['en/assets/fonts/body.woff2']['sha'])
        self.assertNotIn('en/assets/logo.png', changes)

    def test_materialized_stylesheet_keeps_relative_media_outside_assets(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        current = languages.layout_for('zh-hans', settings)
        stylesheet = 'assets/styles/website.css'
        original_css = (b'body {background: url("../../shared-hero.svg")} '
                        b'@font-face {src:url(\'../../fonts/body.woff2?v=1\')} '
                        b'p {background: url("/static/banner.png")} '
                        b'/* url("../../unused.svg") */')
        repo = Mock()
        files = {
            'zh-hans/README.md': b'# Chinese',
            'en/README.md': b'# English',
            'zh-hans/' + stylesheet: b'body {color:blue}',
            'en/' + stylesheet: b'@import url("../../../assets/styles/website.css");\n',
            stylesheet: original_css,
            'shared-hero.svg': b'hero',
            'fonts/body.woff2': b'font',
            'en/fonts/body.woff2': b'custom English font',
            'static/banner.png': b'public banner',
            'unused.svg': b'unused image',
        }
        set_repository_files(repo, files)
        repo.json.side_effect = lambda path, *_args, **_kwargs: {'pluginsConfig': {'seenical': {}}} if path == 'book.json' else None
        repo.blob.side_effect = lambda data: data

        site = {'language': 'zh-hans', 'name': 'Docs', 'site_languages': {**settings, 'default_language': 'en'}}
        languages.sync(repo, 'master', '', 'head', current, site, lambda _, config, changed: config, None)
        changes = repo.commit.call_args.args[1]
        self.assertEqual(original_css, changes[f'en/{stylesheet}']['sha'])
        self.assertNotIn(f'zh-hans/{stylesheet}', changes)
        self.assertEqual(b'hero', changes['en/shared-hero.svg']['sha'])
        self.assertEqual(b'hero', changes['zh-hans/shared-hero.svg']['sha'])
        self.assertIsNone(changes['shared-hero.svg']['sha'])
        self.assertEqual(b'font', changes['zh-hans/fonts/body.woff2']['sha'])
        self.assertIsNone(changes['fonts/body.woff2']['sha'])
        self.assertNotIn('en/fonts/body.woff2', changes)
        self.assertNotIn('static/banner.png', changes)
        self.assertNotIn('unused.svg', changes)

        # A prefixed site can have localized the CSS while retaining its public root assets.
        files[f'en/{stylesheet}'] = original_css
        files['en/shared-hero.svg'] = files['zh-hans/shared-hero.svg'] = b'hero'
        files['zh-hans/fonts/body.woff2'] = b'font'
        set_repository_files(repo, files)
        languages.sync(repo, 'master', '', 'head', current, site, lambda _, config, changed: config, None)
        later_changes = repo.commit.call_args.args[1]
        self.assertIsNone(later_changes['shared-hero.svg']['sha'])
        self.assertIsNone(later_changes['fonts/body.woff2']['sha'])
        self.assertNotIn('en/shared-hero.svg', later_changes)
        self.assertNotIn(f'en/{stylesheet}', later_changes)

    def test_switch_default_language_rejects_distinct_root_asset_without_writing(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        current = languages.layout_for('zh-hans', settings)
        site = {'language': 'zh-hans', 'name': 'Docs',
                'site_languages': {**settings, 'default_language': 'en'}}
        repo = Mock()
        files = {'assets/logo.png': b'custom root', 'zh-hans/assets/logo.png': b'Chinese logo',
                 'en/assets/logo.png': b'English logo'}
        set_repository_files(repo, files)

        with self.assertRaisesRegex(ValueError, 'root asset assets/logo.png conflicts with en/assets/logo.png'):
            languages.sync(repo, 'master', '', 'head', current, site, Mock(), None)
        repo.blob.assert_not_called()
        repo.commit.assert_not_called()

    def test_switch_default_language_rejects_occupied_path_before_writing(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        current = languages.layout_for('zh-hans', settings, 'zh-hans')
        site = {'language': 'zh-hans', 'name': 'Docs',
                'site_languages': {**settings, 'default_language': 'en'}}
        repo = Mock()
        repo.read.side_effect = lambda path, *_args, **_kwargs: b'occupied' if path == 'en/zh-hans' else None
        with self.assertRaisesRegex(ValueError, 'en/zh-hans'):
            languages.sync(repo, 'master', '', 'head', current, site, Mock(), None)
        repo.blob.assert_not_called()
        repo.commit.assert_not_called()

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

    def test_second_language_copies_complete_assets(self):
        for primary, added in (('zh-hans', 'en'), ('en', 'zh-hans')):
            with self.subTest(primary=primary):
                settings = {'default_language': primary, 'overrides': {added: {}}}
                site = {'language': primary, 'site_languages': settings, 'name': 'Docs'}
                current = languages.layout_for(primary, {'default_language': primary, 'overrides': {}}, primary)
                files = {f'{primary}/README.md': b'# Home',
                         f'{primary}/assets/styles/website.css': b'body {}',
                         f'{primary}/assets/logo.png': b'logo', f'{primary}/assets/favicon.ico': b'icon',
                         f'{primary}/assets/apple-touch-icon.png': b'touch icon',
                         f'{primary}/assets/fonts/body.woff2': b'font'}
                repo = Mock()
                set_repository_files(repo, files)
                repo.json.side_effect = lambda p, *_args, **_opts: {'pluginsConfig': {'lanying-grow-ai': {}}} if p == 'book.json' else None
                repo.blob.side_effect = lambda data: data
                languages.sync(repo, 'master', '', 'parent', current, site, lambda _, config, changed: config, None)
                changes = repo.commit.call_args.args[1]
                for source, content in files.items():
                    if '/assets/' in source:
                        self.assertEqual(content, changes[added + source.removeprefix(primary)]['sha'])
                self.assertFalse(any(path.startswith('assets/') for path in changes))

    def test_new_english_site_replaces_only_template_seed(self):
        for root in ("", "zh-hans"):
            with self.subTest(root=root):
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
                current = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}}, root)
                site = {'language': 'en', 'name': 'English docs', 'site_language_seed_pending': 'on'}
                languages.sync(repo, 'master', '', 'seed', current, site, lambda _cfg, src, changed: src, Mock())
                entries = repo.commit.call_args.args[1]
                layout = json.loads(blobs[entries['site-layout.json']['sha']])
                self.assertEqual(['en'], list(layout['languages']))
                self.assertEqual('/' if root else '/en/', layout['languages']['en']['public_path'])
                self.assertEqual('en', languages.release_language(layout, {'article_language': 'en'}, 'en'))
                self.assertEqual('logo', entries['en/assets/logo.png']['sha'])
                self.assertIsNone(entries['zh-hans/README.md']['sha'])

    @patch.object(languages, 'Repository')
    def test_seenical_adds_localized_template_homepages(self, template):
        for primary, added, seed in (
                ('zh-hans', 'en', 'scripts/home/en.md'), ('en', 'zh-hans', 'zh-hans/README.md')):
            with self.subTest(added=added):
                template.reset_mock()
                homepage = '# Localized heading\n\n## Quick start\n'.encode()
                template.return_value.read.return_value = homepage
                repo = Mock()
                set_repository_files(repo, {f'{primary}/README.md': b'# Custom home'})
                book = {'pluginsConfig': {'edit-link': {'base': 'github'}, 'seenical': {}}}
                repo.json.side_effect = lambda path, *_args, **_opts: book if path == 'book.json' else None
                repo.blob.side_effect = lambda data: data
                current = languages.layout_for(primary, {'default_language': primary, 'overrides': {}})
                site = {'language': primary, 'name': 'Docs',
                        'site_languages': {'default_language': primary, 'overrides': {added: {}}}}
                languages.sync(repo, 'master', '', 'parent', current, site,
                               lambda _site, config, _changed: config, None)
                template.return_value.read.assert_called_once_with(seed, 'master')
                entries = repo.commit.call_args.args[1]
                self.assertEqual(homepage, entries[f'{added}/README.md']['sha'])
                self.assertNotIn(f'{primary}/README.md', entries)
                expected_label = 'Home' if added == 'en' else '首页'
                self.assertEqual(f'# Summary\n\n* [{expected_label}](README.md)\n'.encode(),
                                 entries[f'{added}/SUMMARY.md']['sha'])

    def test_seenical_sync_corrects_default_labels_and_preserves_custom_navigation(self):
        settings = {'default_language': 'zh-hans', 'overrides': {'en': {}}}
        site = {'language': 'zh-hans', 'name': 'Docs', 'site_languages': settings}
        layout = languages.layout_for('zh-hans', settings, 'zh-hans')
        tail = b'\r\n* [Documentation](articles/README.md)\r\n'
        files = {'en/README.md': b'# Home', 'zh-hans/README.md': b'# Home',
                 'en/SUMMARY.md': b'# Summary\r\n\r\n* [Documentation](README.md)' + tail,
                 'zh-hans/SUMMARY.md': '# Summary\n\n* [产品文档](README.md)'.encode()}
        books = {'book.json': {'pluginsConfig': {'seenical': {}, 'edit-link': {'label': '在GitHub上编辑'}}}}
        repo = Mock()
        set_repository_files(repo, files)
        repo.json.side_effect = lambda path, *_args, **_opts: books.get(path)
        repo.blob.side_effect = lambda content: content

        def sync():
            languages.sync(repo, 'master', '', 'parent', layout, site, lambda _s, book, _c: book, None,
                           previous=languages.book_sync_state(site))
            return {k: v['sha'] for k, v in repo.commit.call_args.args[1].items()}

        edits = sync()
        self.assertEqual(b'# Summary\r\n\r\n* [Home](README.md)' + tail, edits['en/SUMMARY.md'])
        self.assertEqual('# Summary\n\n* [首页](README.md)'.encode(), edits['zh-hans/SUMMARY.md'])
        files.update(edits)
        books['en/book.json'] = json.loads(edits['en/book.json'])
        edit_link = books['en/book.json']['pluginsConfig']['edit-link']
        self.assertEqual('Edit on GitHub', edit_link['label'])
        self.assertEqual({}, sync())

        files['en/SUMMARY.md'] = b'# Summary\n\n* [Start here](README.md)' + tail
        edit_link['label'] = 'Suggest an edit'
        self.assertEqual({}, sync())
        del edit_link['label']
        books['book.json']['pluginsConfig']['edit-link']['label'] = 'Edit with our team'
        self.assertEqual({}, sync())

    def test_seenical_footer_marks_only_site_owned_links(self):
        from test_grow_ai_patch import load_grow_ai
        transform = load_grow_ai().transform_site_to_book_json
        for key in ('seenical', 'lanying-grow-ai'):
            for official in ('', 'https://company.example/en?ref=docs#position'):
                with self.subTest(plugin=key, official=official):
                    book = {'pluginsConfig': {key: {}, 'tbfed-pagefooter': {}}}
                    site = {'language': 'en', 'copyright': 'Example', 'official_website_url': official}
                    plugins = transform(site, book, 'org', 'site', 'master',
                                        {'copyright'})['pluginsConfig']
                    footer = plugins['tbfed-pagefooter']['copyright']
                    self.assertIn(f"href='{official or '/'}'", footer)
                    self.assertIn("href='/sitemap.xml'", footer)
                    self.assertIn('>Home<' if key == 'seenical' and not official else '>Official Website<', footer)
                    self.assertEqual(key == 'seenical', 'data-seenical-site-path="/sitemap.xml"' in footer)
                    self.assertEqual(key == 'seenical' and not official, 'data-seenical-site-path="/"' in footer)

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

    def test_patch_commit_uses_settings_as_parent_and_updates_ref_once(self):
        repo = languages.Repository('https://example.invalid', {})
        settings = {'book.json': languages.entry('book.json', 'user-book')}
        upgrade = {'book.json': languages.entry('book.json', 'upgraded-user-book')}
        for first in (settings, {}):
            with self.subTest(settings=bool(first)):
                replies = [{'tree': {'sha': 'base-tree'}}]
                if first:
                    replies += [{'sha': 'settings-tree'}, {'sha': 'settings-head'}]
                replies += [{'sha': 'patch-tree'}, {'sha': 'patch-head'}, {}]
                with patch.object(repo, 'request', side_effect=replies) as request:
                    repo.commit('base-head', first, 'main', '', patch_entries=upgrade)
                calls = request.call_args_list
                self.assertEqual(['POST', 'git/trees'], list(calls[-3].args))
                self.assertEqual({'base_tree': 'settings-tree' if first else 'base-tree',
                                  'tree': list(upgrade.values())}, calls[-3].kwargs['json'])
                self.assertEqual(['settings-head' if first else 'base-head'], calls[-2].kwargs['json']['parents'])
                self.assertEqual('Upgrade Seenical site template to v1', calls[-2].kwargs['json']['message'])
                refs = [call for call in calls if 'git/refs/' in call.args[1]]
                self.assertEqual([calls[-1]], refs)
                request.assert_called_with('PATCH', 'git/refs/heads/main', json={'sha': 'patch-head', 'force': False})

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
