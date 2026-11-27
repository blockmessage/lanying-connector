import base64
import importlib.util
import json
import pathlib
import sys
import tempfile
import types
import unittest
from contextlib import nullcontext
from unittest import mock


class FakePipeline:
    def __init__(self, revision):
        self.revision = revision
        self.hmset_calls = []

    def watch(self, key):
        return None

    def hget(self, key, field):
        return str(self.revision).encode()

    def unwatch(self):
        return None

    def multi(self):
        return None

    def setnx(self, key, value):
        return None

    def rpush(self, key, value):
        return None

    def hmset(self, key, values):
        self.hmset_calls.append((key, dict(values)))

    def execute(self):
        return []


class FakeRedis:
    def __init__(self, revision):
        self.pipeline_value = FakePipeline(revision)
        self.hmset_calls = []

    def pipeline(self, transaction=True):
        return self.pipeline_value

    def hmset(self, key, values):
        self.hmset_calls.append((key, dict(values)))


def load_grow_ai():
    module_name = "lanying_grow_ai_patch_test"
    sys.modules.pop(module_name, None)
    empty = types.SimpleNamespace()
    stubs = {
        # These tests isolate patching/material logic; deletion lifecycle has its own suite.
        "lanying_seenical_deletion": types.SimpleNamespace(
            guarded=lambda kind: lambda fn: fn, Cancelled=type('Cancelled', (Exception,), {}),
            check_run=lambda *args: None, deleting_task_ids=lambda app: set()),
        "lanying_redis": types.SimpleNamespace(get_redis_connection=lambda: None),
        "lanying_chatbot": empty,
        "lanying_config": empty,
        "requests": empty,
        "lanying_utils": empty,
        "lanying_file_storage": empty,
        "lanying_im_api": empty,
        "lanying_image": empty,
        "lanying_async": types.SimpleNamespace(executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None)),
        "lanying_schedule": empty,
        "yaml": empty,
        "lanying_cdn": empty,
        "lanying_cert": empty,
        "lanying_slack": empty,
        "lanying_google_analytics": empty,
        "lanying_baidu": empty,
        "lanying_google": empty,
        "lanying_oss": empty,
        "lanying_agent_tools_storage": types.SimpleNamespace(
            is_enabled=lambda: True,
            should_save_config_revision=lambda app_id, chatbot_id="": True,
            save_seenical_config_revision=lambda *args, **kwargs: {"result": "ok"},
            get_seenical_config_revision=lambda *args, **kwargs: None,
            list_seenical_config_revisions=lambda *args, **kwargs: []),
        "github": types.SimpleNamespace(Github=object),
        "dateutil": types.SimpleNamespace(),
        "dateutil.relativedelta": types.SimpleNamespace(relativedelta=object),
    }
    old_modules = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        path = pathlib.Path(__file__).resolve().parents[1] / "lanying_grow_ai.py"
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, old_module in old_modules.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module


def task(revision=4, schedule="off"):
    return {
        "app_id": "app", "task_id": "task", "name": "Plan", "note": "Topic",
        "chatbot_id": "bot", "prompt": "Original", "article_prompt": "Old article prompt",
        "article_language": "zh-hans", "article_language_scoped": "on", "keywords": "docs",
        "word_count_min": 800, "word_count_max": 1200, "image_count": 0,
        "article_count": 2, "cycle_type": "cycle", "cycle_interval": 86400,
        "file_list": [], "deploy": {"type": "none"}, "title_reuse": "on",
        "site_id_list": [], "target_dir": "/articles", "commit_type": "branch",
        "target_summary_dir": "", "embedding_condition": {}, "auto_deploy": "off",
        "schedule": schedule, "status": "normal", "revision": revision,
    }


class GrowAIPatchTest(unittest.TestCase):
    def setUp(self):
        self.module = load_grow_ai()

    def test_deployment_receipt_is_optional_and_dispatches_only_once(self):
        grow = self.module
        response = mock.Mock()
        response.json.return_value = {'workflow_run_id': 731}
        with mock.patch.object(grow, 'get_github_site', return_value='fixture'), \
                mock.patch.object(grow, 'get_site', return_value={}), \
                mock.patch.object(grow, 'set_deploy_code'), \
                mock.patch.object(grow.lanying_utils, 'get_internet_connector_server', return_value='https://example.test', create=True), \
                mock.patch.object(grow.requests, 'post', return_value=response, create=True) as dispatch:
            for requested, http_status, expected in (
                (False, 204, {'success': True}), (True, 200, {'success': True, 'workflow_run_id': 731}),
                (True, 204, {'success': True}), (True, 500, None),
            ):
                with self.subTest(requested=requested, http_status=http_status):
                    dispatch.reset_mock()
                    response.status_code = http_status
                    result = grow.start_deploy_github_action('app', '', 'site', 'owner', 'docs', 'tag',
                                                             return_run_details=requested)
                    dispatch.assert_called_once()
                    body = dispatch.call_args.kwargs['json']
                    self.assertEqual(body.get('return_run_details', False), requested)
                    self.assertEqual(set(body['inputs']), {'book_url', 'oss_path', 'cdn_url', 'check_url', 'callback_url'})
                    self.assertEqual(result['result'], 'error' if expected is None else 'ok')
                    self.assertEqual(result.get('data'), expected)

    def test_hosted_template_selection_only_applies_to_new_repositories(self):
        grow = self.module
        github = mock.Mock()
        for brand, template in [(None, 'growai-gitbook'), ('seenical', 'seenical-gitbook')]:
            with self.subTest(brand=brand):
                site = {'site_id': 7, 'github_hosting': 'on'}
                if brand:
                    site['site_template'] = brand
                with mock.patch.object(grow, 'get_github', return_value=github), \
                        mock.patch.object(grow, 'get_github_org', return_value='org'), \
                        mock.patch.object(grow, 'update_site_field'), \
                        mock.patch.object(grow, 'get_site', return_value=site), \
                        mock.patch.object(grow.lanying_slack, 'async_send_grafana_message', create=True):
                    grow.maybe_init_github_site_repo('app', site, site)
                    github.get_repo.assert_called_once_with('maxim-top/' + template)
                    github.reset_mock()
                    site['github_hosting_url'] = 'https://github.com/org/existing'
                    self.assertIs(site, grow.maybe_init_github_site_repo('app', site, site))
                    github.get_repo.assert_not_called()

    def test_failed_template_retries_the_saved_site_in_background(self):
        grow = self.module
        settings = mock.Mock(app_id='app', github_hosting='on')
        settings.name = 'Docs'
        settings.to_hmset_fields.return_value = {'name': 'Docs'}
        site = {'app_id': 'app', 'site_id': 7, 'name': 'Docs', 'github_hosting': 'on',
                'site_template': 'seenical', 'template_initialization': 'failed'}
        redis = mock.MagicMock()
        redis.get.return_value = None
        redis.transaction.side_effect = lambda reserve, *_args, **_kwargs: reserve(redis)
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.hmset.side_effect = lambda _key, values: site.update(values)
        redis.hmset.side_effect = lambda _key, values: site.update(values)
        pending = {'result': 'ok', 'data': {'site_id': 7, 'status': 'initializing'}}
        with mock.patch.object(grow, 'get_site', return_value=site), \
                mock.patch.object(grow, 'get_site_list', return_value={'data': {'list': [site]}}), \
                mock.patch.object(grow, 'check_site_num_limit') as quota, \
                mock.patch.object(grow, 'generate_site_id') as allocate, \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'initialize_new_site', side_effect=[site, {**site, 'github_hosting_url': 'url'}]) as init, \
                mock.patch.object(grow, 'sync_to_github', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'executor') as executor:
            self.assertEqual(pending, grow.create_site(settings, 'seenical'))
            self.assertEqual(pending, grow.retry_site_initialization('app', 7))
            executor.submit.assert_called_once_with(grow.initialize_seenical_site, 'app', 7)
            init.assert_not_called()

            initialize, *args = executor.submit.call_args.args
            failed = initialize(*args)
            self.assertEqual('error', failed['result'])
            self.assertIn('Site 7', failed['message'])
            self.assertEqual('failed', site['template_initialization'])

            self.assertEqual(pending, grow.retry_site_initialization('app', 7))
            self.assertEqual(2, executor.submit.call_count)
            ready = initialize(*args)
            self.assertEqual({'site_id': 7, 'status': 'ready'}, ready['data'])
            redis.hdel.assert_called_once_with(grow.get_site_key('app', 7), 'template_initialization', 'site_initialization_until')
            quota.assert_not_called()
            allocate.assert_not_called()

    def test_seenical_create_returns_before_initialization_and_reuses_site(self):
        grow = self.module
        for language in ('zh-hans', 'en'):
            with self.subTest(language=language):
                settings = mock.Mock(app_id='app', github_hosting='on', language=language)
                settings.name = 'Docs'
                settings.to_hmset_fields.return_value = {'name': 'Docs', 'language': language}
                redis = mock.MagicMock()
                values = {}
                redis.get.side_effect = values.get
                redis.set.side_effect = lambda key, value, **_kwargs: values.update({key: value})
                redis.hexists.return_value = True
                redis.transaction.side_effect = lambda reserve, *_args, **_kwargs: reserve(redis)
                site = {
                    'site_template': 'seenical', 'github_hosting': 'on',
                    'template_initialization': 'pending',
                    'site_initialization_until': int(grow.time.time()) + 1200,
                }
                pending = {'result': 'ok', 'data': {'site_id': 7, 'status': 'initializing'}}
                ready = {'result': 'ok', 'data': {'site_id': 7, 'status': 'ready'}}
                with mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                        mock.patch.object(grow, 'get_site_list', return_value={'data': {'list': []}}), \
                        mock.patch.object(grow, 'get_site', return_value=site), \
                        mock.patch.object(grow, 'check_site_setting', return_value={'result': 'ok'}), \
                        mock.patch.object(grow, 'check_site_num_limit', return_value={'result': 'ok'}), \
                        mock.patch.object(grow, 'generate_site_id', return_value=7) as allocate, \
                        mock.patch.object(grow, 'initialize_seenical_site') as initialize, \
                        mock.patch.object(grow, 'executor') as executor:
                    self.assertEqual(pending, grow.create_site(settings, 'seenical'))
                    self.assertEqual(pending, grow.create_site(settings, 'seenical'))
                    site.pop('template_initialization')
                    site.pop('site_initialization_until')
                    site['github_hosting_url'] = 'https://example.test'
                    self.assertEqual(ready, grow.create_site(settings, 'seenical'))
                allocate.assert_called_once()
                initialize.assert_not_called()
                executor.submit.assert_called_once_with(initialize, 'app', 7)
                redis.rpush.assert_called_once_with(grow.get_site_list_key('app'), 7)

    def test_navigation_readmes_preserve_existing_files_at_the_base_commit(self):
        grow = self.module
        pages = [('docs/wiki/README.md', 'Wiki'), ('docs/wiki/latest/README.md', 'Latest'),
                 ('docs/wiki/20261127/README.md', 'Archive')]
        repo = mock.Mock()
        repo.request.side_effect = [{'type': 'file'}, None, {'type': 'file'}]
        repo.blob.return_value = 'new-page'
        entries = grow.missing_navigation_readmes(repo, 'base', pages)
        self.assertEqual([grow.site_languages.entry(pages[1][0], 'new-page')], entries)
        repo.blob.assert_called_once_with(b'# Latest')
        self.assertEqual([mock.call('GET', 'contents/' + path, params={'ref': 'base'}, missing_ok=True)
                          for path, _ in pages], repo.request.call_args_list)

    def test_navigation_readme_lookup_failure_never_creates_a_replacement(self):
        repo = mock.Mock()
        repo.request.side_effect = ValueError('HTTP 403')
        with self.assertRaisesRegex(ValueError, '403'):
            self.module.missing_navigation_readmes(repo, 'base', [('wiki/README.md', 'Wiki')])
        repo.blob.assert_not_called()

    def test_publish_rejects_reserved_language_directories_before_github_writes(self):
        grow = self.module
        base_repo = 'https://github.com/org/site'
        site = {'site_id': 57, 'language': 'zh-hans', 'max_latest_num': 5,
                'github_hosting': 'on', 'github_url': base_repo, 'github_hosting_url': base_repo}
        layout = grow.site_languages.layout_for(
            'zh-hans', {'default_language': 'en', 'overrides': {'en': {}}}, 'zh-hans')
        run = {'status': 'success', 'task_id': 'task', 'zip_file': 'result.zip',
               'article_language': 'zh-hans'}
        redis = mock.Mock()
        redis.lock.side_effect = lambda *_args, **_kwargs: nullcontext()
        repo = mock.Mock()
        repo.json.return_value = layout
        github = mock.Mock()
        github.get.return_value = mock.Mock(status_code=200)
        github.get.return_value.json.return_value = {'object': {'sha': 'base'}}

        for field, folder, preview in (
                ('target_dir', 'en', None), ('target_summary_dir', 'zh-hans', 'preview')):
            with self.subTest(field=field), \
                    mock.patch.object(grow, 'get_task', return_value={**task(), field: f'/{folder}/articles'}), \
                    mock.patch.object(grow, 'get_task_run', return_value=run), \
                    mock.patch.object(grow, 'get_task_site_list', return_value=[site]), \
                    mock.patch.object(grow, 'get_github_org', return_value='org'), \
                    mock.patch.object(grow, 'get_github_token', return_value='fixture-token'), \
                    mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                    mock.patch.object(grow.site_languages, 'Repository', return_value=repo), \
                    mock.patch.object(grow, 'requests', github):
                result = grow.do_deploy_task_run_internal('app', 'run', False, preview)
                self.assertEqual('error', result['result'])
                self.assertFalse(result['retry'])
                self.assertIn(f'/{folder}/ is reserved', result['message'])

        github.post.assert_not_called()
        github.patch.assert_not_called()
        repo.commit.assert_not_called()

    def test_initializing_site_waits_for_github_ref_before_syncing(self):
        grow = self.module
        languages = grow.site_languages
        site = {'app_id': 'app', 'site_id': 57, 'language': 'zh-hans', 'site_template': 'seenical',
                'template_initialization': 'pending'}
        layout = languages.layout_for('zh-hans', {'default_language': 'zh-hans', 'overrides': {}})
        repo = languages.Repository('https://api.github.test/repos/org/site', {})
        redis = mock.Mock()
        redis.lock.side_effect = lambda *_args, **_kwargs: nullcontext()
        waiting = mock.Mock(status_code=409, ok=False)
        missing = mock.Mock(status_code=404, ok=False)
        ready = mock.Mock(status_code=200, ok=True)
        ready.json.return_value = {'object': {'sha': 'base'}}

        with mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'get_site', return_value=site), \
                mock.patch.object(grow, 'managed_site_repository', return_value=(repo, 'org', 'site')), \
                mock.patch.object(languages.requests, 'request', side_effect=[waiting, missing, ready], create=True) as request, \
                mock.patch.object(languages.time, 'sleep') as sleep, \
                mock.patch.object(repo, 'json', return_value=layout), \
                mock.patch.object(languages, 'sync', return_value='') as sync:
            result = grow.sync_hosted_languages({}, site, background=True)

        ref_request = mock.call('GET', 'https://api.github.test/repos/org/site/git/ref/heads/master',
                                headers={}, timeout=25)
        self.assertEqual([ref_request] * 3, request.call_args_list)
        self.assertEqual([mock.call(1), mock.call(2)], sleep.call_args_list)
        sync.assert_called_once()
        self.assertEqual('base', sync.call_args.args[3])
        self.assertEqual({'upgrade_url': ''}, result)

    def test_closed_upgrade_pr_does_not_mark_canonical_as_synced(self):
        grow = self.module
        languages = grow.site_languages
        old = {'app_id': 'app', 'site_id': 7, 'name': 'Docs', 'language': 'zh-hans',
               'canonical_link': 'https://old.example'}
        site = {**old, 'canonical_link': 'https://new.example', 'site_languages': {
            'default_language': 'zh-hans', 'overrides': {'en': {'title': 'Docs'}}}}
        old_state = json.dumps(languages.book_sync_state(old))
        state = {'book_sync_state': old_state}
        redis = mock.Mock()
        redis.lock.side_effect = lambda *_args, **_kw: nullcontext()
        redis.hget.side_effect = lambda _key, field: state.get(field)
        redis.hmset.side_effect = lambda _key, values: state.update(values)
        repo = mock.Mock()
        repo.request.return_value = {'object': {'sha': 'base'}}
        repo.read.return_value = None
        book = {'pluginsConfig': {'lanying-grow-ai': {'sitemap_hostname': old['canonical_link']},
                'canonical-link': {'baseURL': old['canonical_link']}, 'edit-link': {'base': 'github'}}}
        repo.json.side_effect = lambda path, *_args, **_kw: book if path == 'book.json' else None
        repo.blob.side_effect = lambda data: data
        repo.commit.side_effect = ['https://example.invalid/pull/1', 'https://example.invalid/pull/2']
        repo.upgrade_is_open.return_value = False
        with mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'managed_site_repository', return_value=(repo, 'org', 'site')), \
                mock.patch.object(languages, 'legacy_upgrade_entries', return_value={}):
            grow.sync_hosted_languages(old, site, allow_upgrade=True, before_publish=mock.Mock())
            self.assertEqual(old_state, state['book_sync_state'])
            grow.sync_hosted_languages({**site, **state}, site, allow_upgrade=True, before_publish=mock.Mock())
        entries = repo.commit.call_args.args[1]
        plugins = json.loads(entries['book.json']['sha'])['pluginsConfig']
        self.assertEqual(site['canonical_link'], plugins['canonical-link']['baseURL'])
        self.assertEqual(site['canonical_link'], plugins['lanying-grow-ai']['sitemap_hostname'])

    def test_merged_upgrade_keeps_repository_titles_on_name_only_save(self):
        grow = self.module
        languages = grow.site_languages
        old = {'app_id': 'app', 'site_id': 7, 'name': 'Docs', 'language': 'zh-hans', 'title': 'Old'}
        site = {**old, 'title': 'Published', 'site_languages': {
            'default_language': 'zh-hans', 'overrides': {'en': {'title': 'Published English'}}}}
        url = 'https://example.invalid/pull/1'
        site['language_upgrade_url'] = url
        stored = {'book_sync_state': json.dumps(languages.book_sync_state(old)),
                  'language_upgrade_sync_state': json.dumps(languages.book_sync_state(site))}
        redis = mock.Mock()
        redis.lock.side_effect = lambda *_args, **_kw: nullcontext()
        redis.hget.side_effect = lambda _key, field: stored.get(field)
        redis.hmset.side_effect = lambda _key, fields: stored.update(fields)
        repo = mock.Mock()
        repo.request.return_value = {'object': {'sha': 'base'}}
        repo.read.return_value = b'# Home'
        repo.blob.side_effect = lambda data: data
        repo.commit.return_value = ''
        repo.upgrade_is_merged.return_value = True
        books = {'book.json': {'title': 'Manual root title', 'pluginsConfig': {'lanying-grow-ai': {}}},
                 'site-layout.json': languages.layout_for('zh-hans', site['site_languages'], 'zh-hans'),
                 'zh-hans/book.json': {'title': 'Manual Chinese title'},
                 'en/book.json': {'title': 'Manual English title'}}
        repo.json.side_effect = lambda file, *_a, **_kw: books.get(file)
        with mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'managed_site_repository', return_value=(repo, 'org', 'site')):
            grow.sync_hosted_languages(site, {**site, 'name': 'Renamed'})
        repo.upgrade_is_merged.assert_called_once_with(url)
        self.assertEqual({}, repo.commit.call_args.args[1])
        self.assertEqual(languages.book_sync_state(site), json.loads(stored['book_sync_state']))

    def test_old_console_configures_new_gitbook_directory(self):
        grow = self.module
        old = {'app_id': 'app', 'site_id': 7, 'name': 'Docs', 'language': 'zh-hans',
               'title': 'Published title', 'canonical_link': 'https://current.example',
               'github_hosting': 'off', 'github_url': 'https://github.com/org/site',
               'github_base_dir': '/old', 'github_base_branch': 'master'}
        new = {**old, 'github_base_dir': '/new/', 'hook_sentence_image': 'https://cdn.example/banner.png'}
        previous = grow.site_languages.book_sync_state(old)
        book = {'title': 'Template title', 'pluginsConfig': {'lanying-grow-ai': {},
                'canonical-link': {'baseURL': 'https://template.example'}}}
        read = mock.Mock(status_code=200)
        read.json.return_value = {'sha': 'base', 'content': base64.b64encode(json.dumps(book).encode())}
        with mock.patch.object(grow, 'site_book_baseline', return_value=previous), \
                mock.patch.object(grow.requests, 'get', return_value=read, create=True), \
                mock.patch.object(grow.requests, 'put', return_value=mock.Mock(status_code=200), create=True) as write, \
                mock.patch.object(grow, 'upload_file_to_github', side_effect=[
                    {'result': 'error'}, {'result': 'ok'}]) as upload, \
                mock.patch.object(grow, 'update_site_field') as update:
            failed = grow.sync_to_github(old, new)
            self.assertEqual('error', failed['result'])
            self.assertIn('Save again', failed['message'])
            write.assert_not_called()
            update.assert_not_called()
            # The saved Console directory is already /new; Git still has the old baseline.
            self.assertEqual({'result': 'ok'}, grow.sync_to_github(new, new))
            self.assertEqual(2, upload.call_count)
            update.assert_called_once()
        self.assertTrue(write.call_args.args[0].endswith('/contents/new/book.json'))
        updated = json.loads(base64.b64decode(write.call_args.kwargs['json']['content']))
        self.assertEqual(new['title'], updated['title'])
        self.assertEqual(new['canonical_link'], updated['pluginsConfig']['canonical-link']['baseURL'])
        self.assertEqual('assets/hook-sentence/banner.png', updated['pluginsConfig']['lanying-grow-ai']['hook_sentence_image'])

    def test_hook_image_upload_creates_or_updates_the_target_file(self):
        grow = self.module
        site = {'github_url': 'https://github.com/org/site', 'github_hosting': 'off',
                'github_base_dir': '/new', 'github_base_branch': 'docs'}
        url = 'https://cdn.example/banner.png'
        for lookup_status, upload_status in [(404, 201), (200, 200)]:
            with self.subTest(lookup_status=lookup_status):
                image = mock.Mock(status_code=200, content=b'image')
                existing = mock.Mock(status_code=lookup_status)
                existing.json.return_value = {'sha': 'file-sha'}
                with mock.patch.object(grow.lanying_oss, 'get_cdn_url', return_value='https://cdn.example/', create=True), \
                        mock.patch.object(grow.requests, 'get', side_effect=[image, existing], create=True), \
                        mock.patch.object(grow.requests, 'put', return_value=mock.Mock(status_code=upload_status), create=True) as put:
                    self.assertEqual({'result': 'ok'}, grow.upload_file_to_github(site, url, 'Upload image'))
                self.assertTrue(put.call_args.args[0].endswith('/contents/new/assets/hook-sentence/banner.png'))
                payload = put.call_args.kwargs['json']
                self.assertEqual('docs', payload['branch'])
                self.assertEqual('file-sha' if lookup_status == 200 else None, payload.get('sha'))

    def test_repository_lock_releases_only_its_own_lease_without_lua(self):
        redis = mock.MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        lock = self.module.SiteRepositoryLock(redis, 'repo-lock', timeout=1200)
        self.assertTrue(lock.acquire())
        pipe.get.return_value = lock.local.token
        lock.release()
        pipe.watch.assert_called_once_with('repo-lock')
        pipe.delete.assert_called_once_with('repo-lock')
        pipe.execute.assert_called_once()
        redis.register_script.return_value.assert_not_called()

        pipe.reset_mock()
        self.assertTrue(lock.acquire())
        pipe.get.return_value = b'new-owner'
        with self.assertRaises(self.module.LockNotOwnedError):
            lock.release()
        pipe.delete.assert_not_called()

    def test_established_language_save_persists_before_enqueuing(self):
        grow = self.module
        original = {'app_id': 'app', 'site_id': '7', 'language': 'zh-hans', 'agent_tools_revision': '4',
                    'github_hosting': 'on', 'github_url': 'https://github.com/org/site',
                    'github_hosting_url': 'https://github.com/org/site',
                    'site_languages_enabled': 'on',
                    'site_languages': {'default_language': 'zh-hans', 'overrides': {'en': {'title': 'English'}}}}
        site_setting = mock.Mock(app_id='app', baidu_token=None, github_hosting='on')
        site_setting.to_hmset_fields.side_effect = lambda: dict(
            github_hosting='on', github_url='', github_token='', github_base_branch='master',
            baidu_token=None, name='Edited name')
        change = {'default_language': 'en', 'overrides': original['site_languages']['overrides']}
        for language_payload in (change, None):
            with self.subTest(language_payload=language_payload):
                redis = mock.MagicMock()
                pipe = redis.pipeline.return_value.__enter__.return_value
                pipe.hget.return_value = b'4'
                with mock.patch.object(grow, 'check_site_setting', return_value={'result': 'ok'}), \
                        mock.patch.object(grow, 'get_github_org', return_value='org'), \
                        mock.patch.object(grow, 'get_site', return_value=original), \
                        mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                        mock.patch.object(grow, 'sync_hosted_languages') as sync, \
                        mock.patch.object(grow, 'enqueue_site_language_sync') as enqueue:
                    def verify_persisted(site):
                        self.assertEqual(5, site['agent_tools_revision'])
                        pipe.execute.assert_called_once()
                    enqueue.side_effect = verify_persisted
                    self.assertEqual('ok', grow.configure_site('7', site_setting, language_payload)['result'])
                    sync.assert_not_called()
                    enqueue.assert_called_once()
                    fields = pipe.hmset.call_args.args[1]
                    self.assertEqual('queued', fields['site_languages_sync_status'])
                    self.assertEqual('on', fields['site_languages_sync_pending'])
                    if language_payload is None:
                        self.assertNotIn('site_languages', fields)
                    else:
                        self.assertEqual('en', json.loads(fields['site_languages'])['default_language'])

    def test_failed_collaborator_save_keeps_pending_language_sync(self):
        grow = self.module
        old = {'app_id': 'app', 'site_id': '7', 'name': 'Docs', 'type': 'gitbook',
               'language': 'zh-hans', 'agent_tools_revision': '4', 'collaborator': '',
               'github_hosting': 'on', 'github_url': 'https://github.com/org/site',
               'github_hosting_url': 'https://github.com/org/site',
               'site_languages': {'default_language': 'en', 'overrides': {'en': {}}},
               'site_languages_enabled': 'on', 'site_languages_sync_status': 'queued',
               'site_languages_sync_pending': 'on'}
        setting = mock.Mock(app_id='app', baidu_token=None, github_hosting='on')
        setting.to_hmset_fields.return_value = {
            'github_hosting': 'on', 'github_url': '', 'github_token': '',
            'github_base_branch': 'master', 'baidu_token': None, 'collaborator': 'maintainer',
        }
        redis = mock.MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.hget.return_value = b'4'
        stored = dict(old)
        pipe.hmset.side_effect = lambda _key, fields: stored.update(fields)
        github = mock.Mock(get_repo=mock.Mock(side_effect=TimeoutError))
        with mock.patch.object(grow, 'check_site_setting', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'get_github_org', return_value='org'), \
                mock.patch.object(grow, 'get_site', side_effect=lambda *_: dict(stored)), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'get_github', return_value=github):
            with self.assertRaises(TimeoutError):
                grow.configure_site('7', setting)
        self.assertEqual('maintainer', stored['collaborator'])
        self.assertEqual('queued', stored['site_languages_sync_status'])
        self.assertEqual('on', stored['site_languages_sync_pending'])
        self.assertEqual('en', stored['site_languages']['default_language'])

    def test_stalled_language_sync_can_be_requeued_once_at_the_threshold(self):
        grow = self.module
        delay = grow.SITE_LANGUAGE_SYNC_REQUEUE_SECONDS
        now = 1000
        stored = {'agent_tools_revision': 4, 'site_languages_sync_status': 'queued',
                  'site_languages_sync_queued_at': now - delay + 1}
        site = {'app_id': 'app', 'site_id': '7', **stored}
        redis, pipe = mock.Mock(), mock.Mock()
        redis.transaction.side_effect = lambda fn, _key, **_kwargs: fn(pipe)
        pipe.hget.side_effect = lambda _key, field: stored.get(field)
        pipe.hmset.side_effect = lambda _key, fields: stored.update(fields)
        with mock.patch.object(grow.time, 'time', side_effect=lambda: now), \
                mock.patch.object(grow, 'is_standard_hosted_site', return_value=True), \
                mock.patch.object(grow, 'get_site', side_effect=lambda *_: {**site, **stored}), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow.lanying_redis, 'redis_hget', create=True,
                                  side_effect=lambda _pipe, _key, field: stored.get(field)), \
                mock.patch.object(grow, 'enqueue_site_language_sync') as enqueue:
            grow.retry_site_language_sync('app', '7')
            enqueue.assert_not_called()
            now += 1
            grow.retry_site_language_sync('app', '7')
            grow.retry_site_language_sync('app', '7')
            enqueue.assert_called_once()
        self.assertEqual(now, stored['site_languages_sync_queued_at'])
        self.assertEqual(4, stored['agent_tools_revision'])

    def test_first_language_upgrade_still_returns_its_pr(self):
        grow = self.module
        old = {'app_id': 'app', 'site_id': '7', 'language': 'zh-hans', 'agent_tools_revision': '4',
               'github_hosting': 'on', 'github_url': 'https://github.com/org/site',
               'github_hosting_url': 'https://github.com/org/site'}
        setting = mock.Mock(app_id='app', baidu_token=None, github_hosting='on')
        setting.to_hmset_fields.return_value = {
            'github_hosting': 'on', 'github_url': '', 'github_token': '',
            'github_base_branch': 'master', 'baidu_token': None,
        }
        redis = mock.MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.hget.return_value = b'4'

        def sync(_original, _candidate, *, allow_upgrade, before_publish):
            self.assertTrue(allow_upgrade)
            before_publish('upgrade-branch')
            return {'upgrade_url': 'https://github.com/org/site/pull/1'}

        with mock.patch.object(grow, 'check_site_setting', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'get_github_org', return_value='org'), \
                mock.patch.object(grow, 'get_site', return_value=old), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'sync_hosted_languages', side_effect=sync), \
                mock.patch.object(grow, 'enqueue_site_language_sync') as enqueue, \
                mock.patch.object(grow, 'maybe_invite_github_member'), \
                mock.patch.object(grow, 'maybe_register_github_site'), \
                mock.patch.object(grow, 'maybe_init_analytics'):
            result = grow.configure_site('7', setting, {'default_language': 'en', 'overrides': {'en': {}}})
        self.assertEqual('https://github.com/org/site/pull/1', result['data']['language_upgrade_url'])
        enqueue.assert_not_called()

    def test_failed_queue_job_records_failure_for_its_revision(self):
        grow = self.module
        site = {'app_id': 'app', 'site_id': '7', 'agent_tools_revision': '4',
                'site_languages_sync_status': 'queued'}
        with mock.patch.object(grow, 'get_site', return_value=site), \
                mock.patch.object(grow, 'sync_hosted_languages', side_effect=ValueError), \
                mock.patch.object(grow, 'transition_site_language_sync', return_value=True) as transition, \
                self.assertLogs(level='WARNING'):
            grow.sync_queued_site_languages('app', '7')
        transition.assert_called_once_with(site, 'queued', 'failed')

    def test_queued_sync_keeps_new_revision_pending_after_github_commit(self):
        grow = self.module
        site = {'app_id': 'app', 'site_id': '7', 'language': 'zh-hans', 'agent_tools_revision': '4',
                'site_languages_sync_status': 'queued',
                'site_languages': {'default_language': 'en', 'overrides': {'en': {}}}}
        redis = mock.MagicMock()
        repository = mock.Mock()
        repository.request.return_value = {'object': {'sha': 'head'}}
        repository.json.return_value = {'version': 2}
        key = grow.get_site_key('app', '7')
        with mock.patch.object(grow, 'managed_site_repository', return_value=(repository, 'org', 'repo')), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'site_book_baseline', return_value={}), \
                mock.patch.object(grow, 'get_site', return_value=site), \
                mock.patch.object(grow.site_languages, 'sync', return_value='') as sync, \
                mock.patch.object(grow, 'transition_site_language_sync', return_value=False) as transition:
            with self.assertRaises(grow.WatchError):
                grow.sync_hosted_languages(site, site, queued=True)
            sync.assert_called_once()
            redis.hset.assert_called_once_with(key, 'book_sync_state', json.dumps(grow.site_languages.book_sync_state(site)))
            redis.hmset.assert_not_called()
            transition.assert_called_once_with(site, 'queued', 'synced')

    def test_retry_queues_only_the_already_saved_revision(self):
        grow = self.module
        site = {'app_id': 'app', 'site_id': '7', 'agent_tools_revision': '4',
                'site_languages_sync_status': 'failed'}
        queued = {**site, 'site_languages_sync_status': 'queued'}
        with mock.patch.object(grow, 'is_standard_hosted_site', return_value=True), \
                mock.patch.object(grow, 'get_site', side_effect=[site, queued]), \
                mock.patch.object(grow, 'transition_site_language_sync', return_value=True) as transition, \
                mock.patch.object(grow, 'enqueue_site_language_sync') as enqueue:
            result = grow.retry_site_language_sync('app', '7')
            self.assertEqual('queued', result['data']['site_languages_sync_status'])
            transition.assert_called_once_with(site, 'failed', 'queued')
            enqueue.assert_called_once_with(site)

    def test_english_site_creation_returns_before_hosting_starts(self):
        grow = self.module
        settings = mock.Mock(app_id='app', language='en', github_hosting='on')
        settings.to_hmset_fields.return_value = {}
        redis = mock.Mock()
        with mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(grow, 'check_site_num_limit', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'check_site_setting', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'generate_site_id', return_value=7), \
                mock.patch.object(grow.executor, 'submit') as submit, \
                mock.patch.object(grow, 'initialize_new_site', side_effect=TimeoutError) as initialize:
            self.assertEqual(grow.create_site(settings), {'result': 'ok', 'data': {'site_id': 7}})
            initialize.assert_not_called()
            submit.assert_called_once_with(grow.initialize_english_site, 'app', 7)
            fields = redis.hmset.call_args.args[1]
            self.assertEqual('on', fields['site_language_seed_pending'])
            with mock.patch.object(grow, 'get_site', return_value=fields):
                self.assertEqual('error', grow.configure_site(7, settings)['result'])
            with self.assertLogs(level='ERROR'):
                grow.initialize_english_site('app', 7)
            redis.hdel.assert_called_once_with(grow.get_site_key('app', 7), 'site_initialization_until')
            redis.rpush.assert_called_once_with(grow.get_site_list_key('app'), 7)

    def test_task_binding_allows_disabled_security_with_any_site_configuration(self):
        for site_ids in [[], ["default-site"], ["custom-domain-site"], ["default-site", "custom-domain-site"]]:
            with self.subTest(site_ids=site_ids), mock.patch.object(
                    self.module.lanying_chatbot, "get_chatbot", create=True,
                    return_value={"content_security": "off"}), mock.patch.object(
                    self.module, "get_site_list") as sites:
                result = self.module.validate_task_chatbot("app", types.SimpleNamespace(
                    chatbot_id="bot", site_id_list=site_ids))
                self.assertEqual(result, {"result": "ok"})
                sites.assert_not_called()

    def test_task_binding_still_rejects_missing_chatbot(self):
        with mock.patch.object(self.module.lanying_chatbot, "get_chatbot", create=True, return_value=None):
            result = self.module.validate_task_chatbot("app", types.SimpleNamespace(chatbot_id="missing"))
        self.assertEqual(result, {"result": "error", "message": "chatbot id not exist"})

    def test_plan_delete_checks_preview_before_deleting_any_run(self):
        runs = [{'task_run_id': 'first', 'status': 'success'},
                {'task_run_id': 'second', 'status': 'success', 'preview_id': 'preview'}]
        with mock.patch.object(self.module, 'get_task', return_value=task()), \
                mock.patch.object(self.module, 'get_task_run_list', return_value={'data': {'list': runs}}), \
                mock.patch.object(self.module, 'get_preview', return_value={}), \
                mock.patch.object(self.module, 'delete_task_run') as delete:
            self.assertEqual(self.module.delete_task('app', 'task')['message'], 'task_run has preview')
            delete.assert_not_called()

    def test_plan_delete_stops_when_child_deletion_is_rejected(self):
        with mock.patch.object(self.module, 'get_task', return_value=task()), \
                mock.patch.object(self.module, 'get_task_run_list',
                                  return_value={'data': {'list': [{'task_run_id': 'run', 'status': 'success'}]}}), \
                mock.patch.object(self.module, 'delete_task_run',
                                  return_value={'result': 'error', 'message': 'material_run_active'}), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection') as redis:
            self.assertEqual(self.module.delete_task('app', 'task')['result'], 'error')
            redis.assert_not_called()

    def test_queue_failure_cancels_run_releases_materials_and_fences_late_delivery(self):
        current = dict(task(), reference_document_ids=['1-1'])
        state = {}
        redis = mock.MagicMock()
        redis.hmset.side_effect = lambda k, v: state.update(v)
        pipe = mock.MagicMock()
        pipe.__enter__.return_value = pipe
        pipe.hset.side_effect = lambda k, f, v: state.update({f: v})
        redis.pipeline.return_value = pipe
        materials = types.SimpleNamespace(MaterialError=ValueError, set_references=mock.Mock(return_value=['1-1']),
                                          release_owner=mock.Mock(), enqueue=mock.Mock())
        job = types.SimpleNamespace(apply_async=mock.Mock(side_effect=RuntimeError('queue down')))
        with mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials,
                                          'lanying_tasks': types.SimpleNamespace(grow_ai_run_task=job)}), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(self.module.lanying_redis, 'redis_hgetall', create=True, side_effect=lambda p, k: dict(state)), \
                mock.patch.object(self.module, 'get_task', return_value=current), \
                mock.patch.object(self.module, 'get_task_run', side_effect=lambda *a: dict(state)), \
                mock.patch.object(self.module, 'generate_task_run_id', return_value='run'), \
                mock.patch.object(self.module, 'generate_dummy_user_id', return_value='user'), \
                mock.patch.object(self.module, '_task_run_notification_route', return_value={}), \
                mock.patch.object(self.module, 'resolve_article_language', return_value='en'), \
                mock.patch.object(self.module, 'set_admin_token'), \
                mock.patch.object(self.module, 'update_task_run_field', side_effect=lambda a, r, f, v: state.update({f: v})):
            result = self.module.run_task('app', 'task')
            self.assertEqual(result['result'], 'error')
            self.assertEqual(state['status'], 'error')
            materials.release_owner.assert_called_once_with('app', 'run', 'run')
            old_dispatch = state['dispatch_id']
            self.assertFalse(self.module.transition_run_dispatch('app', 'run', old_dispatch))
            # Manual retry is possible, and another publish failure is recoverable.
            result = self.module.task_run_retry('app', 'run')
            self.assertEqual(result['result'], 'error')
            self.assertEqual(state['status'], 'error')
            self.assertEqual(materials.release_owner.call_count, 2)
            self.assertNotEqual(state['dispatch_id'], old_dispatch)
            self.assertFalse(self.module.transition_run_dispatch('app', 'run', old_dispatch))

    def test_dispatch_cannot_cancel_materials_already_claimed_by_worker(self):
        state = {'dispatch_id': 'current', 'dispatch_state': 'waiting'}
        pipe = mock.MagicMock()
        pipe.__enter__.return_value = pipe
        pipe.hset.side_effect = lambda k, f, v: state.update({f: v})
        redis = mock.Mock(pipeline=lambda: pipe)
        with mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(self.module.lanying_redis, 'redis_hgetall', create=True, side_effect=lambda p, k: dict(state)):
            self.assertFalse(self.module.transition_run_dispatch('app', 'run', 'old-delivery'))
            self.assertTrue(self.module.transition_run_dispatch('app', 'run', 'current'))
            self.assertFalse(self.module.cancel_unclaimed_run('app', 'run', 'current'))
            self.assertEqual(state['dispatch_state'], 'claimed')
            self.assertTrue(self.module.transition_run_dispatch('app', 'run', 'current'))

    def test_material_patch_uses_mysql_and_omission_preserves_references(self):
        current = task()
        materials = types.SimpleNamespace(MaterialError=ValueError, set_references=mock.Mock(
            side_effect=lambda *args, **kwargs: kwargs['persist_owner']()))
        with mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}), \
                mock.patch.object(self.module, 'get_task', return_value=current), \
                mock.patch.object(self.module, 'validate_task_chatbot', return_value={'result': 'ok'}), \
                mock.patch.object(self.module, 'update_task_field'):
            for changes in [{'name': 'Renamed'}, {'reference_document_ids': []}, {'reference_document_ids': ['1-1']}]:
                redis = FakeRedis(4)
                materials.set_references.reset_mock()
                with mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis):
                    result = self.module.patch_task('app', 'task', changes)
                self.assertEqual(result['result'], 'ok')
                self.assertNotIn('file_list', redis.hmset_calls[0][1])
                self.assertNotIn('schedule', redis.hmset_calls[0][1])
                self.assertNotIn('reference_document_ids', redis.hmset_calls[0][1])
                if 'reference_document_ids' in changes:
                    materials.set_references.assert_called_once_with('app', 'plan', 'task', changes['reference_document_ids'],
                                                                     persist_owner=mock.ANY)
                else:
                    materials.set_references.assert_not_called()

    def test_material_cleanup_error_does_not_change_terminal_generation_result(self):
        redis = mock.Mock()
        materials = types.SimpleNamespace(release_owner=mock.Mock(side_effect=RuntimeError('MySQL unavailable')), enqueue=mock.Mock())
        with mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis):
            self.module.update_task_run_field('app', 'run', 'status', 'success')
        redis.hset.assert_called_once_with(self.module.get_task_run_key('app', 'run'), 'status', 'success')
        materials.enqueue.assert_called_once_with('app')

    def test_run_snapshots_input_and_acquires_materials_before_queueing(self):
        current = dict(task(), reference_document_ids=['1-1'])
        redis = mock.Mock()
        events = []
        def acquire(*args, **kwargs):
            events.append('acquire')
            return list(args[3])
        materials = types.SimpleNamespace(MaterialError=ValueError, set_references=mock.Mock(side_effect=acquire))
        job = types.SimpleNamespace(apply_async=mock.Mock(side_effect=lambda *a, **k: events.append('queue')))
        with mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials,
                                          'lanying_tasks': types.SimpleNamespace(grow_ai_run_task=job)}), \
                mock.patch.object(self.module, 'get_task', return_value=current), \
                mock.patch.object(self.module, 'generate_task_run_id', return_value='run'), \
                mock.patch.object(self.module, 'generate_dummy_user_id', return_value='9'), \
                mock.patch.object(self.module, '_task_run_notification_route', return_value={}), \
                mock.patch.object(self.module, 'set_admin_token'), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis):
            result = self.module.run_task('app', 'task')
        self.assertEqual(result['result'], 'ok')
        self.assertEqual(events, ['acquire', 'queue'])
        materials.set_references.assert_called_once_with('app', 'run', 'run', ['1-1'], ready_only=True)
        run = {'start_from': 0}
        for call in redis.hmset.call_args_list:
            run.update(call.args[1])
        snapshot = json.loads(run['material_input_snapshot'])
        current['article_prompt'] = 'Changed after enqueue'
        current['reference_document_ids'].clear()
        current['title_reuse'] = 'off'
        self.assertEqual(snapshot['article_prompt'], 'Old article prompt')
        self.assertEqual(snapshot['reference_document_ids'], ['1-1'])
        self.assertEqual(snapshot['file_list'], [])
        self.assertEqual(snapshot['title_reuse'], 'on')
        # The worker must use this run's policy, not the edited plan's policy.
        redis.hexists.return_value = False
        with mock.patch.object(self.module, 'get_task_run', return_value=run), \
                mock.patch.object(self.module, 'get_task', return_value=current), \
                mock.patch.object(self.module.lanying_chatbot, 'get_chatbot', return_value={'user_id': '9'}, create=True), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(self.module, 'parse_file_keywords', return_value=[]), \
                mock.patch.object(self.module, 'find_title', return_value={'result': 'error', 'message': 'stop'}) as find, \
                mock.patch.object(self.module, 'make_task_run_result_zip_file'):
            self.module.do_run_task_internal('app', 'run', False)
        self.assertEqual(find.call_args.args[4], 'on')

    def test_writing_preference_patch_rejects_races_and_clears_legacy_rules(self):
        from redis.exceptions import WatchError

        module = self.module
        legacy = ('内容主题：\n产品介绍\n\n自定义提示词（兼容旧数据）：\n先写 FAQ'
                  '\n\nSeenical 会话上下文：Agent A')
        original = dict(task(), prompt=legacy, article_prompt='', article_language_scoped='off')
        updated = dict(original, prompt='产品介绍')
        self.assertEqual(('产品介绍', '先写 FAQ'), module.split_legacy_writing_prompt(legacy))
        self.assertEqual(('Unrelated prompt', None), module.split_legacy_writing_prompt('Unrelated prompt'))
        redis = mock.MagicMock()
        pipeline = redis.pipeline.return_value.__enter__.return_value
        stored = {'create_time': '1', 'revision': str(original['revision']), 'prompt': legacy}
        with mock.patch.object(module, 'get_task', return_value=original) as read_task, \
                mock.patch.object(module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(module.lanying_redis, 'redis_hgetall', create=True, return_value=stored), \
                mock.patch.object(module, 'validate_task_chatbot', return_value={'result': 'ok'}):
            stale = module.patch_task('app', 'task', {'article_prompt': ''}, expected_article_prompt='old draft')
            self.assertEqual(module.WRITING_PREFERENCES_CONFLICT, stale['message'])
            redis.pipeline.assert_not_called()
            pipeline.execute.side_effect = WatchError()
            raced = module.patch_task('app', 'task', {'article_prompt': ''}, expected_article_prompt='先写 FAQ')
            self.assertEqual(module.WRITING_PREFERENCES_CONFLICT, raced['message'])
            pipeline.execute.side_effect = None
            read_task.side_effect = [original, updated, updated]
            result = module.patch_task('app', 'task', {'article_prompt': ''}, expected_article_prompt='先写 FAQ')
        self.assertEqual('ok', result['result'])
        submitted = pipeline.hmset.call_args.args[1]
        self.assertEqual('', submitted['article_prompt'])
        self.assertEqual('产品介绍', submitted['prompt'])
        self.assertNotIn('schedule', submitted)
        redis.hmset.assert_not_called()

    def test_full_editor_patch_guards_both_settings_and_material_references(self):
        from redis.exceptions import WatchError

        module = self.module
        original = task()
        changes = {'prompt': 'Edited topic', 'article_prompt': 'Proposed preference', 'reference_document_ids': ['1-1']}
        redis = mock.MagicMock()
        pipeline = redis.pipeline.return_value.__enter__.return_value
        stored = dict(original, create_time='1')
        references = []

        def set_references(app_id, owner_type, owner_id, values, persist_owner):
            persist_owner()
            references[:] = values

        materials = types.SimpleNamespace(MaterialError=ValueError, set_references=mock.Mock(side_effect=set_references))
        with mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}), \
                mock.patch.object(module, 'get_task', return_value=original), \
                mock.patch.object(module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(module.lanying_redis, 'redis_hgetall', create=True, return_value=stored), \
                mock.patch.object(module, 'validate_task_chatbot', return_value={'result': 'ok'}):
            stale = module.patch_task('app', 'task', changes, expected_article_prompt='stale')
            self.assertEqual(module.WRITING_PREFERENCES_CONFLICT, stale['message'])
            materials.set_references.assert_not_called()
            pipeline.execute.side_effect = WatchError()
            raced = module.patch_task('app', 'task', changes, expected_article_prompt=original['article_prompt'])
            self.assertEqual(module.WRITING_PREFERENCES_CONFLICT, raced['message'])
            self.assertEqual([], references)
            pipeline.execute.side_effect = None
            result = module.patch_task('app', 'task', changes, expected_article_prompt=original['article_prompt'])
            self.assertEqual('ok', result['result'])
        submitted = pipeline.hmset.call_args.args[1]
        self.assertEqual(changes['prompt'], submitted['prompt'])
        self.assertEqual(changes['article_prompt'], submitted['article_prompt'])
        self.assertEqual(changes['reference_document_ids'], references)
        self.assertNotIn('schedule', submitted)
        redis.hmset.assert_not_called()

    def test_task_read_preserves_the_difference_between_cleared_and_unsaved_prompt(self):
        with mock.patch.object(self.module.lanying_redis, 'redis_hgetall', create=True) as read, \
                mock.patch.dict(sys.modules, {
                    'lanying_seenical_materials': types.SimpleNamespace(reference_ids=lambda *args: [])
                }):
            read.side_effect = [{'create_time': '1'}, {'create_time': '1', 'article_prompt': ''}]
            for saved in (False, True):
                result = self.module.get_task('app', 'task')
                self.assertEqual('', result['article_prompt'])
                self.assertIs(saved, result['article_prompt_saved'])

    def test_patch_only_writes_requested_fields_and_keeps_paused_schedule(self):
        current = task()
        updated = dict(current, article_prompt="New article prompt", revision=5)
        redis = FakeRedis(4)
        with mock.patch.object(self.module, "get_task", side_effect=[current, updated, updated]), mock.patch.object(
                self.module, "validate_task_chatbot", return_value={"result": "ok"}), mock.patch.object(
                self.module.lanying_redis, "get_redis_connection", return_value=redis), mock.patch.object(
                self.module, "update_task_field") as update_field:
            result = self.module.patch_task(
                "app", "task", {"article_prompt": "New article prompt"},
                expected_revision=4, request_id="request-1")

        self.assertEqual("ok", result["result"])
        written = redis.hmset_calls[0][1]
        self.assertEqual("New article prompt", written["article_prompt"])
        self.assertEqual(5, written["revision"])
        self.assertNotIn("schedule", written)
        self.assertEqual("off", result["data"]["task"]["schedule"])
        update_field.assert_not_called()

    def test_run_counts_matching_typed_and_file_titles_once_when_reuse_is_off(self):
        run = {'task_id': 'task', 'article_count': 1, 'cycle_type': 'none', 'start_from': 0}
        redis = mock.Mock()
        redis.hexists.return_value = False
        for reuse, expected in [('off', ['Same', 'Another']), ('on', ['Same', 'Same', 'Another'])]:
            with self.subTest(title_reuse=reuse), \
                    mock.patch.object(self.module, 'get_task_run', return_value=run), \
                    mock.patch.object(self.module, 'get_task', return_value=dict(task(), title_reuse=reuse, keywords='Same')), \
                    mock.patch.object(self.module.lanying_chatbot, 'get_chatbot', return_value={'user_id': '9'}, create=True), \
                    mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis), \
                    mock.patch.object(self.module, 'parse_file_keywords', return_value=['Same', 'Another']), \
                    mock.patch.object(self.module, 'update_task_run_field') as update, \
                    mock.patch.object(self.module, 'find_title', return_value={'result': 'error', 'message': 'stop'}) as find, \
                    mock.patch.object(self.module, 'make_task_run_result_zip_file'):
                self.module.do_run_task_internal('app', 'run', False)
            self.assertEqual(expected, find.call_args.args[3])
            update.assert_called_once_with('app', 'run', 'article_count', len(expected))

    def test_title_edit_keeps_recurring_schedule_and_its_next_run_time(self):
        current = dict(task(), cycle_type='cycle', cycle_interval=3600, schedule_id='timer', keywords='Old title')
        updated = dict(current, keywords='New title', revision=5)
        redis = FakeRedis(4)
        schedule = mock.Mock()
        with mock.patch.object(self.module, 'get_task', side_effect=[current, updated, updated]), \
                mock.patch.object(self.module, 'validate_task_chatbot', return_value={'result': 'ok'}), \
                mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.object(self.module, 'lanying_schedule', schedule):
            result = self.module.patch_task('app', 'task', {
                'keywords': 'New title', 'file_list': [], 'cycle_type': 'cycle', 'cycle_interval': 3600
            })

        self.assertEqual('off', result['data']['task']['schedule'])
        self.assertNotIn('schedule', redis.hmset_calls[0][1])
        self.assertEqual([], schedule.mock_calls)

    def test_legacy_title_file_history_blocks_matching_title_in_original_language(self):
        redis = mock.Mock()
        legacy_key = self.module.article_title_used_key('app', 'task')
        redis.hexists.side_effect = lambda key, title: key == legacy_key and title == 'Same\n'
        with mock.patch.object(self.module.lanying_redis, 'get_redis_connection', return_value=redis):
            self.assertTrue(self.module.is_article_title_used('app', 'task', 'Same'))
            self.assertTrue(self.module.is_article_title_used('app', 'task', 'Same', 'en', 'on', 'en'))
            self.assertFalse(self.module.is_article_title_used('app', 'task', 'Same', 'zh-hans', 'on', 'en'))

    def test_file_titles_remove_line_endings_before_counting_characters(self):
        boundary = '中' * 999
        files = [{'url': 'https://file.example.test/titles'}]
        with mock.patch.object(self.module, 'get_task_file_info', return_value={'object_name': 'titles'}), \
                mock.patch.object(self.module.lanying_utils, 'get_temp_filename', return_value='titles.txt', create=True), \
                mock.patch.object(self.module.lanying_file_storage, 'download', return_value={'result': 'ok'}, create=True), \
                mock.patch.object(self.module, 'readlines_auto', return_value=['Title\r\n', boundary + '\n', '\n']):
            self.assertEqual(['Title', boundary], self.module.parse_file_keywords('app', 'task', files))

    def test_title_file_without_utf16_bom_can_use_gb18030(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = pathlib.Path(directory) / 'titles.txt'
            filename.write_bytes('文章标题\r\n'.encode('gb18030'))
            self.assertEqual([(1, '文章标题')], list(self.module.iter_file_title_lines(filename)))

    def test_new_file_with_overlong_title_reports_the_line_before_it_is_saved(self):
        files = [{'url': 'https://file.example.test/titles'}]
        with mock.patch.object(self.module, 'get_task_file_info', return_value=None), \
                mock.patch.object(self.module.lanying_utils, 'get_temp_filename', return_value='titles.txt', create=True), \
                mock.patch.object(self.module, 'get_dummy_lanying_connector', return_value={}), \
                mock.patch.object(self.module.lanying_chatbot, 'get_default_user_id', return_value='1', create=True), \
                mock.patch.object(self.module.lanying_im_api, 'download_url', return_value={'result': 'ok'}, create=True), \
                mock.patch.object(self.module, 'readlines_auto', return_value=['Valid\n', '\n', 'x' * 1000 + '\n']), \
                mock.patch.object(self.module.lanying_file_storage, 'upload', create=True) as upload, \
                mock.patch.object(self.module, 'set_task_file_info') as save:
            result = self.module.handle_task_file_list('app', 'task', files)
        self.assertEqual({'result': 'error', 'message': 'Title file line 3 exceeds 999 characters'}, result)
        upload.assert_not_called()
        save.assert_not_called()

    def test_stale_revision_does_not_block_last_write_wins_update(self):
        current = task(revision=7)
        updated = dict(current, article_language="en", revision=8)
        redis = FakeRedis(7)
        with mock.patch.object(self.module, "get_task", side_effect=[current, updated, updated]), mock.patch.object(
                self.module, "validate_task_chatbot", return_value={"result": "ok"}), mock.patch.object(
                self.module.lanying_redis, "get_redis_connection", return_value=redis), mock.patch.object(
                self.module, "update_task_field"):
            result = self.module.patch_task(
                "app", "task", {"article_language": "en"}, expected_revision=6)
        self.assertEqual("ok", result["result"])
        self.assertEqual("en", redis.hmset_calls[0][1]["article_language"])

    def test_patch_continues_when_revision_snapshot_cannot_be_saved(self):
        current = task()
        redis = FakeRedis(4)
        with mock.patch.object(self.module, "get_task", return_value=current), mock.patch.object(
                self.module, "validate_task_chatbot", return_value={"result": "ok"}), mock.patch.object(
                self.module.lanying_redis, "get_redis_connection", return_value=redis), mock.patch.object(
                self.module.lanying_agent_tools_storage, "save_seenical_config_revision",
                return_value={"result": "error"}):
            result = self.module.patch_task(
                "app", "task", {"article_prompt": "New article prompt"},
                expected_revision=4, request_id="request-1")

        self.assertEqual("ok", result["result"])
        self.assertEqual("New article prompt", redis.hmset_calls[0][1]["article_prompt"])

    def test_durable_revision_excludes_file_urls_and_deploy_payload(self):
        snapshot = self.module._task_revision_snapshot(task())
        self.assertNotIn("file_list", snapshot)
        self.assertNotIn("deploy", snapshot)
        self.assertEqual("Old article prompt", snapshot["article_prompt"])

    def test_task_result_list_pages_across_runs_and_adds_parent_ids(self):
        run_results = {
            "run-2": [
                {"article_id": "run-2-2", "create_time": 22, "title": "second"},
                {"article_id": "run-2-1", "create_time": 21, "title": "first"},
            ],
            "run-1": [
                {"article_id": "run-1-1", "create_time": 11, "title": "older"},
            ],
        }

        def get_results(app_id, run_id):
            return {"result": "ok", "data": {"list": run_results[run_id]}}

        with mock.patch.object(self.module, "get_task_run_id_list", return_value=["run-2", "run-1"]), mock.patch.object(
                self.module, "get_task_run_result_list", side_effect=get_results):
            first = self.module.get_task_result_list("app", "task", 2)
            second = self.module.get_task_result_list("app", "task", 2, first["data"]["next"])

        self.assertTrue(first["data"]["has_more"])
        self.assertTrue(first["data"]["next"])
        self.assertNotIn("run-2", first["data"]["next"])
        self.assertEqual(["run-2-2", "run-2-1"], [item["article_id"] for item in first["data"]["list"]])
        self.assertEqual("task", first["data"]["list"][0]["task_id"])
        self.assertEqual("run-2", first["data"]["list"][0]["task_run_id"])
        self.assertFalse(second["data"]["has_more"])
        self.assertEqual(["run-1-1"], [item["article_id"] for item in second["data"]["list"]])

    def test_task_result_cursor_remains_stable_when_new_run_is_prepended(self):
        run_results = {
            "run-3": [{"article_id": "run-3-1", "create_time": 31}],
            "run-2": [
                {"article_id": "run-2-2", "create_time": 22},
                {"article_id": "run-2-1", "create_time": 21},
            ],
            "run-1": [{"article_id": "run-1-1", "create_time": 11}],
        }

        def get_results(app_id, run_id):
            return {"result": "ok", "data": {"list": run_results[run_id]}}

        with mock.patch.object(self.module, "get_task_run_id_list", return_value=["run-2", "run-1"]), mock.patch.object(
                self.module, "get_task_run_result_list", side_effect=get_results):
            first = self.module.get_task_result_list("app", "task", 1)
        run_results["run-2"].insert(0, {"article_id": "run-2-3", "create_time": 23})
        with mock.patch.object(self.module, "get_task_run_id_list", return_value=["run-3", "run-2", "run-1"]), mock.patch.object(
                self.module, "get_task_run_result_list", side_effect=get_results):
            second = self.module.get_task_result_list("app", "task", 1, first["data"]["next"])

        self.assertEqual(["run-2-2"], [item["article_id"] for item in first["data"]["list"]])
        self.assertEqual(["run-2-1"], [item["article_id"] for item in second["data"]["list"]])

    def test_task_result_list_rejects_invalid_cursor_and_limit(self):
        self.assertEqual("error", self.module.get_task_result_list("app", "task", 0)["result"])
        self.assertEqual("error", self.module.get_task_result_list("app", "task", 50, "bad")["result"])

    def test_deployment_rollback_requires_unchanged_branch_and_rotates_versions(self):
        current = "a" * 40
        previous = "b" * 40
        site = {
            "site_id": "site-1", "github_base_branch": "main",
            "current_deploy_commit_sha": current,
            "previous_deploy_commit_sha": previous,
        }

        class Lock:
            def __enter__(inner_self):
                return inner_self

            def __exit__(inner_self, *args):
                return False

        response = types.SimpleNamespace(
            status_code=200, json=lambda: {"object": {"sha": current}})
        redis = types.SimpleNamespace(lock=lambda *args, **kwargs: Lock())
        context = {
            "result": "ok", "site": site, "repository": "owner/repo",
            "api_url": "https://api.github.test/repos/owner/repo", "headers": {},
        }
        with mock.patch.object(self.module, "get_site_github_context", return_value=context), mock.patch.object(
                self.module.lanying_redis, "get_redis_connection", return_value=redis), mock.patch.object(
                self.module.requests, "get", return_value=response, create=True), mock.patch.object(
                self.module.requests, "patch", return_value=response, create=True) as patch_ref, mock.patch.object(
                self.module, "update_site_field") as update_field:
            result = self.module.rollback_site_deployment("app", "site-1")

        self.assertEqual("ok", result["result"])
        self.assertEqual({"sha": previous, "force": True}, patch_ref.call_args.kwargs["json"])
        update_field.assert_any_call("app", "site-1", "current_deploy_commit_sha", previous)
        update_field.assert_any_call("app", "site-1", "previous_deploy_commit_sha", current)

    def test_deployment_rollback_recovers_publish_interrupted_after_branch_move(self):
        pending = "c" * 40
        previous = "d" * 40
        site = {
            "site_id": "site-1", "github_base_branch": "main",
            "current_deploy_commit_sha": "",
            "pending_deploy_commit_sha": pending,
            "previous_deploy_commit_sha": previous,
        }

        class Lock:
            def __enter__(inner_self):
                return inner_self

            def __exit__(inner_self, *args):
                return False

        response = types.SimpleNamespace(
            status_code=200, json=lambda: {"object": {"sha": pending}})
        redis = types.SimpleNamespace(lock=lambda *args, **kwargs: Lock())
        context = {
            "result": "ok", "site": site, "repository": "owner/repo",
            "api_url": "https://api.github.test/repos/owner/repo", "headers": {},
        }
        with mock.patch.object(self.module, "get_site_github_context", return_value=context), mock.patch.object(
                self.module.lanying_redis, "get_redis_connection", return_value=redis), mock.patch.object(
                self.module.requests, "get", return_value=response, create=True), mock.patch.object(
                self.module.requests, "patch", return_value=response, create=True), mock.patch.object(
                self.module, "update_site_field") as update_field:
            result = self.module.rollback_site_deployment("app", "site-1")

        self.assertEqual("ok", result["result"])
        self.assertEqual(pending, result["data"]["rolled_back_from"])
        update_field.assert_any_call("app", "site-1", "current_deploy_commit_sha", previous)
        update_field.assert_any_call("app", "site-1", "previous_deploy_commit_sha", pending)


class LoopBindingMaterialFailureTest(unittest.TestCase):
    def test_material_failure_keeps_committed_binding_cache_consistent_on_retry(self):
        grow = load_grow_ai()
        old_key = grow.get_loop_conversation_binding_key('app', 'old')
        new_key = grow.get_loop_conversation_binding_key('app', 'new')
        cache = {old_key: 'old-binding'}
        redis = mock.Mock()
        redis.delete.side_effect = lambda key: cache.pop(key, None)
        redis.set.side_effect = lambda key, value: cache.__setitem__(key, value)
        materials = types.SimpleNamespace(reference_ids=mock.Mock(
            side_effect=[RuntimeError('material read failed'), []]))
        with mock.patch.object(grow.lanying_agent_tools_storage, 'save_seenical_conversation_binding',
                create=True, side_effect=[
                    {'result': 'ok', 'data': {'unbound_task_ids': ['old']}},
                    {'result': 'ok', 'data': {'unbound_task_ids': []}}]), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis), \
                mock.patch.dict(sys.modules, {'lanying_seenical_materials': materials}):
            with self.assertRaisesRegex(RuntimeError, 'material read failed'):
                grow.set_loop_conversation_binding('app', 'new', {'conversation_id': 'group'})
            self.assertNotIn(old_key, cache)
            self.assertEqual(json.loads(cache[new_key])['conversation_id'], 'group')
            self.assertEqual(grow.set_loop_conversation_binding(
                'app', 'new', {'conversation_id': 'group'})['result'], 'ok')
            self.assertNotIn(old_key, cache)


if __name__ == "__main__":
    unittest.main()
