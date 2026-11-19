import importlib.util
import json
import pathlib
import sys
import tempfile
import types
import unittest
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
