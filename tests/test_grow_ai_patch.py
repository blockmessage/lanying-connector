import importlib.util
import json
import pathlib
import sys
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
        materials = types.SimpleNamespace(MaterialError=ValueError, set_references=mock.Mock(),
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
                mock.patch.object(self.module, 'check_task_content_security', return_value={'result': 'ok'}), \
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
        materials = types.SimpleNamespace(MaterialError=ValueError,
            set_references=mock.Mock(side_effect=lambda *a, **k: events.append('acquire')))
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
        snapshot = json.loads(redis.hmset.call_args.args[1]['material_input_snapshot'])
        current['article_prompt'] = 'Changed after enqueue'
        current['reference_document_ids'].clear()
        self.assertEqual(snapshot['article_prompt'], 'Old article prompt')
        self.assertEqual(snapshot['reference_document_ids'], ['1-1'])
        self.assertEqual(snapshot['file_list'], [])

    def test_patch_only_writes_requested_fields_and_keeps_paused_schedule(self):
        current = task()
        updated = dict(current, article_prompt="New article prompt", revision=5)
        redis = FakeRedis(4)
        with mock.patch.object(self.module, "get_task", side_effect=[current, updated, updated]), mock.patch.object(
                self.module, "check_task_content_security", return_value={"result": "ok"}), mock.patch.object(
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

    def test_stale_revision_does_not_block_last_write_wins_update(self):
        current = task(revision=7)
        updated = dict(current, article_language="en", revision=8)
        redis = FakeRedis(7)
        with mock.patch.object(self.module, "get_task", side_effect=[current, updated, updated]), mock.patch.object(
                self.module, "check_task_content_security", return_value={"result": "ok"}), mock.patch.object(
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
                self.module, "check_task_content_security", return_value={"result": "ok"}), mock.patch.object(
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


if __name__ == "__main__":
    unittest.main()
