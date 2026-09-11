import importlib.util
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
        "lanying_pgvector": types.SimpleNamespace(
            is_enabled=lambda: True,
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
                self.module.lanying_pgvector, "save_seenical_config_revision",
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
