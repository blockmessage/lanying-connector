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

    def pipeline(self, transaction=True):
        return self.pipeline_value


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
        written = redis.pipeline_value.hmset_calls[0][1]
        self.assertEqual("New article prompt", written["article_prompt"])
        self.assertEqual(5, written["revision"])
        self.assertNotIn("schedule", written)
        self.assertEqual("off", result["data"]["task"]["schedule"])
        update_field.assert_not_called()

    def test_revision_conflict_returns_current_task_without_writing(self):
        current = task(revision=7)
        with mock.patch.object(self.module, "get_task", return_value=current):
            result = self.module.patch_task(
                "app", "task", {"article_language": "en"}, expected_revision=6)
        self.assertEqual("error", result["result"])
        self.assertEqual("revision_conflict", result["code"])
        self.assertEqual(7, result["data"]["revision"])


if __name__ == "__main__":
    unittest.main()
