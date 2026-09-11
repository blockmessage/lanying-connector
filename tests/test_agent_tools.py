import importlib.util
import json
import pathlib
import sys
import types
import unittest
from unittest import mock


def load_agent_tools():
    module_name = "lanying_agent_tools_test"
    sys.modules.pop(module_name, None)
    stubs = {
        "requests": types.SimpleNamespace(RequestException=Exception),
        "lanying_ai_plugin": types.SimpleNamespace(),
        "lanying_chatbot": types.SimpleNamespace(),
        "lanying_grow_ai": types.SimpleNamespace(ARTICLE_LANGUAGE_VALUES={"auto", "zh-hans", "en"}),
        "lanying_pgvector": types.SimpleNamespace(
            append_agent_tool_audit_log=lambda value: {"result": "ok"},
            get_active_public_skill_catalog=lambda: None,
            get_public_skill_revision=lambda skill_id, revision: None,
            save_public_skill_catalog=lambda value: {"result": "ok"}),
        "lanying_redis": types.SimpleNamespace(),
        "lanying_vendor": types.SimpleNamespace(),
        "yaml": types.SimpleNamespace(safe_load=lambda value: {}),
    }
    old_modules = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        path = pathlib.Path(__file__).resolve().parents[1] / "lanying_agent_tools.py"
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


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.sets = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def setex(self, key, ttl, value):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(value)

    def smembers(self, key):
        return self.sets.get(key, set())

    def pipeline(self, transaction=True):
        return self

    def watch(self, *keys):
        return None

    def unwatch(self):
        return None

    def multi(self):
        return None

    def execute(self):
        return []


class AgentToolsTest(unittest.TestCase):
    def setUp(self):
        self.module = load_agent_tools()
        self.redis = FakeRedis()

    def template_catalog(self):
        root = pathlib.Path(__file__).resolve().parents[2] / "seenical-skill-repository-template"
        manifest = (root / ".seenical/manifest.json").read_text()
        files = {
            path.relative_to(root).as_posix(): (path.read_text(), "sha")
            for path in [
                root / "SKILL.md", root / "agents/openai.yaml",
                root / "references/tool-api.md", root / ".seenical/runtime.json",
                root / ".seenical/tools.json",
            ]
        }
        config = {
            "repository_url": "https://github.com/seenical/skills",
            "owner": "seenical", "repo": "skills", "ref": "main",
            "manifest_path": ".seenical/manifest.json",
        }
        with mock.patch.object(
                self.module, "_github_file",
                side_effect=lambda owner, repo, path, ref, token, limit: files[path]):
            return self.module._normalize_public_skill_catalog(
                config, "a" * 40, manifest, "manifest-sha",
                {".seenical/manifest.json", *files.keys()})

    def activate_catalog(self):
        catalog = self.template_catalog()
        self.redis.set(self.module.PUBLIC_CATALOG_CACHE_KEY, json.dumps(catalog))
        return catalog

    def bind_app(self, app_id="app", user_id="22"):
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            result = self.module.sync_im_binding_projection(app_id, {
                "status": "BOUND", "im_user_id": user_id, "revision": 1
            })
        self.assertEqual("ok", result["result"])

    def test_template_loads_dynamic_butler_tools(self):
        catalog = self.template_catalog()
        skill = catalog["skills"][0]
        self.assertEqual("seenical-console", skill["skill_id"])
        self.assertEqual("butler_api", skill["runtime"]["type"])
        self.assertEqual(set(skill["required_tools"]), {tool["tool_id"] for tool in skill["tools"]})
        self.assertEqual("/app/grow_ai/configure_task",
                         next(tool for tool in skill["tools"] if tool["tool_id"] == "seenical.plan.update")["request"]["path"])

    def test_runtime_rejects_absolute_urls_headers_and_weakened_risk(self):
        runtime = json.dumps({
            "schema_version": 1, "skill_id": "seenical-console",
            "runtime": {"type": "butler_api", "version": 1, "authentication": "host_console_session"},
            "tools_file": ".seenical/tools.json"
        })
        base = {
            "tool_id": "seenical.test", "version": 1, "function_name": "seenical_test",
            "title": "Test", "description": "Test", "risk": "read",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            "request": {"method": "GET", "path": "/app/test", "arguments": "query"},
            "result_fields": ["status"]
        }
        for mutate in [
            lambda tool: tool["request"].update(path="https://evil.example/app/test"),
            lambda tool: tool["request"].update(headers={"Authorization": "x"}),
            lambda tool: tool.update(risk="read", request={"method": "POST", "path": "/app/test", "arguments": "body"}),
        ]:
            tool = json.loads(json.dumps(base))
            mutate(tool)
            with self.subTest(tool=tool), self.assertRaises(ValueError):
                self.module._normalize_butler_runtime(runtime, json.dumps({
                    "schema_version": 1, "tools": [tool], "definitions": {}
                }), "seenical-console")

    def test_capability_is_runtime_scoped_and_requires_bound_im_user(self):
        self.activate_catalog()
        self.bind_app()
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "is_feature_enabled", return_value=True):
            result = self.module.register_capabilities("app", {
                "subject_id": "11", "tenement_id": "2", "im_user_id": "22"
            }, {
                "schema_version": 1, "client_instance_id": "tab-a",
                "seenical_session_id": "session-a", "chatbot_id": "bot-a",
                "chatbot_ids": ["bot-a", "bot-b"],
                "conversation_type": "CHAT", "conversation_id": "22", "im_user_id": "22",
                "runtimes": [{"type": "butler_api", "version": 1}]
            })
        self.assertEqual("ok", result["result"])
        self.assertEqual("butler_api", result["data"]["runtimes"][0]["type"])
        self.assertEqual({"tab-a"}, self.redis.smembers(self.module.capability_index_key(
            "app", "bot-b", "CHAT", "22")))

    def test_binding_projection_rejects_same_revision_with_different_user(self):
        self.bind_app(user_id="22")
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            result = self.module.sync_im_binding_projection("app", {
                "status": "BOUND", "im_user_id": "23", "revision": 1
            })
        self.assertEqual("error", result["result"])
        stored = json.loads(self.redis.get(self.module.im_binding_projection_key("app")))
        self.assertEqual("22", stored["im_user_id"])

    def test_unbound_sender_does_not_receive_skill_or_tools(self):
        self.activate_catalog()
        self.bind_app(user_id="22")
        messages = [{"role": "user", "content": "list plans"}]
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            output_messages, functions = self.module.apply_active_skills(
                "app", {"chatbot_id": "bot", "send_from": "23",
                        "reply_msg_type": "CHAT", "reply_to": "23"}, messages, [])
        self.assertEqual(messages, output_messages)
        self.assertEqual([], functions)

    def test_request_freezes_runtime_and_original_message_context(self):
        catalog = self.activate_catalog()
        self.bind_app()
        skill = catalog["skills"][0]
        capability = {
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "actor_subject_id": "11", "actor_tenement_id": "2",
            "runtimes": {"butler_api": 1}
        }
        config = {
            "chatbot_id": "bot", "reply_msg_type": "CHAT", "reply_to": "22", "send_from": "22",
            "request_msg_id": "9001", "seenical_client_context": {
                "client_instance_id": "tab-a", "seenical_session_id": "session-a"
            }
        }
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "find_capability", return_value=capability), mock.patch.object(
                self.module, "_audit"):
            function = self.module.registry_function("seenical.plan.list")
            function["seenical_builtin_tool"] = True
            result = self.module.create_client_request(
                "app", config, {"id": "call"}, function, {}, {"config": {}})
        self.assertEqual("ok", result["result"])
        request = result["data"]
        self.assertEqual("9001", request["trigger_message_id"])
        self.assertEqual("22", request["trigger_from_user_id"])
        self.assertEqual("session-a", request["seenical_session_id"])
        self.assertEqual("/app/grow_ai/get_task_list", request["request"]["path"])
        self.assertEqual(skill["revision"], request["skill_versions"][0]["revision"])

    def test_client_result_is_field_constrained_and_rejects_credentials(self):
        result = self.module._constrain_client_result({
            "ok": True, "data": {"task_id": "1", "extra": "hidden"}
        }, ["task_id"])
        self.assertEqual({"ok": True, "data": {"task_id": "1"}}, result)
        with self.assertRaises(ValueError):
            self.module._constrain_client_result({"ok": True, "password": "secret"}, ["status"])

    def test_repeated_client_approval_does_not_repeat_business_request(self):
        now = self.module.time.time()
        request = {
            "schema_version": 1, "request_id": "request-a", "app_id": "app",
            "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT",
            "conversation_id": "22", "execution": "butler_api", "risk": "write",
            "runtime": {"type": "butler_api", "version": 1},
            "status": "pending", "expires_at": int(now) + 60,
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"], "conversation_type": "CHAT",
            "conversation_id": "22", "runtimes": {"butler_api": 1},
        }
        actor = {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"}
        self.redis.set(self.module.request_key("request-a"), json.dumps(request))
        self.redis.set(self.module.capability_key("app", "tab-a"), json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "_request_execution_error", return_value=""), mock.patch.object(
                self.module, "_audit"):
            first = self.module._decide_request_locked("app", "request-a", actor, "approve")
            second = self.module._decide_request_locked("app", "request-a", actor, "approve")
        self.assertTrue(first["data"]["execute_allowed"])
        self.assertFalse(second["data"]["execute_allowed"])

    def test_plan_preview_only_contains_business_fields(self):
        with mock.patch.object(
                self.module.lanying_grow_ai, "get_task",
                create=True,
                return_value={"task_id": "1", "prompt": "old", "revision": 3}):
            preview = self.module._preview_tool("app", "seenical.plan.update", {
                "task_id": "1", "expected_revision": 3, "prompt": "new"
            }, {})
        self.assertEqual({"prompt": "old"}, preview["before"])
        self.assertEqual({"prompt": "new"}, preview["after"])

    def test_non_client_functions_keep_legacy_behavior(self):
        functions = [
            {"name": "legacy_http", "function_call": {"type": "http"}},
            {"name": "legacy_system", "function_call": {"type": "system"}},
        ]
        self.assertEqual(functions, self.module.filter_supported_client_functions(
            "app", {"chatbot_id": "1"}, functions))

    def test_public_notification_limits_are_shared(self):
        class Pipeline:
            def __init__(self, values): self.values = values
            def incr(self, key): return self
            def expire(self, key, ttl): return self
            def execute(self): return self.values
        for values, expected in [([10, True, 60, True], True), ([11, True, 1, True], False), ([1, True, 61, True], False)]:
            with mock.patch.object(self.module, "_redis", return_value=types.SimpleNamespace(
                    pipeline=lambda transaction=True, values=values: Pipeline(values))):
                self.assertEqual(expected, self.module._rate_limit_notification("127.0.0.1"))


if __name__ == "__main__":
    unittest.main()
