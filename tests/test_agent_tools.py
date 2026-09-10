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
        "lanying_async": types.SimpleNamespace(executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None)),
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


class AgentToolsTest(unittest.TestCase):
    def setUp(self):
        self.module = load_agent_tools()

    def test_client_function_requires_registry_skill_plugin_and_online_client(self):
        function = {
            "name": "repository_controlled_name",
            "doc_id": "doc-1",
            "description": "untrusted description",
            "parameters": {"type": "object", "properties": {"secret": {"type": "string"}}},
            "function_call": {
                "type": "client", "tool_id": "seenical.plan.update",
                "execution": "local_action", "risk": "read"
            },
        }
        config = {"chatbot_id": "chatbot-1"}
        with mock.patch.object(
                self.module, "_active_skill_authorizations",
                return_value=[{"skill_id": "writer", "revision": "rev"}]), mock.patch.object(
                self.module, "_bound_plugin_id", return_value="plugin-1"), mock.patch.object(
                self.module, "find_capability", return_value={"client_instance_id": "tab-1"}):
            result = self.module.filter_supported_client_functions("app", config, [function])

        self.assertEqual(1, len(result))
        self.assertEqual("plugin-1", result[0]["seenical_plugin_id"])
        self.assertEqual("seenical_plan_update", result[0]["name"])
        self.assertEqual("修改生成计划", result[0]["description"])
        self.assertNotIn("secret", result[0]["parameters"]["properties"])
        self.assertEqual("console_action", result[0]["function_call"]["execution"])
        self.assertEqual("write", result[0]["function_call"]["risk"])

        for patch_name, patch_value in [
                ("_active_skill_authorizations", []),
                ("_bound_plugin_id", ""),
                ("find_capability", None)]:
            defaults = {
                "_active_skill_authorizations": [{"skill_id": "writer", "revision": "rev"}],
                "_bound_plugin_id": "plugin-1",
                "find_capability": {"client_instance_id": "tab-1"},
            }
            defaults[patch_name] = patch_value
            with mock.patch.object(self.module, "_active_skill_authorizations", return_value=defaults["_active_skill_authorizations"]), mock.patch.object(
                    self.module, "_bound_plugin_id", return_value=defaults["_bound_plugin_id"]), mock.patch.object(
                    self.module, "find_capability", return_value=defaults["find_capability"]):
                self.assertEqual([], self.module.filter_supported_client_functions("app", config, [function]))

    def test_non_client_functions_keep_legacy_behavior(self):
        functions = [
            {"name": "legacy_http", "function_call": {"type": "http"}},
            {"name": "legacy_system", "function_call": {"type": "system"}},
        ]
        self.assertEqual(
            functions,
            self.module.filter_supported_client_functions("app", {"chatbot_id": "1"}, functions),
        )

    def test_repository_paths_reject_absolute_and_traversal_values(self):
        for value in ["/etc/passwd", "../SKILL.md", "skills/../../secret", "skills\\secret"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.module._validate_repo_path(value)
        self.assertEqual(".seenical/skills/writer", self.module._validate_repo_path(".seenical/skills/writer/"))

    def test_public_request_never_returns_continuation_or_integrity_fields(self):
        value = {
            "request_id": "abc", "arguments": {"name": "safe"},
            "continuation": {"config": {"access_token": "secret"}},
            "tool_call": {"id": "call"}, "actor_tenement_id": "2",
            "arguments_hash": "hash",
        }
        result = self.module.public_request(value)
        self.assertEqual({"request_id": "abc", "arguments": {"name": "safe"}}, result)

    def test_actor_check_binds_console_im_user_and_browser_instance(self):
        request_info = {
            "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a",
        }
        self.assertEqual("", self.module._request_actor_error(request_info, {
            "subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"
        }))
        self.assertIn("IM user", self.module._request_actor_error(request_info, {
            "subject_id": "11", "im_user_id": "23", "client_instance_id": "tab-a"
        }))
        with mock.patch.object(self.module, "_redis", return_value=types.SimpleNamespace(get=lambda key: None)):
            self.assertIn("client instance", self.module._request_actor_error(request_info, {
                "subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-b"
            }))
        request_info.update({
            "app_id": "app", "chatbot_id": "bot", "conversation_type": "CHAT",
            "conversation_id": "22", "tool_id": "seenical.plan.list"
        })
        alternate = dict(request_info, client_instance_id="tab-b", tools={"seenical.plan.list": 1})
        with mock.patch.object(
                self.module, "_redis",
                return_value=types.SimpleNamespace(get=lambda key: json.dumps(alternate))):
            self.assertEqual("", self.module._request_actor_error(request_info, {
                "subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-b"
            }))

    def test_skill_markdown_rejects_executable_frontmatter(self):
        with mock.patch.object(self.module.yaml, "safe_load", return_value={"handler": "shell"}):
            with self.assertRaises(ValueError):
                self.module._parse_skill_markdown("---\nhandler: shell\n---\nDo work")

    def test_public_catalog_uses_fixed_config_and_validates_skill_path(self):
        manifest = json.dumps({
            "schema_version": 1,
            "skills": [{
                "skill_id": "writer",
                "name": "Writer",
                "description": "Writes content",
                "path": ".seenical/skills/writer",
                "tools": [{"id": "seenical.plan.list", "min_version": 1}],
                "scopes": ["plans:read"],
            }],
        })
        config = {
            "repository_url": "https://github.com/seenical/skills",
            "owner": "seenical", "repo": "skills", "ref": "main",
            "manifest_path": ".seenical/manifest.json",
        }
        with mock.patch.object(
                self.module, "_github_file",
                return_value=("# Instructions\nUse the plan list tool.", "file-sha")):
            catalog = self.module._normalize_public_skill_catalog(
                config, "a" * 40, manifest, "manifest-sha")
        self.assertEqual("writer", catalog["skills"][0]["skill_id"])
        self.assertEqual(["seenical.plan.list"], catalog["skills"][0]["required_tools"])
        self.assertNotIn("handler", catalog["skills"][0])

        bad = json.loads(manifest)
        bad["skills"][0]["path"] = ".seenical/skills/another"
        with self.assertRaises(ValueError):
            self.module._normalize_public_skill_catalog(
                config, "a" * 40, json.dumps(bad), "manifest-sha")

    def test_public_catalog_rejects_unlisted_repository_files(self):
        manifest = json.dumps({
            "schema_version": 1,
            "skills": [{
                "skill_id": "writer", "name": "Writer",
                "description": "Writes content",
                "path": ".seenical/skills/writer",
                "tools": [], "scopes": [],
            }],
        })
        config = {
            "repository_url": "https://github.com/seenical/skills",
            "owner": "seenical", "repo": "skills", "ref": "main",
            "manifest_path": ".seenical/manifest.json",
        }
        with mock.patch.object(
                self.module, "_github_file",
                return_value=("Use the configured tools.", "file-sha")):
            with self.assertRaisesRegex(ValueError, "unsupported files"):
                self.module._normalize_public_skill_catalog(
                    config, "a" * 40, manifest, "manifest-sha", {
                        ".seenical/manifest.json",
                        ".seenical/skills/writer/SKILL.md",
                        ".seenical/skills/writer/run.sh",
                    })

    def test_authorization_projection_is_app_and_agent_scoped(self):
        module = self.module

        class FakeRedis:
            def __init__(inner_self):
                inner_self.values = {}

            def get(inner_self, key):
                return inner_self.values.get(key)

            def set(inner_self, key, value, **kwargs):
                inner_self.values[key] = value
                return True

        fake = FakeRedis()
        skill = {
            "skill_id": "writer", "revision": "rev-1",
            "required_tools": ["seenical.plan.list"], "instructions": "Use it"
        }
        with mock.patch.object(module, "_redis", return_value=fake), mock.patch.object(
                module, "get_public_skill_revision", return_value=skill):
            result = module.sync_authorization_projection("app-a", {
                "enabled": True, "authorization_revision": 4,
                "skills": [{"skill_id": "writer", "revision": "rev-1",
                            "chatbot_ids": ["bot-a"]}],
            })
            self.assertEqual("ok", result["result"])
            self.assertEqual(1, len(module.get_active_skills("app-a", "bot-a")))
            self.assertEqual([], module.get_active_skills("app-a", "bot-b"))
            self.assertEqual([], module.get_active_skills("app-b", "bot-a"))

    def test_public_catalog_notification_has_ip_and_global_limits(self):
        class FakePipeline:
            def __init__(self, values):
                self.values = values

            def incr(self, key):
                return self

            def expire(self, key, ttl):
                return self

            def execute(self):
                return self.values

        for values, expected in [
                ([10, True, 60, True], True),
                ([11, True, 1, True], False),
                ([1, True, 61, True], False)]:
            redis = types.SimpleNamespace(
                pipeline=lambda transaction=True, values=values: FakePipeline(values))
            with mock.patch.object(self.module, "_redis", return_value=redis):
                self.assertEqual(expected, self.module._rate_limit_notification("127.0.0.1"))

    def test_tool_arguments_are_strictly_validated_before_persistence(self):
        tool = self.module.TOOL_REGISTRY["seenical.plan.schedule"]
        self.module.validate_tool_arguments(tool, {
            "task_id": "task-1", "schedule": "off", "expected_revision": 2
        })
        for arguments in [
                {"task_id": "task-1", "schedule": "sometimes", "expected_revision": 2},
                {"task_id": "task-1", "schedule": "off", "expected_revision": True},
                {"task_id": "task-1", "schedule": "off", "expected_revision": 2,
                 "access_token": "must-not-be-stored"},
                {"task_id": "task-1", "schedule": "off", "expected_revision": 2,
                 "unexpected": "value"}]:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                self.module.validate_tool_arguments(tool, arguments)

    def test_execution_result_hides_task_attachments_and_tokens(self):
        value = {
            "result": "error", "code": "revision_conflict",
            "data": {"task": {
                "task_id": "task-1", "site_cdn_token": "secret",
                "file_list": [{"url": "https://files.example/private"}],
                "deploy": {"type": "github", "token": "secret"}
            }}
        }
        result = self.module._safe_execution_result(value)
        task = result["data"]["task"]
        self.assertNotIn("site_cdn_token", task)
        self.assertNotIn("file_list", task)
        self.assertEqual(1, task["attachment_count"])
        self.assertNotIn("token", task["deploy"])

    def test_repeated_approval_only_retries_failed_model_resume_once(self):
        request_id = "a" * 32
        request_info = {
            "request_id": request_id, "app_id": "app", "status": "completed",
            "expires_at": 4102444800, "resume_status": "failed",
            "actor_subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a",
        }
        final_result = {"status": "completed", "execution": {"result": "ok"}}
        module = self.module

        class FakeRedis:
            def __init__(self):
                self.values = {
                    module.request_key(request_id): json.dumps(request_info),
                    module.result_key(request_id): json.dumps(final_result),
                }

            def get(inner_self, key):
                return inner_self.values.get(key)

            def set(inner_self, key, value, ex=None, nx=False):
                if nx and key in inner_self.values:
                    return False
                inner_self.values[key] = value
                return True

            def setex(inner_self, key, ttl, value):
                inner_self.values[key] = value

        fake = FakeRedis()
        actor = {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"}
        with mock.patch.object(self.module, "_redis", return_value=fake), mock.patch.object(
                self.module, "_audit"):
            first = self.module.decide_request("app", request_id, actor, "approve")
            second = self.module.decide_request("app", request_id, actor, "approve")

        self.assertTrue(first["resume"])
        self.assertFalse(second["resume"])
        self.assertEqual("completed", second["data"]["status"])

    def test_terminal_request_uses_result_retention_and_remains_readable(self):
        request_id = "b" * 32
        request_info = {
            "request_id": request_id, "app_id": "app", "status": "completed",
            "expires_at": 1, "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a",
        }

        class FakeRedis:
            def __init__(inner_self):
                inner_self.values = {self.module.request_key(request_id): json.dumps(request_info)}
                inner_self.ttl = None

            def get(inner_self, key):
                return inner_self.values.get(key)

            def setex(inner_self, key, ttl, value):
                inner_self.values[key] = value
                inner_self.ttl = ttl

        fake = FakeRedis()
        actor = {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"}
        with mock.patch.object(self.module, "_redis", return_value=fake):
            self.module._store_request(request_info)
            result = self.module.get_request_for_actor("app", request_id, actor)

        self.assertEqual(self.module.RESULT_TTL_SECONDS, fake.ttl)
        self.assertEqual("ok", result["result"])


if __name__ == "__main__":
    unittest.main()
