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
        "lanying_chatbot": types.SimpleNamespace(get_chatbot=lambda app_id, chatbot_id: None),
        "lanying_grow_ai": types.SimpleNamespace(
            ARTICLE_LANGUAGE_VALUES={"auto", "zh-hans", "en"},
            get_task=lambda app_id, task_id: None,
            set_loop_conversation_binding=lambda app_id, task_id, value: {"result": "ok"}),
        "lanying_im_api": types.SimpleNamespace(
            get_group_info=lambda app_id, group_id: {},
            filter_group_member_ids=lambda app_id, group_id, user_ids: []),
        "lanying_agent_tools_storage": types.SimpleNamespace(
            append_agent_tool_audit_log=lambda value: {"result": "ok"},
            save_agent_tool_request_view=lambda value: {"result": "ok"},
            get_agent_tool_request_view=lambda app_id, request_id: None,
            list_seenical_conversation_bindings=lambda app_id: [],
            save_seenical_conversation_binding=lambda value: {"result": "ok"},
            deactivate_seenical_conversation_binding=lambda *args: {"result": "ok"},
            is_feature_enabled=lambda app_id, chatbot_id="": False,
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
        root = pathlib.Path(__file__).resolve().parents[2] / "seenical-skills"
        manifest = (root / ".seenical/manifest.json").read_text()
        paths = sorted(path for path in (root / "skills").rglob("*") if path.is_file())
        files = {
            path.relative_to(root).as_posix(): (path.read_text(), "sha")
            for path in paths
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
        self.assertEqual(3, len(catalog["skills"]))
        self.assertEqual("seenical-api", skill["skill_id"])
        self.assertEqual("知见API", skill["name_zh"])
        self.assertEqual("Seenical API", skill["name_en"])
        self.assertIn("Butler APIs", skill["description_en"])
        self.assertIn("name: seenical-api", skill["skill_markdown"])
        self.assertEqual("butler_api", skill["runtime"]["type"])
        self.assertEqual(set(skill["required_tools"]), {tool["tool_id"] for tool in skill["tools"]})
        self.assertEqual("/app/grow_ai/configure_task",
                         next(tool for tool in skill["tools"] if tool["tool_id"] == "seenical.plan.update")["request"]["path"])
        schedule = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.plan.schedule")
        self.assertEqual(["task_id", "schedule"], schedule["parameters"]["required"])
        domain = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.site.domain.create")
        self.assertEqual("destructive", domain["risk"])
        self.assertEqual(["site_id", "domain_name"], domain["parameters"]["required"])
        domain_check = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.site.domain.check")
        self.assertEqual("write", domain_check["risk"])
        plan_update = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.plan.update")
        title_pool = plan_update["parameters"]["properties"]["keywords"]
        self.assertIn("article-title pool", title_pool["description"])
        self.assertIn("replaces the whole pool", title_pool["description"])
        self.assertIn("Never store a requested title", plan_update["description"])
        self.assertIn("non-recurring plan is run manually again",
                      plan_update["parameters"]["properties"]["title_reuse"]["description"])
        self.assertIn("GitBook navigation/SUMMARY directory",
                      plan_update["parameters"]["properties"]["target_summary_dir"]["description"])
        plan_run = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.plan.run")
        self.assertIn("existing uploaded title file in file_list",
                      plan_run["description"])
        self.assertIn("Never put a requested article title",
                      skill["instructions"])
        self.assertIn("Never treat a choice number", skill["instructions"])
        self.assertIn("action marker such as A/B/C", skill["instructions"])
        self.assertIn("Before running a plan", skill["instructions"])
        plugin_functions = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.plugin.functions.list")
        self.assertEqual(["plugin_id"], plugin_functions["parameters"]["required"])
        knowledge_documents = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.knowledge.documents.list")
        self.assertEqual(["embedding_name"], knowledge_documents["parameters"]["required"])
        plugin_update = next(
            tool for tool in skill["tools"]
            if tool["tool_id"] == "seenical.plugin.update")
        self.assertIn("headers", plugin_update["parameters"]["properties"])
        self.module.validate_tool_arguments(plugin_update, {
            "plugin_id": "plugin-1",
            "headers": {"Authorization": "__MASKED_SENSITIVE_VALUE__"},
            "auth": {"type": "basic", "password": "replacement"},
        })
        self.assertGreater(len(skill["tools"]), 40)
        instruction_skills = {
            item["skill_id"]: item for item in catalog["skills"][1:]
        }
        self.assertEqual(
            {"seenical-console", "seenical-product-onboarding"},
            set(instruction_skills))
        self.assertTrue(all(item["runtime"] is None
                            and item["tools"] == []
                            and item["required_tools"] == []
                            for item in instruction_skills.values()))

    def test_legacy_public_skill_id_remains_active_during_catalog_upgrade(self):
        catalog = self.template_catalog()
        catalog["skills"][0]["skill_id"] = "seenical-console"
        self.redis.set(self.module.PUBLIC_CATALOG_CACHE_KEY, json.dumps(catalog))
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            self.assertEqual("seenical-console", self.module._official_skill()["skill_id"])

    def test_public_skill_detail_exposes_downloadable_markdown_only_in_detail(self):
        catalog = self.template_catalog()
        summary = self.module.public_catalog_view(catalog)
        self.assertNotIn("skill_markdown", summary["skills"][0])
        with mock.patch.object(self.module, "get_public_catalog", return_value=catalog):
            detail = self.module.public_skill_detail("seenical-api")
        self.assertEqual("ok", detail["result"])
        self.assertIn("name: seenical-api", detail["data"]["skill_markdown"])

    def test_runtime_rejects_absolute_urls_headers_and_weakened_risk(self):
        runtime = json.dumps({
            "schema_version": 1, "skill_id": "seenical-console",
            "runtime": {"type": "butler_api", "version": 1, "authentication": "host_console_session"},
            "tools_files": ["references/api/content.json"]
        })
        base = {
            "tool_id": "seenical.test", "version": 1, "function_name": "seenical_test",
            "title": "Test", "description": "Test", "risk": "read",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
            "request": {"method": "GET", "path": "/app/grow_ai/get_task_list", "arguments": "query"},
            "result_fields": ["status"]
        }
        for mutate in [
            lambda tool: tool["request"].update(path="https://evil.example/app/test"),
            lambda tool: tool["request"].update(headers={"Authorization": "x"}),
            lambda tool: tool.update(risk="read", request={"method": "POST", "path": "/app/test", "arguments": "body"}),
            lambda tool: tool["parameters"]["properties"].update(token={"type": "string"}),
            lambda tool: tool.update(result_fields=["access_token"]),
            lambda tool: tool.update(result_fields=["unexpected"]),
        ]:
            tool = json.loads(json.dumps(base))
            mutate(tool)
            with self.subTest(tool=tool), self.assertRaises(ValueError):
                self.module._normalize_butler_runtime(runtime, {
                    "references/api/content.json": json.dumps({
                        "schema_version": 1, "tools": [tool], "definitions": {}
                    })
                }, "seenical-console")

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

    def test_loop_conversation_binding_validates_group_and_members(self):
        self.bind_app(user_id="22")
        stored = []
        group = {"code": 200, "data": {
            "description": json.dumps({"seenical": {
                "scene": "agent_session", "app_id": "app",
                "agent_user_id": 33, "session_id": "session-a",
                "loop_id": ""
            }}),
            "ext": json.dumps({"seenical": {
                "scene": "agent_session", "app_id": "app",
                "agent_user_id": 33, "session_id": "session-a",
                "loop_id": "191"
            }})
        }}
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_grow_ai, "get_task", return_value={
                 "task_id": "191", "chatbot_id": "chatbot-a"
             }), \
             mock.patch.object(self.module.lanying_chatbot, "get_chatbot", return_value={
                 "user_id": 33
             }), \
             mock.patch.object(self.module.lanying_im_api, "get_group_info", return_value=group), \
             mock.patch.object(self.module.lanying_im_api, "filter_group_member_ids", return_value=["22", "33"]), \
             mock.patch.object(self.module.lanying_grow_ai, "set_loop_conversation_binding", side_effect=lambda app_id, task_id, value: (stored.append(value), {"result": "ok"})[1]):
            result = self.module.bind_loop_conversation("app", {
                "im_user_id": "22"
            }, {
                "task_id": "191", "conversation_type": "GROUPCHAT",
                "conversation_id": "1001", "seenical_session_id": "session-a"
            })

        self.assertEqual("ok", result["result"])
        self.assertEqual("1001", stored[0]["conversation_id"])
        self.assertEqual("33", stored[0]["agent_user_id"])

    def test_loop_conversation_binding_rejects_mismatched_metadata(self):
        self.bind_app(user_id="22")
        group = {"code": 200, "data": {"description": json.dumps({
            "seenical": {
                "scene": "agent_session", "app_id": "app",
                "agent_user_id": 33, "session_id": "another-session",
                "loop_id": "191"
            }
        })}}
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_grow_ai, "get_task", return_value={
                 "task_id": "191", "chatbot_id": "chatbot-a"
             }), \
             mock.patch.object(self.module.lanying_chatbot, "get_chatbot", return_value={
                 "user_id": 33
             }), \
             mock.patch.object(self.module.lanying_im_api, "get_group_info", return_value=group), \
             mock.patch.object(self.module.lanying_grow_ai, "set_loop_conversation_binding") as save:
            result = self.module.bind_loop_conversation("app", {
                "im_user_id": "22"
            }, {
                "task_id": "191", "conversation_type": "GROUPCHAT",
                "conversation_id": "1001", "seenical_session_id": "session-a"
            })

        self.assertEqual("error", result["result"])
        save.assert_not_called()

    def test_register_seenical_conversation_persists_verified_group(self):
        self.bind_app(user_id="22")
        group = {"code": 200, "data": {"name": "Child", "ext": json.dumps({
            "seenical": {
                "scene": "agent_session", "app_id": "app",
                "agent_id": "chatbot-a", "agent_user_id": 33,
                "session_id": "session-a", "loop_id": ""
            }
        })}}
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_chatbot, "get_chatbot", return_value={"user_id": 33}), \
             mock.patch.object(self.module.lanying_im_api, "get_group_info", return_value=group), \
             mock.patch.object(self.module.lanying_im_api, "filter_group_member_ids", return_value=["22", "33"]), \
             mock.patch.object(self.module.lanying_agent_tools_storage, "save_seenical_conversation_binding", return_value={"result": "ok"}) as save:
            result = self.module.register_seenical_conversation("app", {
                "im_user_id": "22"
            }, {
                "chatbot_id": "chatbot-a", "conversation_type": "GROUPCHAT",
                "conversation_id": "1001", "seenical_session_id": "session-a"
            })

        self.assertEqual("ok", result["result"])
        self.assertEqual("Child", save.call_args.args[0]["conversation_name"])
        self.assertEqual("", save.call_args.args[0]["task_id"])

    def test_unregister_seenical_conversation_uses_bound_actor_and_exact_target(self):
        self.bind_app(user_id="22")
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_agent_tools_storage,
                               "deactivate_seenical_conversation_binding",
                               return_value={"result": "ok"}) as deactivate:
            result = self.module.unregister_seenical_conversation("app", {
                "im_user_id": "22"
            }, {
                "conversation_type": "GROUPCHAT", "conversation_id": "1001",
                "seenical_session_id": "session-a"
            })
        self.assertEqual("ok", result["result"])
        deactivate.assert_called_once_with(
            "app", "session-a", "GROUPCHAT", "1001", "22")

    def test_unregister_seenical_conversation_rejects_unbound_actor(self):
        self.bind_app(user_id="22")
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_agent_tools_storage,
                               "deactivate_seenical_conversation_binding") as deactivate:
            result = self.module.unregister_seenical_conversation("app", {
                "im_user_id": "23"
            }, {
                "conversation_type": "GROUPCHAT", "conversation_id": "1001",
                "seenical_session_id": "session-a"
            })
        self.assertEqual("error", result["result"])
        deactivate.assert_not_called()

    def test_unregister_seenical_conversation_rejects_existing_bound_loop(self):
        self.bind_app(user_id="22")
        stored = [{
            "seenical_session_id": "session-a",
            "conversation_type": "GROUPCHAT", "conversation_id": "1001",
            "bound_im_user_id": "22", "task_id": "191",
        }]
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_agent_tools_storage,
                               "list_seenical_conversation_bindings",
                               return_value=stored), \
             mock.patch.object(self.module.lanying_grow_ai, "get_task",
                               return_value={"task_id": "191"}), \
             mock.patch.object(self.module.lanying_agent_tools_storage,
                               "deactivate_seenical_conversation_binding") as deactivate:
            result = self.module.unregister_seenical_conversation("app", {
                "im_user_id": "22"
            }, {
                "conversation_type": "GROUPCHAT", "conversation_id": "1001",
                "seenical_session_id": "session-a"
            })
        self.assertEqual("error", result["result"])
        self.assertEqual("Seenical conversation has a bound LOOP",
                         result["message"])
        deactivate.assert_not_called()

    def test_conversation_list_reports_storage_unavailable(self):
        self.bind_app(user_id="22")
        with mock.patch.object(self.module, "_redis", return_value=self.redis), \
             mock.patch.object(self.module.lanying_agent_tools_storage,
                               "list_seenical_conversation_bindings",
                               side_effect=RuntimeError("MySQL disabled")):
            result = self.module.list_seenical_conversations("app", {
                "im_user_id": "22"
            })
        self.assertEqual("error", result["result"])
        self.assertEqual("Seenical conversation storage unavailable",
                         result["message"])

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

    def test_bound_sender_without_matching_capability_does_not_receive_skill(self):
        self.activate_catalog()
        self.bind_app(user_id="22")
        messages = [{"role": "user", "content": "list plans"}]
        config = {
            "chatbot_id": "bot", "send_from": "22", "reply_msg_type": "CHAT",
            "reply_to": "22", "seenical_client_context": {
                "schema_version": 1, "client_instance_id": "tab-a",
                "seenical_session_id": "session-a"
            }
        }
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "is_feature_enabled", return_value=True):
            output_messages, functions = self.module.apply_active_skills(
                "app", config, messages, [])
        self.assertEqual(messages, output_messages)
        self.assertEqual([], functions)

    def test_catalog_storage_failure_disables_skill_for_current_message(self):
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module.lanying_agent_tools_storage,
                "get_active_public_skill_catalog", side_effect=RuntimeError("mysql down")), mock.patch.object(
                self.module.logging, "exception"):
            self.assertIsNone(self.module.get_public_catalog())

    def test_matching_seenical_capability_loads_complete_official_skill(self):
        catalog = self.activate_catalog()
        self.bind_app(user_id="22")
        capability = {
            "app_id": "app", "im_user_id": "22", "chatbot_ids": ["bot"],
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1}, "updated_at": 1
        }
        self.redis.set(self.module.capability_key("app", "tab-a"), json.dumps(capability))
        self.redis.sadd(self.module.capability_index_key("app", "bot", "CHAT", "22"), "tab-a")
        messages = [{"role": "user", "content": "list plans"}]
        config = {
            "chatbot_id": "bot", "send_from": "22", "reply_msg_type": "CHAT",
            "reply_to": "22", "seenical_client_context": {
                "schema_version": 1, "client_instance_id": "tab-a",
                "seenical_session_id": "session-a"
            }
        }
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "is_feature_enabled", return_value=True):
            output_messages, functions = self.module.apply_active_skills(
                "app", config, messages, [])
        self.assertEqual(len(catalog["skills"][0]["tools"]), len(functions))
        self.assertIn("Seenical Skill", output_messages[0]["content"])
        plan_list_description = next(
            item for item in functions
            if item["name"] == "seenical_plan_list")["description"]
        self.assertIn("prompt is the plan topic", plan_list_description)
        self.assertIn("article_prompt is the per-article instruction",
                      plan_list_description)
        self.assertIn("never substitute prompt or note", plan_list_description)

    def test_verified_workspace_context_is_injected_as_reference_data(self):
        self.activate_catalog()
        self.bind_app(user_id="22")
        capability = {
            "app_id": "app", "im_user_id": "22", "chatbot_ids": ["bot"],
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1}, "updated_at": 1
        }
        self.redis.set(self.module.capability_key("app", "tab-a"), json.dumps(capability))
        self.redis.sadd(self.module.capability_index_key("app", "bot", "CHAT", "22"), "tab-a")
        config = {
            "chatbot_id": "bot", "send_from": "22", "reply_msg_type": "CHAT",
            "reply_to": "22", "seenical_client_context": {
                "schema_version": 1, "client_instance_id": "tab-a",
                "seenical_session_id": "session-a", "workspace_context": {
                    "task_id": "task-a", "site_id": "site-a",
                    "task_run_id": "run-a", "preview_id": "preview-a"
                }
            }
        }
        grow_ai = self.module.lanying_grow_ai
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "is_feature_enabled", return_value=True), mock.patch.object(
                self.module.lanying_chatbot, "get_chatbot", create=True,
                return_value={"name": "Writer"}), mock.patch.object(
                grow_ai, "get_task", create=True,
                return_value={"task_id": "task-a", "chatbot_id": "bot", "name": "News", "schedule": "off", "article_language": "en"}), mock.patch.object(
                grow_ai, "get_site", create=True,
                return_value={"site_id": "site-a", "name": "Docs", "language": "en"}), mock.patch.object(
                grow_ai, "get_task_run", create=True,
                return_value={"task_run_id": "run-a", "task_id": "task-a", "status": "success"}), mock.patch.object(
                grow_ai, "get_preview", create=True,
                return_value={"preview_id": "preview-a", "task_run_id": "run-a", "site_id": "site-a", "status": "ready"}):
            messages, _ = self.module.apply_active_skills(
                "app", config, [{"role": "user", "content": "update this plan"}], [])
        workspace_messages = [item for item in messages if "verified workspace context" in item.get("content", "")]
        self.assertEqual(1, len(workspace_messages))
        self.assertIn('"task_id":"task-a"', workspace_messages[0]["content"])
        self.assertEqual("task-a", config["seenical_verified_workspace_context"]["task"]["task_id"])

    def test_workspace_context_omits_wrong_agent_plan_and_mismatched_run(self):
        config = {
            "chatbot_id": "bot", "reply_msg_type": "CHAT", "reply_to": "22",
            "seenical_client_context": {
                "seenical_session_id": "session-a", "workspace_context": {
                    "task_id": "task-a", "task_run_id": "run-a"
                }
            }
        }
        with mock.patch.object(
                self.module.lanying_chatbot, "get_chatbot", create=True,
                return_value={"name": "Writer"}), mock.patch.object(
                self.module.lanying_grow_ai, "get_task", create=True,
                return_value={"task_id": "task-a", "chatbot_id": "other"}), mock.patch.object(
                self.module.lanying_grow_ai, "get_task_run", create=True,
                return_value={"task_run_id": "run-a", "task_id": "other-task"}):
            context = self.module._verified_workspace_context("app", config)
        self.assertNotIn("task", context)
        self.assertNotIn("task_run", context)

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
        self.assertEqual(
            self.module.REQUEST_TTL_SECONDS,
            request["expires_at"] - request["created_at"])

    def test_client_result_is_field_constrained_and_rejects_credentials(self):
        result = self.module._constrain_client_result({
            "ok": True, "data": {"task_id": "1", "extra": "hidden"}
        }, ["task_id"])
        self.assertEqual({"ok": True, "data": {"task_id": "1"}}, result)
        redacted = self.module._constrain_client_result({
            "ok": True, "data": {"list": [{"name": "plugin", "headers": {"Authorization": "secret"}}]}
        }, ["list"])
        self.assertEqual({"ok": True, "data": {"list": [{"name": "plugin"}]}}, redacted)

    def test_client_result_redacts_secret_field_name_variants(self):
        result = self.module._constrain_client_result({
            "ok": True,
            "data": {
                "apiKey": "top-secret",
                "resource": {
                    "name": "Plan",
                    "temporary_password": "nested-secret",
                },
            },
        }, ["apiKey", "resource"])
        self.assertEqual({
            "ok": True,
            "data": {"resource": {"name": "Plan"}},
        }, result)

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

    def test_same_console_user_can_resume_request_after_client_refresh(self):
        request = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-before-refresh", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "runtime": {"type": "butler_api", "version": 1},
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-after-refresh", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"],
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1},
        }
        actor = {
            "subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-after-refresh",
        }
        self.redis.set(
            self.module.capability_key("app", "tab-after-refresh"),
            json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            self.assertEqual("", self.module._request_actor_error(request, actor))

    def test_same_console_user_can_resume_request_after_browser_session_restore(self):
        request = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-a", "seenical_session_id": "local-session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "runtime": {"type": "butler_api", "version": 1},
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-b", "seenical_session_id": "restored-session-b",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"],
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1},
        }
        actor = {
            "subject_id": "11", "im_user_id": "22", "client_instance_id": "browser-b",
        }
        self.redis.set(
            self.module.capability_key("app", "browser-b"),
            json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            self.assertEqual("", self.module._request_actor_error(request, actor))

    def test_expired_request_uses_mysql_display_snapshot(self):
        now = int(self.module.time.time())
        snapshot = {
            "schema_version": 1, "request_id": "request-old", "app_id": "app",
            "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-a", "seenical_session_id": "old-session",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "runtime": {"type": "butler_api", "version": 1},
            "tool_id": "seenical.plan.list", "tool_name": "List plans",
            "execution": "butler_api", "risk": "read", "status": "pending",
            "created_at": now - 9000, "expires_at": now - 1800,
            "trigger_message_id": "message-a",
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-b", "seenical_session_id": "new-session",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"],
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1},
        }
        actor = {
            "subject_id": "11", "im_user_id": "22", "client_instance_id": "browser-b",
        }
        self.redis.set(
            self.module.capability_key("app", "browser-b"), json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module.lanying_agent_tools_storage, "get_agent_tool_request_view",
                return_value=snapshot):
            result = self.module.get_request_for_actor("app", "request-old", actor)
        self.assertEqual("ok", result["result"])
        self.assertEqual("expired", result["data"]["status"])

    def test_mysql_display_snapshot_excludes_execution_context_and_credentials(self):
        snapshot = self.module._request_view_snapshot({
            "request_id": "request-a", "app_id": "app", "status": "pending",
            "expires_at": 1700000000,
            "arguments": {
                "name": "Plan", "token": "secret", "apiKey": "secret",
            },
            "preview": {"after": {
                "name": "Plan", "temporary_password": "secret",
                "url": "https://example.com/file?X-Amz-Signature=secret#access_token=secret",
            }},
            "continuation": {"preset": {"messages": ["private"]}},
            "tool_call": {"id": "call-a"},
            "resume_message": "provider diagnostic",
        })
        self.assertNotIn("continuation", snapshot)
        self.assertNotIn("tool_call", snapshot)
        self.assertNotIn("resume_message", snapshot)
        self.assertNotIn("token", snapshot["arguments"])
        self.assertNotIn("apiKey", snapshot["arguments"])
        self.assertNotIn("temporary_password", snapshot["preview"]["after"])
        self.assertEqual(
            "https://example.com/file",
            snapshot["preview"]["after"]["url"])

    def test_mysql_display_snapshot_removes_url_userinfo(self):
        snapshot = self.module._request_view_snapshot({
            "request_id": "request-a", "app_id": "app", "status": "pending",
            "expires_at": 1700000000,
            "arguments": {
                "url": "https://user:password@example.com:8443/file?mode=read#section",
            },
        })
        self.assertEqual(
            "https://example.com:8443/file",
            snapshot["arguments"]["url"])

    def test_mysql_display_snapshot_keeps_malformed_text(self):
        snapshot = self.module._request_view_snapshot({
            "request_id": "request-a", "app_id": "app", "status": "pending",
            "expires_at": 1700000000,
            "arguments": {"article_prompt": "Explain http://[ as plain text"},
        })
        self.assertEqual(
            "Explain http://[ as plain text",
            snapshot["arguments"]["article_prompt"])

    def test_callback_endpoint_rejects_query_credentials_and_fragments(self):
        for endpoint in [
                "https://api.example.com/callback?token=secret",
                "https://api.example.com/callback?mode=test",
                "https://api.example.com/callback#credential"]:
            with self.assertRaisesRegex(ValueError, "callback endpoint"):
                self.module._validate_public_url(
                    endpoint, https_only=True, allow_query=False)

    def test_public_url_rejects_sensitive_query_parameter_variants(self):
        for url in [
                "https://example.com/page?access_token=secret",
                "https://example.com/page?apiKey=secret",
                "https://example.com/page?temporary-password=secret"]:
            with self.assertRaisesRegex(ValueError, "credential query"):
                self.module._validate_public_url(url)

    def test_visible_request_still_requires_the_same_im_conversation(self):
        request = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-a", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "GROUPCHAT", "conversation_id": "group-a",
            "runtime": {"type": "butler_api", "version": 1},
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "browser-b", "seenical_session_id": "session-b",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"],
            "conversation_type": "GROUPCHAT", "conversation_id": "group-b",
            "runtimes": {"butler_api": 1},
        }
        actor = {
            "subject_id": "11", "im_user_id": "22", "client_instance_id": "browser-b",
        }
        self.redis.set(
            self.module.capability_key("app", "browser-b"),
            json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            self.assertEqual(
                "tool request client capability is no longer valid",
                self.module._request_actor_error(request, actor))

    def test_another_console_user_cannot_resume_visible_request(self):
        request = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "runtime": {"type": "butler_api", "version": 1},
        }
        actor = {
            "subject_id": "12", "im_user_id": "22", "client_instance_id": "tab-b",
        }
        self.assertEqual(
            "tool request does not belong to current user",
            self.module._request_actor_error(request, actor))

    def test_read_request_can_be_reclaimed_after_client_refresh(self):
        now = self.module.time.time()
        request = {
            "schema_version": 1, "request_id": "request-read", "app_id": "app",
            "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-before-refresh", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "execution": "butler_api", "risk": "read",
            "runtime": {"type": "butler_api", "version": 1},
            "status": "awaiting_client_result", "expires_at": int(now) + 60,
        }
        capability = {
            "app_id": "app", "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-after-refresh", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "chatbot_ids": ["bot-a"],
            "conversation_type": "CHAT", "conversation_id": "22",
            "runtimes": {"butler_api": 1},
        }
        actor = {
            "subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-after-refresh",
        }
        self.redis.set(self.module.request_key("request-read"), json.dumps(request))
        self.redis.set(
            self.module.capability_key("app", "tab-after-refresh"),
            json.dumps(capability))
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "_request_execution_error", return_value=""), mock.patch.object(
                self.module, "_audit"):
            result = self.module._decide_request_locked(
                "app", "request-read", actor, "approve")
        self.assertTrue(result["data"]["execute_allowed"])

    def test_plan_preview_only_contains_business_fields(self):
        with mock.patch.object(
                self.module.lanying_grow_ai, "get_task",
                create=True,
                return_value={"task_id": "1", "prompt": "old", "revision": 3}):
            preview = self.module._preview_tool("app", "seenical.plan.update", {
                "task_id": "1", "prompt": "new"
            }, {})
        self.assertEqual({"prompt": "old"}, preview["before"])
        self.assertEqual({"prompt": "new"}, preview["after"])

    def test_pending_plan_request_can_retarget_and_refreeze_arguments(self):
        self.activate_catalog()
        now = int(self.module.time.time())
        request = {
            "schema_version": 1, "request_id": "request-target", "app_id": "app",
            "actor_subject_id": "11", "im_user_id": "22",
            "client_instance_id": "tab-a", "seenical_session_id": "session-a",
            "chatbot_id": "bot-a", "conversation_type": "CHAT", "conversation_id": "22",
            "tool_id": "seenical.plan.update", "tool_version": 1,
            "execution": "butler_api", "risk": "write",
            "runtime": {"type": "butler_api", "version": 1},
            "arguments": {"task_id": "old", "article_language": "en"},
            "arguments_hash": "old-hash",
            "tool_call": {"id": "call-a", "type": "function", "function": {
                "name": "seenical_plan_update", "arguments": "{}"
            }},
            "status": "pending", "expires_at": now + 60,
        }
        actor = {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"}
        self.redis.set(self.module.request_key("request-target"), json.dumps(request))
        tasks = [
            {"task_id": "new", "name": "New plan", "chatbot_id": "bot-b", "schedule": "off"},
            {"task_id": "old", "name": "Old plan", "chatbot_id": "bot-a", "schedule": "on"},
        ]
        def get_task(app_id, task_id):
            return next((item for item in tasks if item["task_id"] == task_id), None)
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "_request_actor_error", return_value=""), mock.patch.object(
                self.module, "_audit") as audit, mock.patch.object(
                self.module.lanying_grow_ai, "get_task", create=True,
                side_effect=get_task), mock.patch.object(
                self.module.lanying_grow_ai, "get_task_list", create=True,
                return_value={"result": "ok", "data": {"list": tasks}}), mock.patch.object(
                self.module.lanying_chatbot, "get_chatbot", create=True,
                side_effect=lambda app_id, chatbot_id: {"name": "Current" if chatbot_id == "bot-a" else "Other"}):
            result = self.module.retarget_request(
                "app", "request-target", actor, "new")
        self.assertEqual("ok", result["result"])
        updated = json.loads(self.redis.get(self.module.request_key("request-target")))
        self.assertEqual({"task_id": "new", "article_language": "en"}, updated["arguments"])
        self.assertEqual(updated["arguments"], json.loads(updated["tool_call"]["function"]["arguments"]))
        self.assertEqual({"article_language": None}, updated["preview"]["before"])
        self.assertEqual("new", result["data"]["target_selector"]["selected_id"])
        self.assertTrue(result["data"]["target_selector"]["options"][0]["current_agent"])
        audit.assert_called_once()

    def test_non_pending_or_unknown_plan_request_cannot_retarget(self):
        now = int(self.module.time.time())
        base = {
            "request_id": "request-target", "app_id": "app", "tool_id": "seenical.plan.update",
            "status": "completed", "expires_at": now + 60,
        }
        actor = {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"}
        self.redis.set(self.module.request_key("request-target"), json.dumps(base))
        with mock.patch.object(self.module, "_redis", return_value=self.redis), mock.patch.object(
                self.module, "_request_actor_error", return_value=""):
            result = self.module.retarget_request("app", "request-target", actor, "new")
        self.assertEqual("error", result["result"])
        self.assertIn("no longer", result["message"])

    def test_target_change_and_approval_share_one_request_lock(self):
        request_id = "request-locked"
        self.redis.set(
            f"lanying_connector:agent_tools:decision_lock:{request_id}",
            "approving")
        with mock.patch.object(self.module, "_redis", return_value=self.redis):
            result = self.module.retarget_request(
                "app", request_id,
                {"subject_id": "11", "im_user_id": "22", "client_instance_id": "tab-a"},
                "new")
        self.assertEqual("error", result["result"])
        self.assertIn("being updated", result["message"])

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
