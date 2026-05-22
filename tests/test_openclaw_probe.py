import importlib.util
import json
import hashlib
import pathlib
import sys
import types
import unittest
from unittest import mock


def _safe_json_loads(raw, default=None):
    if not isinstance(raw, str):
        return default if default is not None else {}
    try:
        return json.loads(raw)
    except Exception:
        return default if default is not None else {}


def _load_lanying_openclaw():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw.py"
    module_name = "lanying_openclaw_probe_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: object(),
            redis_hget=lambda *args, **kwargs: "",
            redis_get=lambda *args, **kwargs: None,
            redis_hgetall=lambda *args, **kwargs: {},
        ),
        "lanying_config": types.SimpleNamespace(
            get_lanying_admin_token=lambda app_id: "admin-token",
            get_lanying_api_endpoint=lambda app_id: "https://api.example.com",
            get_lanying_connector=lambda app_id: {"access_token": "connector-token"},
        ),
        "lanying_chatbot": types.SimpleNamespace(
            get_chatbot=lambda app_id, chatbot_id: {"name": "Probe Bot", "preset": {"messages": [{"role": "system", "content": "hello"}]}},
        ),
        "lanying_im_api": types.SimpleNamespace(
            send_message_sync=lambda *args, **kwargs: 1,
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=_safe_json_loads,
        ),
        "lanying_vendor": types.SimpleNamespace(
            list_models=lambda app_id: [],
        ),
        "lanying_pgvector": types.SimpleNamespace(),
        "requests": types.SimpleNamespace(),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_openclaw = _load_lanying_openclaw()


class OpenClawProbeTests(unittest.TestCase):
    def test_build_probe_value_hash_distinguishes_missing_and_null(self):
        m = lanying_openclaw
        self.assertNotEqual(
            m.build_probe_value_hash(None, True),
            m.build_probe_value_hash(None, False),
        )

    def test_build_probe_value_hash_normalizes_provider_model_lists_by_id(self):
        m = lanying_openclaw
        path = "models.providers.lanying.models"
        left = [
            {"id": "lanying/openai/gpt-5-mini", "name": "GPT 5 Mini", "maxTokens": 8192},
            {"id": "lanying/volcengine/DeepSeek-R1", "reasoning": True},
        ]
        right = [
            {"id": "lanying/volcengine/DeepSeek-R1", "name": "DeepSeek", "maxTokens": 32000},
            {"id": "lanying/openai/gpt-5-mini", "reasoning": False},
        ]

        self.assertEqual(
            m.build_probe_value_hash(left, True, path),
            m.build_probe_value_hash(right, True, path),
        )

    def test_build_default_probe_checks_uses_per_key_hash_and_prompt_hash(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "session_map_sync": "on",
            "merge_sub_sessions": "off",
        }
        patch_config = {
            "agents": {
                "defaults": {
                    "model": {
                        "primary": "lanying/openai/gpt-5-mini",
                    }
                }
            }
        }

        with mock.patch.object(m, "get_node_chatbot_id", return_value="chatbot-1"):
            checks = m.build_default_probe_checks(node_info, patch_config)

        self.assertEqual(
            checks["config_patch"]["items"],
            [
                {
                    "path": "agents.defaults.model.primary",
                    "expected_hash": m.build_probe_value_hash("lanying/openai/gpt-5-mini", True),
                }
            ],
        )
        expected_prompt = m.build_managed_agents_content(
            "chatbot-1",
            "Probe Bot",
            m.normalize_preset_prompt_for_agents_md("hello"),
        )
        self.assertEqual(
            checks["preset_prompt_content"]["expected_hash"],
            hashlib.sha256(expected_prompt.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(checks["preset_prompt_hook"]["required_path"], m.OPENCLAW_MANAGED_AGENTS_PATH)
        self.assertEqual(checks["session_map_runtime"]["expected_effective_enabled"], True)
        self.assertIn("workspace_files", checks)

    def test_build_default_probe_checks_skips_prompt_checks_without_chatbot_binding(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
        }

        with mock.patch.object(m, "get_node_chatbot_id", return_value=""):
            checks = m.build_default_probe_checks(node_info, {"agents": {"defaults": {"model": {"primary": "x"}}}})

        self.assertNotIn("preset_prompt_content", checks)
        self.assertNotIn("preset_prompt_hook", checks)
        self.assertNotIn("workspace_files", checks)

    def test_get_model_list_returns_stable_sorted_models(self):
        m = lanying_openclaw
        raw_models = [
            {
                "vendor": "zeta",
                "model": "z-model",
                "type": "chat",
                "token_limit": 32000,
                "max_output_tokens": 4096,
                "reasoning": False,
            },
            {
                "vendor": "alpha",
                "model": "a-model",
                "type": "chat",
                "token_limit": 32000,
                "max_output_tokens": 8192,
                "reasoning": True,
            },
        ]

        with mock.patch.object(m.lanying_vendor, "list_models", return_value=raw_models):
            models = m.get_model_list("app-id")

        self.assertEqual(
            [item["id"] for item in models],
            ["alpha/a-model", "zeta/z-model"],
        )

    def test_enrich_node_probe_snapshot_builds_summary_fields(self):
        m = lanying_openclaw
        node_info = {
            "last_probe_report_at": 123,
            "api_version": 4,
            "health_probe_status": "ok",
            "account_config_status": "ok",
            "config_patch_status": "mismatch",
            "preset_prompt_content_status": "not_checked",
            "preset_prompt_hook_status": "not_checked",
            "workspace_files_status": "not_checked",
            "session_map_runtime_status": "not_checked",
            "online_marker_status": "not_checked",
        }

        out = m.enrich_node_probe_snapshot(node_info)
        self.assertEqual(out["probe_summary_text"], "partial_issue")
        self.assertEqual(out["probe_cached_at"], 123)
        self.assertFalse(out["probe_in_sync"])

        clean = dict(node_info)
        clean["config_patch_status"] = "ok"
        out = m.enrich_node_probe_snapshot(clean)
        self.assertEqual(out["probe_summary_text"], "ok")
        self.assertTrue(out["probe_in_sync"])
        self.assertTrue(out["probe_supported"])

    def test_enrich_node_probe_snapshot_marks_explicit_old_api_plugin_as_unsupported(self):
        m = lanying_openclaw
        node_info = {
            "status": "normal",
            "api_version": 3,
            "probe_completed": False,
            "probe_timeout": False,
            "health_probe_status": "not_checked",
            "account_config_status": "not_checked",
            "config_patch_status": "not_checked",
            "preset_prompt_content_status": "not_checked",
            "preset_prompt_hook_status": "not_checked",
            "workspace_files_status": "not_checked",
            "session_map_runtime_status": "not_checked",
            "online_marker_status": "not_checked",
        }

        out = m.enrich_node_probe_snapshot(node_info)

        self.assertFalse(out["probe_supported"])
        self.assertEqual(out["probe_support_state"], "unsupported")
        self.assertEqual(out["probe_summary_text"], "unsupported")

    def test_enrich_node_probe_snapshot_keeps_missing_api_version_as_unknown(self):
        m = lanying_openclaw
        node_info = {
            "status": "normal",
            "api_version": "",
            "probe_completed": False,
            "probe_timeout": False,
            "health_probe_status": "not_checked",
            "account_config_status": "not_checked",
            "config_patch_status": "not_checked",
            "preset_prompt_content_status": "not_checked",
            "preset_prompt_hook_status": "not_checked",
            "workspace_files_status": "not_checked",
            "session_map_runtime_status": "not_checked",
            "online_marker_status": "not_checked",
        }

        out = m.enrich_node_probe_snapshot(node_info)

        self.assertFalse(out["probe_supported"])
        self.assertEqual(out["probe_support_state"], "unknown")
        self.assertEqual(out["probe_summary_text"], "not_checked")

    def test_enrich_node_probe_snapshot_marks_probe_timeout_as_failed(self):
        m = lanying_openclaw
        node_info = {
            "status": "normal",
            "api_version": 4,
            "probe_completed": False,
            "probe_timeout": True,
            "health_probe_status": "not_checked",
            "account_config_status": "not_checked",
            "config_patch_status": "not_checked",
            "preset_prompt_content_status": "not_checked",
            "preset_prompt_hook_status": "not_checked",
            "workspace_files_status": "not_checked",
            "session_map_runtime_status": "not_checked",
            "online_marker_status": "not_checked",
        }

        out = m.enrich_node_probe_snapshot(node_info)

        self.assertTrue(out["probe_supported"])
        self.assertEqual(out["probe_support_state"], "supported")
        self.assertEqual(out["probe_summary_text"], "failed")

    def test_sanitize_probe_response_node_removes_password(self):
        m = lanying_openclaw
        node_info = {
            "node_id": "node-1",
            "password": "secret-password",
            "probe_summary_text": "ok",
        }

        out = m.sanitize_probe_response_node(node_info)

        self.assertEqual(out["node_id"], "node-1")
        self.assertEqual(out["probe_summary_text"], "ok")
        self.assertNotIn("password", out)

    def test_handle_client_event_probe_report_updates_node_state(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_id": "probe-1",
        }
        event = {
            "type": "probe_report",
            "probe_id": "probe-1",
            "plugin_version": "1.2.3",
            "api_version": "3",
            "reported_at": 123456789,
            "results": {
                "health": {"status": "degraded", "details": {"connected": False}},
                "account_config": {"status": "ok", "details": {"enabled": True}},
                "config_patch": {"status": "mismatch", "details": {"mismatched_keys": ["agents.defaults.model.primary"], "failed_keys": []}},
                "preset_prompt_content": {"status": "ok", "details": {"match": True}},
                "preset_prompt_hook": {"status": "mismatch", "details": {"missing_requirements": ["injection_path_missing"]}},
                "workspace_files": {"status": "failed", "details": {"managed_agents_file_exists": False}},
                "session_map_runtime": {"status": "ok", "details": {"effective_enabled": True}},
                "online_marker": {"status": "degraded", "details": {"self_id_ready": False}},
            },
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_fields") as mocked_update, \
             mock.patch.object(m, "maybe_auto_repair_probe_mismatch") as mocked_auto_repair:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_update.assert_called_once()
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["health_probe_status"], "degraded")
        self.assertEqual(json.loads(updates["config_patch_mismatched_keys"]), ["agents.defaults.model.primary"])
        self.assertEqual(json.loads(updates["preset_prompt_hook_missing_requirements"]), ["injection_path_missing"])
        self.assertEqual(updates["workspace_files_status"], "failed")
        self.assertEqual(updates["plugin_version"], "1.2.3")
        self.assertEqual(updates["presence_status"], "online")
        self.assertEqual(updates["presence_source"], "online_marker")
        mocked_auto_repair.assert_not_called()

    def test_handle_client_event_config_sync_report_updates_latest_state(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_config_sync_id": "sync-1",
        }
        event = {
            "type": "config_sync_report",
            "sync_id": "sync-1",
            "object_type": "config_patch",
            "status": "ok",
            "reported_at": 123456,
            "plugin_version": "1.2.3",
            "api_version": "4",
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_fields") as mocked_update:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_update.assert_called_once()
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["last_config_sync_status"], "ok")
        self.assertEqual(updates["last_config_sync_id"], "sync-1")

    def test_handle_client_event_config_sync_report_skips_stale_sync_id(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_config_sync_id": "sync-new",
        }
        event = {
            "type": "config_sync_report",
            "sync_id": "sync-old",
            "object_type": "config_patch",
            "status": "failed",
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_fields") as mocked_update:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_update.assert_not_called()

    def test_handle_client_event_online_marks_presence_online_and_keeps_bootstrap(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "status": "wait",
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_field") as mocked_update_field, \
             mock.patch.object(m, "update_node_fields") as mocked_update_fields, \
             mock.patch.object(m, "get_node", return_value=node_info), \
             mock.patch.object(m, "get_model_patch_config", return_value={}), \
             mock.patch.object(m, "update_node_config"), \
             mock.patch.object(m, "maybe_sync_node_bound_chatbot_preset_prompt"), \
             mock.patch.object(m, "sync_session_map_settings_to_node"), \
             mock.patch.object(m, "sync_session_mapping_snapshot_to_node"), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "schedule_probe_to_node"):
            m.handle_client_event({"type": "online", "plugin_version": "1.2.3", "api_version": "4"}, "app-id", "openclaw-user", "COMMAND")

        mocked_update_fields.assert_called_once()
        presence_updates = mocked_update_fields.call_args.args[2]
        self.assertEqual(presence_updates["presence_status"], "online")
        self.assertEqual(presence_updates["presence_source"], "online_marker")
        mocked_update_field.assert_any_call("app-id", "node-1", "status", "normal")

    def test_handle_client_event_offline_marks_presence_offline_only(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "status": "normal",
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_fields") as mocked_update:
            m.handle_client_event({"type": "offline"}, "app-id", "openclaw-user", "COMMAND")

        mocked_update.assert_called_once()
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["presence_status"], "offline")
        self.assertEqual(updates["presence_source"], "offline_marker")
        self.assertIn("presence_updated_at", updates)
        self.assertNotIn("status", updates)

    def test_handle_client_event_probe_report_skips_stale_probe_id(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_id": "probe-new",
        }
        event = {
            "type": "probe_report",
            "probe_id": "probe-old",
            "results": {},
        }

        with mock.patch.object(m, "get_nodes_by_user_id", return_value=[node_info]), \
             mock.patch.object(m, "update_node_fields") as mocked_update:
            m.handle_client_event(event, "app-id", "openclaw-user", "COMMAND")

        mocked_update.assert_not_called()

    def test_send_probe_to_node_resets_previous_probe_statuses(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
        }

        with mock.patch.object(m, "get_node", return_value=node_info), \
             mock.patch.object(m, "update_node_fields") as mocked_update:
            result = m.send_probe_to_node(node_info, {"health": {}})

        self.assertEqual(result["result"], "ok")
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["health_probe_status"], "not_checked")
        self.assertEqual(updates["config_patch_status"], "not_checked")
        self.assertEqual(updates["online_marker_status"], "not_checked")
        self.assertNotEqual(updates["last_probe_id"], "")

    def test_send_probe_to_node_reuses_recent_inflight_probe(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
        }
        inflight_node = {
            **node_info,
            "last_probe_id": "probe-inflight",
            "last_probe_at": 1000,
            "probe_completed": False,
            "probe_timeout": False,
        }

        with mock.patch.object(m, "get_node", return_value=inflight_node), \
             mock.patch.object(m, "update_node_fields") as mocked_update, \
             mock.patch.object(m, "secrets") as mocked_secrets, \
             mock.patch.object(m.time, "time", return_value=1.001):
            result = m.send_probe_to_node(node_info, {"health": {}})

        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["probe_id"], "probe-inflight")
        self.assertTrue(result["data"]["reused"])
        mocked_update.assert_not_called()
        mocked_secrets.token_hex.assert_not_called()

    def test_sync_model_config_schedules_delayed_probe_when_prompt_sync_enabled(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
        }
        patch_config = {"agents": {"defaults": {"model": {"primary": "x"}}}}

        with mock.patch.object(m, "get_node", return_value=node_info), \
             mock.patch.object(m, "get_model_patch_config", return_value=patch_config), \
             mock.patch.object(m, "update_node_config"), \
             mock.patch.object(m, "maybe_sync_node_bound_chatbot_preset_prompt"), \
             mock.patch.object(m, "has_node_bound_chatbot", return_value=True), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "schedule_probe_to_node") as mocked_schedule:
            result = m.sync_model_config("app-id", "node-1", sync_preset_prompt=True)

        self.assertEqual(result["result"], "ok")
        mocked_schedule.assert_called_once()
        self.assertEqual(mocked_schedule.call_args.kwargs["delay_ms"], m.PROBE_POST_SYNC_DELAY_MS)

    def test_sync_model_config_and_wait_uses_legacy_fallback_for_old_plugin(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "api_version": 3,
        }

        with mock.patch.object(m, "get_node", side_effect=[node_info, node_info]), \
             mock.patch.object(m, "sync_model_config", return_value={"result": "ok", "data": {"success": True}}) as mocked_sync:
            result = m.sync_model_config_and_wait("app-id", "node-1", 1000)

        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["legacy_fallback"])
        self.assertFalse(result["data"]["completed"])
        mocked_sync.assert_called_once_with("app-id", "node-1", True)

    def test_sync_model_config_and_wait_returns_success_on_matching_report(self):
        m = lanying_openclaw
        initial_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "api_version": 4,
            "last_config_sync_report_at": 100,
        }
        updated_node = {
            **initial_node,
            "last_config_sync_id": "sync-1",
            "last_config_sync_report_at": 200,
            "last_config_sync_status": "ok",
        }

        with mock.patch.object(m, "get_node", side_effect=[initial_node, updated_node]), \
             mock.patch.object(m, "get_model_patch_config", return_value={"agents": {"defaults": {"model": {"primary": "x"}}}}), \
             mock.patch.object(m, "update_node_config", return_value={"result": "ok", "data": {"msg_id": 1, "sync_id": "sync-1"}}), \
             mock.patch.object(m, "update_node_fields"), \
             mock.patch.object(m, "is_matching_config_sync_report", side_effect=[True]), \
             mock.patch.object(m.time, "time", return_value=0.0):
            result = m.sync_model_config_and_wait("app-id", "node-1", 1000)

        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["completed"])
        self.assertTrue(result["data"]["success"])

    def test_sync_model_config_and_wait_returns_timeout_without_report(self):
        m = lanying_openclaw
        initial_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "api_version": 4,
            "last_config_sync_report_at": 100,
        }
        pending_node = {
            **initial_node,
            "last_config_sync_id": "sync-1",
            "last_config_sync_status": "pending",
        }

        with mock.patch.object(m, "get_node", side_effect=[initial_node, pending_node, pending_node]), \
             mock.patch.object(m, "get_model_patch_config", return_value={"agents": {"defaults": {"model": {"primary": "x"}}}}), \
             mock.patch.object(m, "update_node_config", return_value={"result": "ok", "data": {"msg_id": 1, "sync_id": "sync-1"}}), \
             mock.patch.object(m, "update_node_fields"), \
             mock.patch.object(m.time, "time", side_effect=[0.0, 0.0, 1.1]), \
             mock.patch.object(m.time, "sleep"):
            result = m.sync_model_config_and_wait("app-id", "node-1", 1000)

        self.assertEqual(result["result"], "ok")
        self.assertFalse(result["data"]["completed"])
        self.assertTrue(result["data"]["timeout"])
        self.assertFalse(result["data"]["success"])

    def test_schedule_probe_to_node_delayed_rebuilds_checks_from_latest_node_state(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
        }

        with mock.patch.object(m, "get_node", return_value=node_info), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}) as mocked_build, \
             mock.patch.object(m, "send_probe_to_node") as mocked_send:
            m._delayed_send_probe_to_node("app-id", "node-1", 0)

        mocked_build.assert_called_once()
        rebuilt_node = mocked_build.call_args.args[0]
        self.assertEqual(rebuilt_node["app_id"], "app-id")
        self.assertEqual(rebuilt_node["node_id"], "node-1")
        mocked_send.assert_called_once_with(rebuilt_node, {"health": {}})

    def test_probe_auto_repair_retries_mismatch_with_limit(self):
        m = lanying_openclaw
        node_info = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "session_map_sync": "on",
            "merge_sub_sessions": "off",
            "last_probe_id": "probe-1",
            "probe_repair_counts": {"config_patch": 0, "preset_prompt": 0, "session_map_runtime": 0},
        }
        event = {
            "probe_id": "probe-1",
            "results": {
                "config_patch": {"status": "mismatch", "details": {"mismatched_keys": ["agents.defaults.model.primary"]}},
            },
        }
        patch_config = {"agents": {"defaults": {"model": {"primary": "x"}}}}

        with mock.patch.object(m, "get_model_patch_config", return_value=patch_config), \
             mock.patch.object(m, "update_node_config") as mocked_update_config, \
             mock.patch.object(m, "sync_bound_chatbot_preset_prompt") as mocked_sync_prompt, \
             mock.patch.object(m, "sync_session_map_settings_to_node") as mocked_sync_session_settings, \
             mock.patch.object(m, "sync_session_mapping_snapshot_to_node") as mocked_sync_snapshot, \
             mock.patch.object(m, "update_probe_repair_counts") as mocked_update_counts, \
             mock.patch.object(m, "get_node", return_value=node_info), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "schedule_probe_to_node") as mocked_schedule, \
             mock.patch.object(m, "has_node_bound_chatbot", return_value=False):
            m.maybe_auto_repair_probe_mismatch(node_info, event)

        mocked_update_config.assert_called_once_with("app-id", "node-1", patch_config)
        mocked_sync_prompt.assert_not_called()
        mocked_sync_session_settings.assert_not_called()
        mocked_sync_snapshot.assert_not_called()
        mocked_update_counts.assert_called_once()
        updated_counts = mocked_update_counts.call_args.args[2]
        self.assertEqual(updated_counts["config_patch"], 1)
        mocked_schedule.assert_called_once()

        capped_node = dict(node_info)
        capped_node["probe_repair_counts"] = {"config_patch": m.PROBE_AUTO_REPAIR_MAX_ATTEMPTS}
        with mock.patch.object(m, "get_model_patch_config", return_value=patch_config), \
             mock.patch.object(m, "update_node_config") as mocked_update_config, \
             mock.patch.object(m, "update_probe_repair_counts") as mocked_update_counts, \
             mock.patch.object(m, "schedule_probe_to_node") as mocked_schedule, \
             mock.patch.object(m, "has_node_bound_chatbot", return_value=False):
            m.maybe_auto_repair_probe_mismatch(capped_node, event)

        mocked_update_config.assert_not_called()
        mocked_update_counts.assert_not_called()
        mocked_schedule.assert_not_called()

    def test_probe_node_waits_for_matching_report(self):
        m = lanying_openclaw
        initial_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_report_at": 100,
            "last_probe_id": "probe-old",
            "probe_repair_last_probe_id": "probe-old",
        }
        completed_node = {
            **initial_node,
            "last_probe_id": "probe-new",
            "probe_repair_last_probe_id": "probe-new",
            "last_probe_report_at": 200,
            "probe_summary_text": "ok",
        }

        with mock.patch.object(m, "get_node", side_effect=[initial_node, completed_node]), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "send_probe_to_node", return_value={"result": "ok", "data": {"probe_id": "probe-new"}}):
            result = m.probe_node("app-id", "node-1", wait_timeout_ms=2000, wait_for_fresh_report=True)

        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["completed"])
        self.assertFalse(result["data"]["timeout"])
        self.assertEqual(result["data"]["probe_id"], "probe-new")
        self.assertEqual(result["data"]["node"]["probe_summary_text"], "ok")

    def test_probe_node_reuses_recent_inflight_probe_without_resending(self):
        m = lanying_openclaw
        inflight_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_id": "probe-inflight",
            "last_probe_at": 1000,
            "last_probe_report_at": 100,
            "probe_completed": False,
            "probe_timeout": False,
            "probe_repair_last_probe_id": "probe-old",
            "probe_summary_text": "partial_issue",
        }

        with mock.patch.object(m, "get_node", side_effect=[inflight_node, inflight_node]), \
             mock.patch.object(m, "send_probe_to_node") as mocked_send, \
             mock.patch.object(m.time, "time", return_value=1.001):
            result = m.probe_node("app-id", "node-1", wait_timeout_ms=2000, wait_for_fresh_report=False)

        mocked_send.assert_not_called()
        self.assertEqual(result["result"], "ok")
        self.assertFalse(result["data"]["triggered"])
        self.assertFalse(result["data"]["completed"])
        self.assertEqual(result["data"]["probe_id"], "probe-inflight")

    def test_probe_node_timeout_returns_latest_cached_snapshot(self):
        m = lanying_openclaw
        initial_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_report_at": 100,
            "last_probe_id": "probe-old",
            "probe_repair_last_probe_id": "probe-old",
            "probe_summary_text": "partial_issue",
        }
        pending_node = {
            **initial_node,
            "last_probe_id": "probe-new",
            "probe_repair_last_probe_id": "probe-old",
        }
        timeout_node = {
            **pending_node,
            "probe_timeout": True,
        }

        with mock.patch.object(m, "get_node", side_effect=[initial_node, pending_node, pending_node, timeout_node, timeout_node, timeout_node]), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "send_probe_to_node", return_value={"result": "ok", "data": {"probe_id": "probe-new"}}), \
             mock.patch.object(m, "update_node_fields") as mocked_update, \
             mock.patch.object(m.time, "time", side_effect=[0.0, 0.0, 0.1, 0.6, 1.1, 1.1]), \
             mock.patch.object(m.time, "sleep"):
            result = m.probe_node("app-id", "node-1", wait_timeout_ms=1000, wait_for_fresh_report=True)

        self.assertEqual(result["result"], "ok")
        self.assertFalse(result["data"]["completed"])
        self.assertTrue(result["data"]["timeout"])
        self.assertEqual(result["data"]["summary"], "failed")
        mocked_update.assert_called_once()
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["presence_status"], "offline")
        self.assertEqual(updates["presence_source"], "probe_timeout")

    def test_probe_node_timeout_does_not_override_success_arriving_at_deadline(self):
        m = lanying_openclaw
        initial_node = {
            "app_id": "app-id",
            "node_id": "node-1",
            "user_id": "openclaw-user",
            "last_probe_report_at": 100,
            "last_probe_id": "probe-old",
            "probe_repair_last_probe_id": "probe-old",
            "probe_summary_text": "partial_issue",
        }
        pending_node = {
            **initial_node,
            "last_probe_id": "probe-new",
            "probe_repair_last_probe_id": "probe-old",
            "probe_completed": False,
            "probe_timeout": False,
        }
        completed_node = {
            **pending_node,
            "probe_repair_last_probe_id": "probe-new",
            "last_probe_report_at": 200,
            "probe_summary_text": "ok",
            "password": "node-password",
        }

        with mock.patch.object(m, "get_node", side_effect=[initial_node, pending_node, pending_node, completed_node]), \
             mock.patch.object(m, "build_default_probe_checks", return_value={"health": {}}), \
             mock.patch.object(m, "send_probe_to_node", return_value={"result": "ok", "data": {"probe_id": "probe-new"}}), \
             mock.patch.object(m, "update_node_fields") as mocked_update, \
             mock.patch.object(m.time, "time", side_effect=[0.0, 0.0, 0.2, 0.6, 1.1]), \
             mock.patch.object(m.time, "sleep"):
            result = m.probe_node("app-id", "node-1", wait_timeout_ms=1000, wait_for_fresh_report=True)

        mocked_update.assert_not_called()
        self.assertEqual(result["result"], "ok")
        self.assertTrue(result["data"]["completed"])
        self.assertFalse(result["data"]["timeout"])
        self.assertEqual(result["data"]["summary"], "ok")
        self.assertNotIn("password", result["data"]["node"])


if __name__ == "__main__":
    unittest.main()
