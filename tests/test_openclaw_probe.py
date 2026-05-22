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
        mocked_auto_repair.assert_called_once()

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

        with mock.patch.object(m, "update_node_fields") as mocked_update:
            result = m.send_probe_to_node(node_info, {"health": {}})

        self.assertEqual(result["result"], "ok")
        updates = mocked_update.call_args.args[2]
        self.assertEqual(updates["health_probe_status"], "not_checked")
        self.assertEqual(updates["config_patch_status"], "not_checked")
        self.assertEqual(updates["online_marker_status"], "not_checked")
        self.assertNotEqual(updates["last_probe_id"], "")

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


if __name__ == "__main__":
    unittest.main()
