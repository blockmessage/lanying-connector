import importlib.util
import json
import pathlib
import sys
import tempfile
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
    module_name = "lanying_openclaw_sync_validation_test"
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
            get_lanying_connector=lambda app_id: {"lanying_admin_token": "admin-token"},
            get_message_antispam=lambda app_id: "antispam",
        ),
        "lanying_chatbot": types.SimpleNamespace(
            get_chatbot=lambda app_id, chatbot_id: {"user_id": "chatbot-user"},
        ),
        "lanying_im_api": types.SimpleNamespace(
            post_send_message=lambda *args, **kwargs: types.SimpleNamespace(status_code=200, json=lambda: {"code": 200, "msg_ids": [101]}),
            fetch_conversation_messages=lambda *args, **kwargs: {"code": 200, "data": {"messages": []}},
            register=lambda *args, **kwargs: {"code": 200, "data": {"user_id": "sender-user"}},
            create_group=lambda *args, **kwargs: {"code": 200, "data": {"group_id": "group-1"}},
            admin_join_group_direct=lambda *args, **kwargs: {"code": 200},
            admin_add_roster_direct=lambda *args, **kwargs: {"code": 200},
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
            executor=types.SimpleNamespace(submit=lambda fn, *args, **kwargs: ("submitted", fn, args, kwargs)),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


def _load_openclaw_sync_validation():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw_sync_validation.py"
    module_name = "lanying_openclaw_sync_validation_test_module"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_config": types.SimpleNamespace(
            get_lanying_connector=lambda app_id: {"lanying_admin_token": "admin-token"},
        ),
        "lanying_im_api": types.SimpleNamespace(
            post_send_message=lambda *args, **kwargs: types.SimpleNamespace(status_code=200, json=lambda: {"code": 200, "msg_ids": [101]}),
            fetch_conversation_messages=lambda *args, **kwargs: {"code": 200, "data": {"messages": []}},
            create_group=lambda *args, **kwargs: {"code": 200, "data": {"group_id": "group-1"}},
            admin_join_group_direct=lambda *args, **kwargs: {"code": 200},
        ),
        "lanying_openclaw": _load_lanying_openclaw(),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda fn, *args, **kwargs: ("submitted", fn, args, kwargs)),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


def _load_openclaw_service(openclaw_module, sync_validation_module):
    module_path = pathlib.Path(__file__).resolve().parents[1] / "services" / "openclaw_service.py"
    module_name = "openclaw_service_sync_validation_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {
        "lanying_openclaw": openclaw_module,
        "lanying_openclaw_sync_validation": sync_validation_module,
    }):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


class OpenClawSyncValidationTests(unittest.TestCase):
    def test_get_base_dir_follows_lanying_logging_layout(self):
        m = _load_openclaw_sync_validation()
        with tempfile.TemporaryDirectory() as tempdir, \
             mock.patch("os.getcwd", return_value=tempdir), \
             mock.patch.object(m.socket, "gethostname", return_value="host-a"):
            previous_cwd = pathlib.Path.cwd()
            try:
                __import__("os").chdir(tempdir)
                base_dir = m.get_base_dir()
            finally:
                __import__("os").chdir(previous_cwd)
        self.assertTrue(base_dir.endswith("log/host-a/openclaw_sync_validation"))

    def test_build_runtime_auto_registers_sender_and_creates_groups(self):
        m = _load_openclaw_sync_validation()
        with mock.patch.object(m.core, "get_node", return_value={"user_id": "openclaw-user"}), \
             mock.patch.object(m.core, "resolve_bound_chatbot_user_id", return_value="chatbot-user"), \
             mock.patch.object(m.lanying_config, "get_lanying_connector", return_value={"lanying_admin_token": "admin-token"}), \
             mock.patch.object(m, "get_validation_config", return_value={
                 "sender_username_prefix": "manual_case",
             }), \
             mock.patch.object(m, "create_validation_user", return_value={
                 "result": "ok",
                 "data": {"user_id": "sender-user", "username": "manual_case_x", "password": "pw"},
             }) as create_user_mock, \
             mock.patch.object(m, "ensure_direct_roster_pair", return_value={"result": "ok", "data": {}}) as roster_mock, \
             mock.patch.object(m, "create_validation_group", side_effect=[
                 {"result": "ok", "data": {"group_id": "group-openclaw"}},
                 {"result": "ok", "data": {"group_id": "group-chatbot"}},
             ]) as create_group_mock:
            result = m.build_runtime("app-id", "node-1")
        self.assertEqual(result["result"], "ok")
        data = result["data"]
        self.assertEqual(data["sender_user_id"], "sender-user")
        self.assertEqual(data["sender_username"], "manual_case_x")
        self.assertEqual(data["sender_password"], "pw")
        self.assertEqual(data["group_openclaw_id"], "group-openclaw")
        self.assertEqual(data["group_chatbot_id"], "group-chatbot")
        create_user_mock.assert_called_once_with("app-id", "manual_case")
        self.assertEqual(roster_mock.call_count, 2)
        self.assertEqual(create_group_mock.call_count, 2)
        self.assertTrue(any(row["label"] == "validation_sender_username" for row in data["provisioning_rows"]))
        self.assertTrue(any(row["label"] == "validation_sender_password" for row in data["provisioning_rows"]))

    def test_create_validation_group_explicitly_joins_members(self):
        m = _load_openclaw_sync_validation()
        with mock.patch.object(m.lanying_im_api, "create_group", return_value={
            "code": 200,
            "data": {
                "group_id": "group-99",
            },
        }) as create_group_mock, \
             mock.patch.object(m.lanying_im_api, "admin_join_group_direct", return_value={"code": 200}) as join_group_mock:
            result = m.create_validation_group("app-id", "owner-user", "test-group", ["10001", "10002"])
        self.assertEqual(result["result"], "ok")
        self.assertEqual(result["data"]["group_id"], "group-99")
        create_group_mock.assert_called_once()
        join_group_mock.assert_called_once_with("app-id", "group-99", [10001, 10002])
        self.assertEqual(result["data"]["join_result"], {"code": 200})

    def test_normalize_sync_validation_scenarios_supports_all_and_single(self):
        m = _load_openclaw_sync_validation()
        self.assertEqual(
            m.normalize_scenarios("all", None)["requested_scenarios"],
            [
                "group_openclaw",
                "group_chatbot",
                "direct_openclaw",
                "direct_chatbot",
                "group_openclaw_subagent",
                "group_chatbot_subagent",
                "direct_openclaw_subagent",
                "direct_chatbot_subagent",
            ],
        )
        self.assertEqual(
            m.normalize_scenarios("direct_openclaw", None)["requested_scenarios"],
            ["direct_openclaw"],
        )

    def test_normalize_sync_validation_scenarios_supports_alias_groups(self):
        m = _load_openclaw_sync_validation()
        self.assertEqual(
            m.normalize_scenarios("subagent", None)["requested_scenarios"],
            [
                "group_openclaw_subagent",
                "group_chatbot_subagent",
                "direct_openclaw_subagent",
                "direct_chatbot_subagent",
            ],
        )
        self.assertEqual(
            m.normalize_scenarios("group", None)["requested_scenarios"],
            [
                "group_openclaw",
                "group_chatbot",
                "group_openclaw_subagent",
                "group_chatbot_subagent",
            ],
        )
        self.assertEqual(
            m.normalize_scenarios(None, "direct,chatbot")["requested_scenarios"],
            [
                "direct_openclaw",
                "direct_chatbot",
                "direct_openclaw_subagent",
                "direct_chatbot_subagent",
                "group_chatbot",
                "group_chatbot_subagent",
            ],
        )
        self.assertEqual(
            m.normalize_scenarios(None, ["basic", "direct_openclaw_subagent", "openclaw"])["requested_scenarios"],
            [
                "group_openclaw",
                "group_chatbot",
                "direct_openclaw",
                "direct_chatbot",
                "direct_openclaw_subagent",
                "group_openclaw_subagent",
            ],
        )

    def test_normalize_sync_validation_scenarios_supports_comma_separated_scenario_field(self):
        m = _load_openclaw_sync_validation()
        result = m.normalize_scenarios("direct_openclaw_subagent,direct_chatbot_subagent", None)
        self.assertEqual(
            result["requested_scenarios"],
            [
                "direct_openclaw_subagent",
                "direct_chatbot_subagent",
            ],
        )
        self.assertEqual(result["invalid_names"], [])

    def test_normalize_sync_validation_scenarios_returns_invalid_names_without_fallback(self):
        m = _load_openclaw_sync_validation()
        result = m.normalize_scenarios("direct_openclaw_subagent,unknown_case", None)
        self.assertEqual(result["requested_scenarios"], ["direct_openclaw_subagent"])
        self.assertEqual(result["invalid_names"], ["unknown_case"])

    def test_start_sync_validation_rejects_invalid_scenario_names(self):
        m = _load_openclaw_sync_validation()
        result = m.start("app-id", "node-1", "direct_openclaw_subagent,unknown_case", None)
        self.assertEqual(result["result"], "error")
        self.assertIn("invalid scenario name", result["message"])
        self.assertIn("unknown_case", result["message"])

    def test_render_sync_validation_report_html_contains_summary_and_failure(self):
        m = _load_openclaw_sync_validation()
        html = m.render_report_html({
            "task_id": "task-1",
            "app_id": "app-id",
            "node_id": "node-1",
            "status": m.STATUS_FAILED,
            "started_at": 1000,
            "ended_at": 2000,
            "report_path": "/tmp/report.html",
            "scenarios": [
                {
                    "name": "group_openclaw",
                    "description": "group case",
                    "status": m.STATUS_FAILED,
                    "started_at": 1000,
                    "ended_at": 1500,
                    "failure_reason": "reply missing",
                    "participant_rows": [{"label": "scenario_sender_user_id", "value": "u1"}, {"label": "validation_sender_password", "value": "p1"}],
                    "request_rows": [{"label": "trigger_text", "value": "hello"}],
                    "comparison_rows": [{"label": "reply_found", "value": False}],
                    "messages": [],
                    "mapping_rows": [],
                    "notes": "",
                }
            ],
        })
        self.assertIn("OpenClaw Sync Validation Report", html)
        self.assertIn("group_openclaw", html)
        self.assertIn("reply missing", html)
        self.assertIn("scenario_sender_user_id", html)
        self.assertIn("validation_sender_password", html)

    def test_build_trigger_text_requests_explicit_non_no_reply_answer(self):
        m = _load_openclaw_sync_validation()
        text = m.build_trigger_text({"name": "direct_openclaw"}, 12345)
        self.assertIn("不要回复 NO_REPLY", text)
        self.assertIn("SYNC_OK_direct_openclaw_12345", text)

    def test_build_trigger_text_for_subagent_scenario_requests_child_session(self):
        m = _load_openclaw_sync_validation()
        text = m.build_trigger_text({"name": "direct_chatbot_subagent", "expect_root_and_sub_sessions": True}, 12345)
        self.assertIn("必须创建一个子 session 或 subagent", text)
        self.assertIn("请不要直接在当前会话回答", text)
        self.assertIn("主会话等待子 session 完成后再继续", text)
        self.assertIn("主会话在收到子 session 结果后", text)
        self.assertIn("不要回复 NO_REPLY", text)
        self.assertIn("SYNC_OK_direct_chatbot_subagent_12345", text)

    def test_list_subagent_conversation_views_accepts_second_precision_mapping_times(self):
        m = _load_openclaw_sync_validation()
        runtime = {"app_id": "app-id", "node_id": "node-1"}
        scenario_def = {"sender_user_id": "sender-user"}
        with mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[
            {
                "session_key": "agent:main:subagent:test-child",
                "group_id": "group-child",
                "sender_user_id": "",
                "origin_user_id": "sender-user",
                "parent_session_key": "agent:main:clawchat-router:group:group-root",
                "root_session_key": "agent:main:clawchat-router:group:group-root",
                "updated_at": 1780644987,
            }
        ]), mock.patch.object(m.core, "get_session_last_message_time", return_value=1780644994717):
            out = m.list_subagent_conversation_views(
                runtime,
                scenario_def,
                1780644969884,
                ["agent:main:clawchat-router:group:group-root"],
            )
        self.assertEqual(out, [{"sender_user_id": "sender-user", "conversation_id": "group-child", "chat_type": "group"}])

    def test_select_relevant_mappings_uses_last_message_time_for_second_precision_updates(self):
        m = _load_openclaw_sync_validation()
        runtime = {"app_id": "app-id", "node_id": "node-1"}
        scenario_def = {
            "sender_user_id": "sender-user",
            "conversation_id": "group-root",
            "expected_reply_user_id": "chatbot-user",
            "chat_type": "group",
        }
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-root",
            "sender_user_id": "",
            "origin_user_id": "sender-user",
            "openclaw_user_id": "chatbot-user",
            "updated_at": 1780644987,
        }
        with mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[mapping]), \
             mock.patch.object(m.core, "get_session_last_message_time", return_value=1780644994717):
            out = m.select_relevant_mappings(runtime, scenario_def, 1780644969884)
        self.assertEqual(out, [mapping])

    def test_send_message_group_uses_client_like_mention_config(self):
        m = _load_openclaw_sync_validation()
        runtime = {
            "app_id": "app-id",
            "config": {"lanying_admin_token": "admin-token"},
            "sender_username": "sync_validation_demo",
        }
        scenario_def = {
            "chat_type": "group",
            "sender_user_id": "sender-user",
            "target_user_id": "6657853037888",
            "conversation_id": "group-1",
        }
        fake_response = types.SimpleNamespace(status_code=200, json=lambda: {"code": 200})
        with mock.patch.object(m.lanying_im_api, "post_send_message", return_value=fake_response) as post_mock:
            result = m.send_message(runtime, scenario_def, "ping")
        self.assertEqual(result["http_status"], 200)
        extra = post_mock.call_args.args[7]
        self.assertEqual(extra["msg_config"]["mentionAll"], False)
        self.assertEqual(extra["msg_config"]["mentionList"], [6657853037888])
        self.assertEqual(extra["msg_config"]["mentionedMessage"], "")
        self.assertEqual(extra["msg_config"]["pushMessage"], "")
        self.assertEqual(extra["msg_config"]["senderNickname"], "sync_validation_demo")

    def test_get_mapping_sender_user_id_prefers_origin_user_id(self):
        m = _load_openclaw_sync_validation()
        self.assertEqual(
            m.get_mapping_sender_user_id({
                "sender_user_id": "",
                "origin_user_id": "origin-user",
            }),
            "origin-user",
        )
        self.assertEqual(
            m.get_mapping_sender_user_id({
                "sender_user_id": "legacy-sender",
                "origin_user_id": "",
            }),
            "legacy-sender",
        )

    def test_extract_request_msg_id_reads_top_level_msg_ids(self):
        m = _load_openclaw_sync_validation()
        self.assertEqual(
            m.extract_request_msg_id({
                "result": {
                    "code": 200,
                    "msg_ids": [12345],
                }
            }),
            "12345",
        )

    def test_find_replies_filters_by_request_msg_id(self):
        m = _load_openclaw_sync_validation()
        matched = m.find_replies([
            {
                "msg_id": "m1",
                "from_user_id": "openclaw-user",
                "content": "reply-1",
                "timestamp": 2000,
                "ext": {"openclaw": {"type": "session_sync_delivery", "request_msg_id": "req-1"}},
            },
            {
                "msg_id": "m2",
                "from_user_id": "openclaw-user",
                "content": "reply-2",
                "timestamp": 2001,
                "ext": {"openclaw": {"type": "session_sync_delivery", "request_msg_id": "req-2"}},
            },
            {
                "msg_id": "m3",
                "from_user_id": "openclaw-user",
                "content": "reply-3",
                "timestamp": 2002,
                "ext": {},
            },
        ], "openclaw-user", 1000, "req-2")
        self.assertEqual([item["msg_id"] for item in matched], ["m2"])

    def test_find_replies_requires_explicit_request_link_when_request_msg_id_known(self):
        m = _load_openclaw_sync_validation()
        matched = m.find_replies([
            {
                "msg_id": "m1",
                "from_user_id": "openclaw-user",
                "content": "reply-1",
                "timestamp": 2000,
                "ext": {},
            },
            {
                "msg_id": "m2",
                "from_user_id": "openclaw-user",
                "content": "reply-2",
                "timestamp": 2001,
                "ext": {"openclaw": {"type": "session_sync_delivery", "trigger_msg_id": "req-1"}},
            },
        ], "openclaw-user", 1000, "req-1")
        self.assertEqual([item["msg_id"] for item in matched], ["m2"])

    def test_find_duplicate_visible_replies_by_content_matches_same_content(self):
        m = _load_openclaw_sync_validation()
        matched = m.find_duplicate_visible_replies_by_content([
            {
                "msg_id": "m1",
                "from_user_id": "openclaw-user",
                "content": "same-reply",
                "timestamp": 2000,
                "ext": {"openclaw": {"type": "session_sync_delivery", "request_msg_id": "req-1"}},
            },
            {
                "msg_id": "m2",
                "from_user_id": "openclaw-user",
                "content": "same-reply",
                "timestamp": 2001,
                "ext": {"openclaw": {"type": "session_sync_delivery", "request_msg_id": "bad-req"}},
            },
            {
                "msg_id": "m3",
                "from_user_id": "openclaw-user",
                "content": "other-reply",
                "timestamp": 2002,
                "ext": {"openclaw": {"type": "session_sync_delivery", "request_msg_id": "req-1"}},
            },
        ], {
            "msg_id": "m1",
            "from_user_id": "openclaw-user",
            "content": "same-reply",
            "timestamp": 2000,
        }, "openclaw-user", 1000)
        self.assertEqual([item["msg_id"] for item in matched], ["m1", "m2"])

    def test_execute_scenario_prefers_exact_reply_session_mapping_for_sender_check(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = int(__import__("time").time() * 1000) + 5000
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 500,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "direct_openclaw",
            "description": "direct case",
            "chat_type": "direct",
            "target_user_id": "openclaw-user",
            "conversation_id": "openclaw-user",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "openclaw-user",
            "require_mapping": True,
        }
        with mock.patch.object(m, "send_message", return_value={"http_status": 200, "result": {"code": 200}}), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [{
                         "msg_id": "m1",
                         "from_xid": {"uid": "openclaw-user"},
                         "to_xid": {"uid": "sender-user"},
                         "content": "reply",
                         "ctype": "TEXT",
                         "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat:direct:sender-user"}}, ensure_ascii=False),
                         "timestamp": reply_timestamp,
                     }]
                 }
             }), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[{
                 "session_key": "agent:main:clawchat:direct:old-user",
                 "sender_user_id": "",
                 "origin_user_id": "",
                 "openclaw_user_id": "openclaw-user",
                 "updated_at": 1,
             }]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", return_value={
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "sender_user_id": "",
                 "origin_user_id": "sender-user",
                 "openclaw_user_id": "openclaw-user",
                 "updated_at": 2,
             }), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_PASSED)
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertTrue(comparison["exact_reply_mapping_found"])
        self.assertEqual(comparison["exact_reply_mapping_sender_user_id"], "sender-user")
        self.assertTrue(comparison["mapping_sender_ok"])
        self.assertEqual(comparison["matched_visible_reply_count"], 1)
        self.assertFalse(comparison["duplicate_reply_detected"])

    def test_execute_scenario_accepts_origin_user_id_for_mapping_sender_check(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = int(__import__("time").time() * 1000) + 5000
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 500,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "group_openclaw",
            "description": "group case",
            "chat_type": "group",
            "target_user_id": "openclaw-user",
            "conversation_id": "group-1",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "openclaw-user",
            "require_mapping": True,
        }
        with mock.patch.object(m, "send_message", return_value={"http_status": 200, "result": {"code": 200}}), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [{
                         "msg_id": "m1",
                         "from_xid": {"uid": "openclaw-user"},
                         "to_xid": {"uid": "sender-user"},
                         "content": "reply",
                         "ctype": "TEXT",
                         "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat:group:group-1"}}, ensure_ascii=False),
                         "timestamp": reply_timestamp,
                     }]
                 }
             }), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", return_value={
                 "session_key": "agent:main:clawchat:group:group-1",
                 "sender_user_id": "",
                 "origin_user_id": "sender-user",
                 "openclaw_user_id": "openclaw-user",
                 "group_id": "group-1",
                 "updated_at": 2,
             }), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_PASSED)

    def test_execute_scenario_fails_when_duplicate_visible_replies_detected(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = int(__import__("time").time() * 1000) + 5000
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 100,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "direct_openclaw",
            "description": "direct case",
            "chat_type": "direct",
            "target_user_id": "openclaw-user",
            "conversation_id": "openclaw-user",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "openclaw-user",
            "require_mapping": True,
        }
        reply_messages = {
            "data": {
                "messages": [
                    {
                        "msg_id": "reply-1",
                        "from_xid": {"uid": "openclaw-user"},
                        "to_xid": {"uid": "sender-user"},
                        "content": "reply-1",
                        "ctype": "TEXT",
                        "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat:direct:sender-user", "request_msg_id": "request-1"}}, ensure_ascii=False),
                        "timestamp": reply_timestamp,
                    },
                    {
                        "msg_id": "reply-2",
                        "from_xid": {"uid": "openclaw-user"},
                        "to_xid": {"uid": "sender-user"},
                        "content": "reply-1",
                        "ctype": "TEXT",
                        "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat:direct:sender-user", "trigger_msg_id": "wrong-request"}}, ensure_ascii=False),
                        "timestamp": reply_timestamp + 1,
                    },
                ]
            }
        }
        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value=reply_messages), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", return_value={
                 "session_key": "agent:main:clawchat:direct:sender-user",
                 "sender_user_id": "",
                 "origin_user_id": "sender-user",
                 "openclaw_user_id": "openclaw-user",
                 "updated_at": 2,
             }), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_FAILED)
        self.assertIn("重复消息", result["failure_reason"])
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertEqual(comparison["matched_visible_reply_count"], 2)
        self.assertTrue(comparison["duplicate_reply_detected"])
        self.assertTrue(comparison["duplicate_content_fallback_used"])

    def test_execute_subagent_scenario_requires_root_and_subagent_replies(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "direct_chatbot_subagent",
            "description": "subagent case",
            "chat_type": "direct",
            "target_user_id": "chatbot-user",
            "conversation_id": "chatbot-user",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "chatbot-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_direct_chatbot_subagent_1000000"
        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-root",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": "Spawned subagent main",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat-router:direct:sender-user", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                         {
                             "msg_id": "reply-sub-task",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": "[Subagent Task] 请只回复指定标记",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 1,
                         },
                         {
                             "msg_id": "reply-sub",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 2,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={"data": {"messages": []}}), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=[
                 {
                     "session_key": "agent:main:clawchat-router:direct:sender-user",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:clawchat-router:direct:sender-user",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:subagent:test-child",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
                     "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                     "updated_at": 3,
                 },
             ]), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_PASSED)
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertEqual(comparison["root_visible_reply_sessions"], ["agent:main:clawchat-router:direct:sender-user"])
        self.assertEqual(comparison["subagent_visible_reply_sessions"], ["agent:main:subagent:test-child"])
        self.assertEqual(comparison["subagent_visible_reply_count"], 2)
        self.assertEqual(comparison["subagent_visible_reply_msg_ids"], ["reply-sub-task", "reply-sub"])
        self.assertTrue(comparison["subagent_task_message_found"])
        self.assertTrue(comparison["subagent_marker_found"])
        self.assertTrue(comparison["subagent_result_message_found"])
        self.assertTrue(comparison["subagent_lineage_ok"])

    def test_execute_subagent_scenario_observes_child_conversation_messages(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "group_chatbot_subagent",
            "description": "subagent case",
            "chat_type": "group",
            "target_user_id": "chatbot-user",
            "conversation_id": "group-root",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "chatbot-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_group_chatbot_subagent_1000000"
        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-root",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": "Spawned subagent main",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat-router:group:group-root", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-sub-task",
                             "from_xid": {"uid": "sender-user"},
                             "to_xid": {"uid": "chatbot-user"},
                             "content": "[Subagent Task] 请只回复指定标记",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 1,
                         },
                         {
                             "msg_id": "reply-sub",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 2,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[
                 {
                     "session_key": "agent:main:subagent:test-child",
                     "group_id": "group-child",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "root_session_key": "agent:main:clawchat-router:group:group-root",
                     "parent_session_key": "agent:main:clawchat-router:group:group-root",
                     "updated_at": reply_timestamp + 10,
                 }
             ]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=[
                 {
                     "session_key": "agent:main:clawchat-router:group:group-root",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:clawchat-router:group:group-root",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:subagent:test-child",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "parent_session_key": "agent:main:clawchat-router:group:group-root",
                     "root_session_key": "agent:main:clawchat-router:group:group-root",
                     "updated_at": 3,
                 },
             ]), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_PASSED)
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertEqual(comparison["root_visible_reply_sessions"], ["agent:main:clawchat-router:group:group-root"])
        self.assertEqual(comparison["subagent_visible_reply_sessions"], ["agent:main:subagent:test-child"])
        self.assertEqual(comparison["subagent_observed_conversation_ids"], ["group-child"])
        self.assertEqual(comparison["subagent_visible_reply_msg_ids"], ["reply-sub-task", "reply-sub"])
        self.assertTrue(comparison["subagent_task_message_found"])
        self.assertTrue(comparison["subagent_result_message_found"])
        self.assertTrue(comparison["subagent_lineage_ok"])

    def test_execute_subagent_scenario_fails_without_subagent_task_message(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "direct_chatbot_subagent",
            "description": "subagent case",
            "chat_type": "direct",
            "target_user_id": "chatbot-user",
            "conversation_id": "chatbot-user",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "chatbot-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_direct_chatbot_subagent_1000000"
        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-root",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": "Spawned subagent main",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat-router:direct:sender-user", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                         {
                             "msg_id": "reply-sub",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 1,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={"data": {"messages": []}}), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=[
                 {
                     "session_key": "agent:main:clawchat-router:direct:sender-user",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:clawchat-router:direct:sender-user",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:subagent:test-child",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
                     "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                     "updated_at": 3,
                 },
             ]), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)
        self.assertEqual(result["status"], m.STATUS_FAILED)
        self.assertIn("root/sub session", result["failure_reason"])
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertEqual(comparison["subagent_visible_reply_count"], 1)
        self.assertFalse(comparison["subagent_task_message_found"])
        self.assertTrue(comparison["subagent_result_message_found"])
        self.assertFalse(comparison["subagent_lineage_ok"])

    def test_execute_subagent_scenario_accepts_parent_relay_marker_without_child_visible_sync(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "group_chatbot_subagent",
            "description": "subagent parent relay case",
            "chat_type": "group",
            "target_user_id": "chatbot-user",
            "conversation_id": "group-root",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "chatbot-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_group_chatbot_subagent_1000000"
        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-root",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat-router:group:group-root", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={"data": {"messages": []}}), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=[
                 {
                     "session_key": "agent:main:clawchat-router:group:group-root",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
                 {
                     "session_key": "agent:main:clawchat-router:group:group-root",
                     "sender_user_id": "",
                     "origin_user_id": "sender-user",
                     "openclaw_user_id": "chatbot-user",
                     "updated_at": 2,
                 },
             ]), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)

        self.assertEqual(result["status"], m.STATUS_PASSED)
        self.assertEqual(result["notes"], "latest_openclaw_parent_relay_mode")
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertTrue(comparison["parent_relay_marker_found"])
        self.assertTrue(comparison["subagent_parent_relay_ok"])
        self.assertFalse(comparison["subagent_lineage_ok"])

    def test_execute_subagent_scenario_infers_root_session_from_child_mapping(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "group_openclaw_subagent",
            "description": "subagent inferred root case",
            "chat_type": "group",
            "target_user_id": "openclaw-user",
            "conversation_id": "group-root",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "openclaw-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_group_openclaw_subagent_1000000"
        child_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-child",
            "sender_user_id": "",
            "origin_user_id": "sender-user",
            "openclaw_user_id": "openclaw-user",
            "parent_session_key": "agent:main:clawchat:group:group-root",
            "root_session_key": "agent:main:clawchat:group:group-root",
            "updated_at": reply_timestamp + 10,
        }
        root_mapping = {
            "session_key": "agent:main:clawchat:group:group-root",
            "sender_user_id": "",
            "origin_user_id": "sender-user",
            "openclaw_user_id": "openclaw-user",
            "updated_at": reply_timestamp + 5,
        }

        def get_mapping(_app_id, _node_id, session_key):
            if session_key == "agent:main:subagent:test-child":
                return dict(child_mapping)
            if session_key == "agent:main:clawchat:group:group-root":
                return dict(root_mapping)
            return None

        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-parent-relay",
                             "from_xid": {"uid": "openclaw-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-sub-task",
                             "from_xid": {"uid": "sender-user"},
                             "to_xid": {"uid": "openclaw-user"},
                             "content": "[Subagent Task] 请只回复指定标记",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 1,
                         },
                         {
                             "msg_id": "reply-sub",
                             "from_xid": {"uid": "openclaw-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 2,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[dict(child_mapping)]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=get_mapping), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)

        self.assertEqual(result["status"], m.STATUS_PASSED)
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertEqual(comparison["root_visible_reply_sessions"], [])
        self.assertEqual(comparison["inferred_root_visible_reply_sessions"], ["agent:main:clawchat:group:group-root"])
        self.assertEqual(comparison["subagent_visible_reply_sessions"], ["agent:main:subagent:test-child"])
        self.assertTrue(comparison["subagent_lineage_ok"])
        self.assertTrue(comparison["mapping_sender_ok"])

    def test_execute_subagent_scenario_resolves_child_sender_from_parent_mapping(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = 1000001
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "subagent_timeout_ms": 100,
            "poll_interval_ms": 10,
            "duplicate_observation_window_ms": 0,
            "provisioning_rows": [],
            "sender_username": "sender-name",
            "sender_password": "sender-pass",
        }
        scenario_def = {
            "name": "direct_chatbot_subagent",
            "description": "subagent inherited sender case",
            "chat_type": "direct",
            "target_user_id": "chatbot-user",
            "conversation_id": "chatbot-user",
            "sender_user_id": "sender-user",
            "expected_reply_user_id": "chatbot-user",
            "require_mapping": True,
            "expect_root_and_sub_sessions": True,
        }
        marker = "SYNC_OK_direct_chatbot_subagent_1000000"
        child_mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "group-child",
            "sender_user_id": "",
            "origin_user_id": "",
            "openclaw_user_id": "chatbot-user",
            "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "updated_at": reply_timestamp + 10,
        }
        root_mapping = {
            "session_key": "agent:main:clawchat-router:direct:sender-user",
            "sender_user_id": "",
            "origin_user_id": "sender-user",
            "openclaw_user_id": "chatbot-user",
            "updated_at": reply_timestamp + 5,
        }

        def get_mapping(_app_id, _node_id, session_key):
            if session_key == "agent:main:subagent:test-child":
                return dict(child_mapping)
            if session_key == "agent:main:clawchat-router:direct:sender-user":
                return dict(root_mapping)
            return None

        fake_send_result = {"http_status": 200, "result": {"code": 200, "msg_ids": ["request-1"]}}
        with mock.patch.object(m, "send_message", return_value=fake_send_result), \
             mock.patch.object(m, "fetch_conversation", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-root",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": "Spawned subagent main",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:clawchat-router:direct:sender-user", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "fetch_conversation_view", return_value={
                 "data": {
                     "messages": [
                         {
                             "msg_id": "reply-sub-task",
                             "from_xid": {"uid": "sender-user"},
                             "to_xid": {"uid": "chatbot-user"},
                             "content": "[Subagent Task] 请只回复指定标记",
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 1,
                         },
                         {
                             "msg_id": "reply-sub",
                             "from_xid": {"uid": "chatbot-user"},
                             "to_xid": {"uid": "sender-user"},
                             "content": marker,
                             "ctype": "TEXT",
                             "ext": json.dumps({"openclaw": {"type": "session_sync_delivery", "session": "agent:main:subagent:test-child", "request_msg_id": "request-1"}}, ensure_ascii=False),
                             "timestamp": reply_timestamp + 2,
                         },
                     ]
                 }
             }), \
             mock.patch.object(m, "select_relevant_mappings", return_value=[]), \
             mock.patch.object(m.core, "list_session_mappings_for_node", return_value=[dict(child_mapping)]), \
             mock.patch.object(m.core, "get_session_mapping_by_session", side_effect=get_mapping), \
             mock.patch.object(m, "build_mapping_rows", return_value=[]), \
             mock.patch.object(m.time, "time", return_value=1000.0):
            result = m.execute_scenario(runtime, scenario_def)

        self.assertEqual(result["status"], m.STATUS_PASSED)
        comparison = {row["label"]: row["value"] for row in result["comparison_rows"]}
        self.assertTrue(comparison["mapping_sender_ok"])
        self.assertTrue(comparison["subagent_lineage_ok"])

    def test_merge_message_snapshots_keeps_all_observed_messages(self):
        m = _load_openclaw_sync_validation()
        merged = m.merge_message_snapshots(
            [
                {"msg_id": "request-1", "timestamp": 1000, "content": "request"},
            ],
            [
                {"msg_id": "request-1", "timestamp": 1000, "content": "request"},
                {"msg_id": "reply-1", "timestamp": 2000, "content": "reply"},
            ],
        )
        self.assertEqual([item["msg_id"] for item in merged], ["request-1", "reply-1"])

    def test_start_and_run_sync_validation_task_writes_report_and_updates_status(self):
        m = _load_openclaw_sync_validation()
        with tempfile.TemporaryDirectory() as tempdir, \
             mock.patch.object(m, "get_base_dir", return_value=tempdir), \
             mock.patch.object(m, "build_runtime", return_value={
                 "result": "ok",
                 "data": {
                     "app_id": "app-id",
                     "node_id": "node-1",
                    "config": {"lanying_admin_token": "admin-token"},
                    "sender_user_id": "sender-user",
                    "sender_username": "sender-name",
                    "sender_password": "sender-pass",
                    "provisioning_rows": [],
                    "timeout_ms": 100,
                    "poll_interval_ms": 10,
                    "duplicate_observation_window_ms": 0,
                },
             }), \
             mock.patch.object(m, "build_scenario_definition", return_value={
                 "name": "direct_openclaw",
                 "description": "direct case",
                 "sender_user_id": "sender-user",
                 "chat_type": "direct",
                 "target_user_id": "openclaw-user",
                 "conversation_id": "openclaw-user",
                 "expected_reply_user_id": "openclaw-user",
                 "require_mapping": True,
             }), \
             mock.patch.object(m, "execute_scenario", return_value={
                 "status": m.STATUS_PASSED,
                 "failure_reason": "",
                 "participant_rows": [],
                 "request_rows": [],
                 "comparison_rows": [],
                 "messages": [],
                 "mapping_rows": [],
                 "notes": "",
             }):
            result = m.start("app-id", "node-1", "direct_openclaw", None)
            self.assertEqual(result["result"], "ok")
            task_id = result["data"]["task_id"]
            task = m.get_task(task_id)
            self.assertEqual(task["status"], m.STATUS_PENDING)
            self.assertTrue(pathlib.Path(task["report_path"]).exists())
            m.run_task(task_id)
            self.assertEqual(task["status"], m.STATUS_PASSED)
            report_text = pathlib.Path(task["report_path"]).read_text(encoding="utf-8")
            self.assertIn("direct_openclaw", report_text)
            self.assertIn("PASSED", report_text.upper())

    def test_run_sync_validation_task_surfaces_failure_reason_in_report(self):
        m = _load_openclaw_sync_validation()
        with tempfile.TemporaryDirectory() as tempdir, \
             mock.patch.object(m, "get_base_dir", return_value=tempdir), \
             mock.patch.object(m, "build_runtime", return_value={
                 "result": "ok",
                 "data": {
                     "app_id": "app-id",
                     "node_id": "node-1",
                    "config": {"lanying_admin_token": "admin-token"},
                    "sender_user_id": "sender-user",
                    "sender_username": "sender-name",
                    "sender_password": "sender-pass",
                    "provisioning_rows": [],
                    "timeout_ms": 100,
                    "poll_interval_ms": 10,
                    "duplicate_observation_window_ms": 0,
                },
             }), \
             mock.patch.object(m, "build_scenario_definition", return_value={
                 "name": "group_chatbot",
                 "description": "group chatbot case",
                 "sender_user_id": "sender-user",
                 "chat_type": "group",
                 "target_user_id": "chatbot-user",
                 "conversation_id": "group-1",
                 "expected_reply_user_id": "chatbot-user",
                 "require_mapping": True,
             }), \
             mock.patch.object(m, "execute_scenario", return_value={
                 "status": m.STATUS_FAILED,
                 "failure_reason": "session mapping 未体现正确发送者身份",
                 "participant_rows": [],
                 "request_rows": [],
                 "comparison_rows": [],
                 "messages": [],
                 "mapping_rows": [],
                 "notes": "",
             }):
            result = m.start("app-id", "node-1", "group_chatbot", None)
            task_id = result["data"]["task_id"]
            task = m.get_task(task_id)
            m.run_task(task_id)
            self.assertEqual(task["status"], m.STATUS_FAILED)
            report_text = pathlib.Path(task["report_path"]).read_text(encoding="utf-8")
            self.assertIn("session mapping", report_text)
            self.assertIn("group_chatbot", report_text)

    @unittest.skipUnless(importlib.util.find_spec("flask") is not None, "flask not installed")
    def test_openclaw_service_sync_validation_route_returns_html(self):
        openclaw_module = _load_lanying_openclaw()
        sync_validation_module = _load_openclaw_sync_validation()
        service_module = _load_openclaw_service(openclaw_module, sync_validation_module)
        with mock.patch.object(service_module, "check_access_token_valid", return_value=True), \
             mock.patch.object(sync_validation_module, "start", return_value={
                 "result": "ok",
                 "data": {
                     "task_id": "task-1",
                     "task_dir": "/tmp/task-1",
                     "report_path": "/tmp/task-1/report.html",
                     "status_url": "/service/openclaw/sync_validation/task-1",
                 },
             }), \
             mock.patch.object(sync_validation_module, "get_task", return_value={
                 "task_id": "task-1",
                 "status": "pending",
                 "app_id": "app-id",
                 "node_id": "node-1",
                 "task_dir": "/tmp/task-1",
             }):
            app = service_module.bp
            flask_app = __import__("flask").Flask(__name__)
            flask_app.register_blueprint(app)
            client = flask_app.test_client()
            response = client.post(
                "/service/openclaw/run_sync_validation",
                data=json.dumps({"app_id": "app-id", "node_id": "node-1", "scenario": "direct_openclaw"}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("text/html", response.content_type)
            self.assertIn("OpenClaw Sync Validation Task", response.get_data(as_text=True))

    @unittest.skipUnless(importlib.util.find_spec("flask") is not None, "flask not installed")
    def test_openclaw_service_sync_validation_route_returns_html_500_on_exception(self):
        openclaw_module = _load_lanying_openclaw()
        sync_validation_module = _load_openclaw_sync_validation()
        service_module = _load_openclaw_service(openclaw_module, sync_validation_module)
        with mock.patch.object(service_module, "check_access_token_valid", return_value=True), \
             mock.patch.object(sync_validation_module, "start", side_effect=RuntimeError("boom")):
            app = service_module.bp
            flask_app = __import__("flask").Flask(__name__)
            flask_app.register_blueprint(app)
            client = flask_app.test_client()
            response = client.post(
                "/service/openclaw/run_sync_validation",
                data=json.dumps({"app_id": "app-id", "node_id": "node-1", "scenario": "direct_openclaw"}),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 500)
            self.assertIn("text/html", response.content_type)
            self.assertIn("run sync validation crashed", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
