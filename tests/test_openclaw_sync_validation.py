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

    def test_normalize_sync_validation_scenarios_supports_all_and_single(self):
        m = _load_openclaw_sync_validation()
        self.assertEqual(
            m.normalize_scenarios("all", None),
            ["group_openclaw", "group_chatbot", "direct_openclaw", "direct_chatbot"],
        )
        self.assertEqual(
            m.normalize_scenarios("direct_openclaw", None),
            ["direct_openclaw"],
        )

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

    def test_execute_scenario_prefers_exact_reply_session_mapping_for_sender_check(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = int(__import__("time").time() * 1000) + 5000
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "poll_interval_ms": 10,
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

    def test_execute_scenario_accepts_origin_user_id_for_mapping_sender_check(self):
        m = _load_openclaw_sync_validation()
        reply_timestamp = int(__import__("time").time() * 1000) + 5000
        runtime = {
            "app_id": "app-id",
            "node_id": "node-1",
            "timeout_ms": 100,
            "poll_interval_ms": 10,
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
