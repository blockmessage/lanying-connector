import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock


def _fake_requests_call(*args, **kwargs):
    raise RuntimeError("fake requests module: please mock requests.request/post in tests")


def _load_lanying_openclaw():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw.py"
    module_name = "lanying_openclaw_router_identity_test"
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
        ),
        "lanying_chatbot": types.SimpleNamespace(
            get_chatbot=lambda app_id, chatbot_id: {"user_id": "chatbot-user"},
        ),
        "lanying_im_api": types.SimpleNamespace(
            send_message_sync=lambda *args, **kwargs: 1,
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=lambda raw, default=None: default if default is not None else {},
        ),
        "lanying_vendor": types.SimpleNamespace(),
        "requests": types.SimpleNamespace(
            post=_fake_requests_call,
            get=_fake_requests_call,
            request=_fake_requests_call,
        ),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_openclaw = _load_lanying_openclaw()


class RouterSessionIdentityTests(unittest.TestCase):
    def test_router_child_mapping_uses_sender_and_bound_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}
        lineage = {
            "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_node_chatbot_id", return_value="chatbot-id"), \
             mock.patch.object(m.lanying_chatbot, "get_chatbot", return_value={"user_id": "chatbot-user"}):
            inherited = m.resolve_inherited_sender_user_id(
                "app-id",
                node_info,
                lineage,
                "management-user",
                "sender-user",
            )
            decision = m.resolve_session_mapping_decision(
                "agent:main:subagent:test-child",
                lineage,
                False,
                inherited,
                "management-user",
                "openclaw-user",
            )
            payload = m.build_session_mapping_payload(
                "app-id",
                "15",
                "openclaw-user",
                "management-user",
                "agent:main:subagent:test-child",
                "group-1",
                lineage,
                "agent:main:subagent:test-child",
                inherited,
            )

        self.assertEqual(inherited["sender_user_id"], "sender-user")
        self.assertEqual(inherited["chatbot_user_id"], "chatbot-user")
        self.assertEqual(decision["owner_user_id"], "chatbot-user")
        self.assertEqual(decision["sender_user_id"], "sender-user")
        self.assertEqual(decision["chatbot_user_id"], "chatbot-user")
        self.assertEqual(payload["sender_user_id"], "sender-user")
        self.assertEqual(payload["chatbot_user_id"], "chatbot-user")

    def test_direct_root_child_mapping_owner_prefers_openclaw_user(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:direct:sender-user",
            "root_session_key": "agent:main:clawchat:direct:sender-user",
        }
        inherited = {
            "sender_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_group_root_child_mapping_owner_prefers_openclaw_user(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        inherited = {
            "sender_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "openclaw-user",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "openclaw-user")

    def test_non_router_owner_falls_back_to_management_when_openclaw_missing(self):
        m = lanying_openclaw
        lineage = {
            "parent_session_key": "agent:main:clawchat:group:group-1",
            "root_session_key": "agent:main:clawchat:group:group-1",
        }
        inherited = {
            "sender_user_id": "sender-user",
            "chatbot_user_id": "",
        }

        decision = m.resolve_session_mapping_decision(
            "agent:main:subagent:test-child",
            lineage,
            False,
            inherited,
            "management-user",
            "",
        )

        self.assertEqual(decision["mode"], "create_temp_group")
        self.assertEqual(decision["owner_user_id"], "management-user")

    def test_router_group_members_include_sender_and_chatbot_only(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-1",
                "openclaw-user",
                "management-user",
                {
                    "sender_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                },
                {
                    "channel": "clawchat-router",
                    "chat_type": "direct",
                    "target_id": "sender-user",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "sender-user", "group-1"),
                ("app-id", "chatbot-user", "group-1"),
            ],
        )

    def test_direct_root_child_group_members_include_sender_and_openclaw_only(self):
        m = lanying_openclaw
        joined_users = []

        def _record_join(app_id, user_id, group_id):
            joined_users.append((app_id, user_id, group_id))
            return True

        with mock.patch.object(m, "ensure_user_joined_group", side_effect=_record_join):
            result = m.ensure_session_mapping_group_members(
                "app-id",
                "group-2",
                "openclaw-user",
                "management-user",
                {
                    "sender_user_id": "sender-user",
                    "chatbot_user_id": "",
                },
                {
                    "channel": "clawchat",
                    "chat_type": "direct",
                    "target_id": "sender-user",
                },
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(
            joined_users,
            [
                ("app-id", "sender-user", "group-2"),
                ("app-id", "openclaw-user", "group-2"),
            ],
        )

    def test_router_assistant_forwarding_uses_chatbot_user(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=123) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-1",
                    "sender_user_id": "sender-user",
                    "chatbot_user_id": "chatbot-user",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat-router:direct:sender-user",
                },
                "assistant",
                "hello from child",
            )

        self.assertEqual(mocked_send.call_args.args[2], "chatbot-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-1")

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=456) as mocked_send:
            m.forward_session_sync_to_direct(
                "app-id",
                node_info,
                "sender-user",
                "sender-user",
                "chatbot-user",
                "assistant",
                "hello direct",
            )

        self.assertEqual(mocked_send.call_args.args[2], "chatbot-user")
        self.assertEqual(mocked_send.call_args.args[3], "sender-user")

    def test_direct_root_assistant_forwarding_uses_openclaw_user_in_group(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=321) as mocked_send:
            m.forward_session_sync_to_group(
                "app-id",
                node_info,
                {
                    "session_key": "agent:main:subagent:test-child",
                    "group_id": "group-1",
                    "sender_user_id": "sender-user",
                    "chatbot_user_id": "",
                    "management_user_id": "management-user",
                    "root_session_key": "agent:main:clawchat:direct:sender-user",
                },
                "assistant",
                "hello from child",
            )

        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "group-1")

    def test_router_mapping_signal_carries_chatbot_user_id(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=1) as mocked_send:
            result = m.send_session_mapping_signal(
                node_info,
                "session_mapping_sync",
                [
                    {
                        "session_key": "agent:main:subagent:test-child",
                        "group_id": "group-1",
                        "openclaw_user_id": "openclaw-user",
                        "chatbot_user_id": "chatbot-user",
                    }
                ],
            )

        self.assertEqual(result["result"], "ok")
        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(
            ext["openclaw"]["mappings"][0]["chatbot_user_id"],
            "chatbot-user",
        )

    def test_router_group_materialization_uses_chatbot_user(self):
        m = lanying_openclaw

        with mock.patch.object(m, "ensure_user_joined_group", return_value=True) as mocked_join:
            result = m.maybe_materialize_existing_clawchat_group_mapping(
                "app-id",
                {
                    "session_key": "agent:main:clawchat-router:group:group-1",
                    "group_id": "group-1",
                    "chatbot_user_id": "chatbot-user",
                },
                "management-user",
                True,
            )

        self.assertEqual(result["result"], "ok")
        self.assertEqual(mocked_join.call_args.args[1], "chatbot-user")

    def test_router_direct_assistant_reply_uses_router_reply_signal(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}

        with mock.patch.object(m.lanying_config, "get_lanying_admin_token", return_value="admin-token"), \
             mock.patch.object(m.lanying_im_api, "send_message_sync", return_value=99) as mocked_send:
            result = m.forward_session_sync_router_direct_reply(
                "app-id",
                node_info,
                "sender-user",
                "child reply",
            )

        self.assertEqual(result, 99)
        self.assertEqual(mocked_send.call_args.args[2], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[3], "openclaw-user")
        self.assertEqual(mocked_send.call_args.args[5], 6)
        ext = mocked_send.call_args.args[7]["ext"]
        self.assertEqual(ext["openclaw"]["type"], "router_reply")
        self.assertEqual(ext["openclaw"]["message"]["to"], "sender-user")
        self.assertEqual(ext["openclaw"]["message"]["toType"], "roster")

    def test_router_root_direct_assistant_sync_prefers_router_reply(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "sender_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_direct_reply", return_value=1) as mocked_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "child reply",
                    },
                },
            )

        mocked_router_reply.assert_called_once()
        mocked_direct.assert_not_called()

    def test_nested_assistant_reply_prefers_parent_group_over_direct_root(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        child_mapping = {
            "session_key": "agent:main:subagent:test-grandchild",
            "parent_session_key": "agent:main:subagent:test-parent",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
            "effective_target_session_key": "agent:main:clawchat-router:direct:sender-user",
        }
        parent_mapping = {
            "session_key": "agent:main:subagent:test-parent",
            "group_id": "parent-group",
        }

        def _mapping_lookup(app_id, node_id, session_key):
            if session_key == "agent:main:subagent:test-grandchild":
                return child_mapping
            if session_key == "agent:main:subagent:test-parent":
                return parent_mapping
            return None

        with mock.patch.object(m, "get_session_mapping_by_session", side_effect=_mapping_lookup), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "direct",
                 "target_user_id": "sender-user",
                 "sender_user_id": "sender-user",
                 "chatbot_user_id": "chatbot-user",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_router_direct_reply") as mocked_direct_router_reply, \
             mock.patch.object(m, "forward_session_sync_to_direct") as mocked_direct, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-grandchild",
                    "message": {
                        "role": "assistant",
                        "content": "nested child reply",
                    },
                },
            )

        mocked_group_reply.assert_not_called()
        mocked_direct_router_reply.assert_not_called()
        mocked_direct.assert_not_called()
        mocked_group.assert_called_once_with(
            "app-id",
            node_info,
            parent_mapping,
            "assistant",
            "nested child reply",
        )

    def test_router_group_root_assistant_sync_prefers_router_reply(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "reply back to router group",
                    },
                },
            )

        mocked_group_reply.assert_called_once()
        forwarded_mapping = mocked_group_reply.call_args.args[2]
        self.assertEqual(forwarded_mapping["session_key"], "agent:main:clawchat-router:group:group-42")
        mocked_group_forward.assert_not_called()

    def test_router_group_child_group_assistant_sync_stays_in_child_group(self):
        m = lanying_openclaw
        node_info = {"app_id": "app-id", "node_id": "15", "user_id": "openclaw-user"}
        mapping = {
            "session_key": "agent:main:subagent:test-child",
            "group_id": "temporary-session-group",
            "root_session_key": "agent:main:clawchat-router:group:group-42",
            "effective_target_session_key": "agent:main:subagent:test-child",
        }

        with mock.patch.object(m, "get_session_mapping_by_session", return_value=mapping), \
             mock.patch.object(m, "resolve_effective_session_sync_target", return_value={
                 "kind": "group",
                 "mapping": mapping,
                 "session_key": "agent:main:subagent:test-child",
             }), \
             mock.patch.object(m, "forward_session_sync_router_group_reply", return_value=1) as mocked_group_reply, \
             mock.patch.object(m, "forward_session_sync_to_group") as mocked_group_forward:
            m.handle_session_message_sync_event(
                "app-id",
                node_info,
                {
                    "source": "control_ui_reply",
                    "session": "agent:main:subagent:test-child",
                    "message": {
                        "role": "assistant",
                        "content": "reply should stay in child group",
                    },
                },
            )

        mocked_group_reply.assert_not_called()
        mocked_group_forward.assert_called_once()

    def test_router_child_mapping_errors_without_bound_chatbot(self):
        m = lanying_openclaw
        node_info = {"node_id": "15", "user_id": "openclaw-user", "name": "OpenClaw-15"}
        lineage = {
            "session_key": "agent:main:subagent:test-child",
            "parent_session_key": "agent:main:clawchat-router:direct:sender-user",
            "root_session_key": "agent:main:clawchat-router:direct:sender-user",
        }

        with mock.patch.object(m, "ensure_openclaw_app_manager_user", return_value={"result": "ok", "data": {"user_id": "management-user"}}), \
             mock.patch.object(m, "resolve_session_lineage", return_value=lineage), \
             mock.patch.object(m, "is_merge_sub_sessions_enabled", return_value=False), \
             mock.patch.object(m, "prewarm_ancestor_session_mappings"), \
             mock.patch.object(m, "get_session_mapping_by_session", return_value=None), \
             mock.patch.object(m, "get_node_chatbot_id", return_value=""), \
             mock.patch.object(m, "create_openclaw_session_group") as mocked_create_group:
            result = m.ensure_session_mapping(
                "app-id",
                node_info,
                "agent:main:subagent:test-child",
                lineage["parent_session_key"],
                lineage["root_session_key"],
                "sender-user",
            )

        self.assertEqual(result["result"], "error")
        self.assertEqual(result["message"], "router chatbot user not ready")
        mocked_create_group.assert_not_called()


if __name__ == "__main__":
    unittest.main()
