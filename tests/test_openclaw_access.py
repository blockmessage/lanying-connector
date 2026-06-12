import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock


class FakeRedis:
    def __init__(self):
        self.hashes = {}

    def hmset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)


def _load_lanying_openclaw():
    fake_redis = FakeRedis()
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw.py"
    module_name = "lanying_openclaw_access_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: fake_redis,
            redis_hget=lambda *args, **kwargs: "",
            redis_get=lambda *args, **kwargs: None,
            redis_hgetall=lambda redis, key: dict(redis.hashes.get(key, {})),
        ),
        "lanying_config": types.SimpleNamespace(
            get_lanying_admin_token=lambda app_id: "admin-token",
            get_lanying_api_endpoint=lambda app_id: "https://api.example.com",
            get_lanying_connector=lambda app_id: {"access_token": "connector-token"},
        ),
        "lanying_chatbot": types.SimpleNamespace(
            get_chatbot=lambda *args, **kwargs: None,
        ),
        "lanying_im_api": types.SimpleNamespace(
            set_user_stranger_chat=lambda *args, **kwargs: {"code": 200},
            set_auth_mode=lambda *args, **kwargs: {"code": 200},
            admin_add_roster_direct=lambda *args, **kwargs: {"code": 200},
            roster_delete=lambda *args, **kwargs: {"code": 200},
            send_message_sync=lambda *args, **kwargs: 1,
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=lambda raw, default=None: default if default is not None else {},
        ),
        "lanying_vendor": types.SimpleNamespace(list_models=lambda app_id: []),
        "lanying_pgvector": types.SimpleNamespace(),
        "requests": types.SimpleNamespace(),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module, fake_redis


class OpenClawAccessTests(unittest.TestCase):
    def setUp(self):
        self.module, self.fake_redis = _load_lanying_openclaw()

    def test_node_setting_normalizes_friend_to_non_support(self):
        setting = self.module.NodeSetting(
            app_id="app-id",
            name="node",
            product_id="1",
            charge_id="1",
            node_id="node-1",
            lanying_link="",
            access_type="friend",
            access_list="",
            chatbot_id="",
            show_in_support="on",
        )
        self.assertEqual(setting.show_in_support, "off")

    def test_get_node_defaults_legacy_public_to_show_in_support_on(self):
        key = self.module.get_node_key("app-id", "node-1")
        self.fake_redis.hmset(key, {
            "node_id": "node-1",
            "create_time": "1",
            "app_id": "app-id",
            "name": "node",
            "user_id": "1001",
            "username": "user",
            "password": "pwd",
            "access_type": "public",
            "access_list": "",
        })
        node = self.module.get_node("app-id", "node-1")
        self.assertEqual(node["show_in_support"], "on")

    def test_get_node_defaults_legacy_friend_to_show_in_support_off(self):
        key = self.module.get_node_key("app-id", "node-1")
        self.fake_redis.hmset(key, {
            "node_id": "node-1",
            "create_time": "1",
            "app_id": "app-id",
            "name": "node",
            "user_id": "1001",
            "username": "user",
            "password": "pwd",
            "access_type": "friend",
            "access_list": "",
        })
        node = self.module.get_node("app-id", "node-1")
        self.assertEqual(node["show_in_support"], "off")


if __name__ == "__main__":
    unittest.main()
