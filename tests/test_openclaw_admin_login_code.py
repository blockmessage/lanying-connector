import importlib.util
import json
import pathlib
import sys
import types
import unittest
from unittest import mock


def _load_lanying_openclaw():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_openclaw.py"
    module_name = "lanying_openclaw_admin_login_code_test"
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
            get_chatbot=lambda *args, **kwargs: None,
        ),
        "lanying_im_api": types.SimpleNamespace(
            token_user=lambda *args, **kwargs: {"code": 200, "data": {"token": "stub-token"}},
            generate_secret_info=lambda *args, **kwargs: {"code": 200, "data": {"code": "stub"}},
        ),
        "lanying_utils": types.SimpleNamespace(
            safe_json_loads=lambda raw, default=None: default if default is not None else {},
        ),
        "lanying_vendor": types.SimpleNamespace(),
        "lanying_pgvector": types.SimpleNamespace(
            append_openclaw_session_map_log=lambda entry: {"result": "ignored", "message": "test stub"},
        ),
        "requests": types.SimpleNamespace(
            post=lambda *args, **kwargs: None,
            get=lambda *args, **kwargs: None,
            request=lambda *args, **kwargs: None,
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


class OpenClawAdminLoginCodeTests(unittest.TestCase):
    def test_generate_app_manager_login_code_uses_manager_credentials(self):
        with mock.patch.object(
            lanying_openclaw,
            "ensure_openclaw_app_manager_user",
            return_value={
                "result": "ok",
                "data": {
                    "user_id": "9876",
                    "username": "openclaw_admin_demo",
                    "password": "secret-pass",
                },
            },
        ), mock.patch.object(
            lanying_openclaw.lanying_im_api,
            "generate_secret_info",
            return_value={"code": 200, "data": {"code": "login-code-123"}},
        ) as mocked_generate:
            result = lanying_openclaw.generate_openclaw_app_manager_login_code("app-demo", 300)

        self.assertEqual(result, {"result": "ok", "data": {"code": "login-code-123"}})
        mocked_generate.assert_called_once_with(
            "app-demo",
            "9876",
            300,
            json.dumps({"app_id": "app-demo", "username": "openclaw_admin_demo", "password": "secret-pass"}, ensure_ascii=False),
        )

    def test_generate_app_manager_login_code_rejects_invalid_manager_info(self):
        with mock.patch.object(
            lanying_openclaw,
            "ensure_openclaw_app_manager_user",
            return_value={
                "result": "ok",
                "data": {
                    "user_id": "",
                    "username": "openclaw_admin_demo",
                    "password": "secret-pass",
                },
            },
        ):
            result = lanying_openclaw.generate_openclaw_app_manager_login_code("app-demo", 0)

        self.assertEqual(result["result"], "error")
        self.assertIn("invalid", result["message"])

    def test_generate_app_manager_login_code_normalizes_non_positive_expire_seconds(self):
        with mock.patch.object(
            lanying_openclaw,
            "ensure_openclaw_app_manager_user",
            return_value={
                "result": "ok",
                "data": {
                    "user_id": "9876",
                    "username": "openclaw_admin_demo",
                    "password": "secret-pass",
                },
            },
        ), mock.patch.object(
            lanying_openclaw.lanying_im_api,
            "generate_secret_info",
            return_value={"code": 200, "data": {"code": "login-code-123"}},
        ) as mocked_generate:
            result = lanying_openclaw.generate_openclaw_app_manager_login_code("app-demo", 0)

        self.assertEqual(result, {"result": "ok", "data": {"code": "login-code-123"}})
        mocked_generate.assert_called_once_with(
            "app-demo",
            "9876",
            300,
            json.dumps({"app_id": "app-demo", "username": "openclaw_admin_demo", "password": "secret-pass"}, ensure_ascii=False),
        )


if __name__ == "__main__":
    unittest.main()
