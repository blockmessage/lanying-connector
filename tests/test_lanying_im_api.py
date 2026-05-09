import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock


def _load_lanying_im_api():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "lanying_im_api.py"
    module_name = "lanying_im_api_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    stub_modules = {
        "lanying_config": types.SimpleNamespace(
            get_lanying_connector=lambda app_id: {"lanying_admin_token": "admin-token"},
            get_lanying_api_endpoint=lambda app_id: "https://api.example.com",
        ),
        "lanying_file_storage": types.SimpleNamespace(),
        "lanying_utils": types.SimpleNamespace(is_lanying_url=lambda url: True),
        "lanying_redis": types.SimpleNamespace(
            get_redis_connection=lambda: object(),
            redis_get=lambda *args, **kwargs: None,
        ),
        "lanying_async": types.SimpleNamespace(
            executor=types.SimpleNamespace(submit=lambda *args, **kwargs: None),
        ),
        "requests": types.SimpleNamespace(
            get=lambda *args, **kwargs: None,
            post=lambda *args, **kwargs: None,
        ),
    }
    with mock.patch.dict(sys.modules, stub_modules):
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


lanying_im_api = _load_lanying_im_api()


class LanyingImApiTests(unittest.TestCase):
    def test_get_group_info_passes_group_id_as_query_param(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200, "data": {"group_id": "9"}}

        with mock.patch.object(lanying_im_api.requests, "get", return_value=fake_response) as mocked_get:
            result = lanying_im_api.get_group_info("app-id", "9")

        self.assertEqual(result["code"], 200)
        mocked_get.assert_called_once_with(
            "https://api.example.com/group/info",
            headers={"app_id": "app-id", "group_id": "9", "access-token": "admin-token"},
            params={"group_id": "9"},
        )

    def test_group_owner_transfer_uses_current_owner_header_and_new_owner_body(self):
        fake_response = mock.Mock()
        fake_response.json.return_value = {"code": 200}

        with mock.patch.object(lanying_im_api.requests, "post", return_value=fake_response) as mocked_post:
            result = lanying_im_api.group_owner_transfer("app-id", "9", "100", "200")

        self.assertEqual(result["code"], 200)
        mocked_post.assert_called_once_with(
            "https://api.example.com/group/transfer",
            headers={"app_id": "app-id", "group_id": "9", "user_id": "100", "access-token": "admin-token"},
            json={"group_id": "9", "new_owner": 200},
        )


if __name__ == "__main__":
    unittest.main()
