import unittest
from unittest import mock

try:
    import lanying_vendor
except ModuleNotFoundError:
    lanying_vendor = None


class _LegacyModule:
    SUPPORT_NATIVE_TOOLS = False

    @staticmethod
    def prepare_chat(auth_info, preset):
        return {"seen_preset": preset}

    @staticmethod
    def chat(prepare_info, preset, model_config):
        return {
            "result": "ok",
            "reply": "",
            "function_call": {"name": "f1", "arguments": "{}", "id": "call_1"},
            "finish_reason": "function_call",
            "usage": {"completion_tokens": 1, "prompt_tokens": 1, "total_tokens": 2},
        }


class _NativeModule:
    SUPPORT_NATIVE_TOOLS = True

    @staticmethod
    def prepare_chat(auth_info, preset):
        return {"seen_preset": preset}

    @staticmethod
    def chat(prepare_info, preset, model_config):
        return {
            "result": "ok",
            "reply": "",
            "tool_calls": [
                {
                    "id": "call_2",
                    "type": "function",
                    "function": {"name": "f2", "arguments": "{}"},
                }
            ],
            "finish_reason": "tool_calls",
            "usage": {"completion_tokens": 1, "prompt_tokens": 1, "total_tokens": 2},
        }


class _ListModelsModule:
    @staticmethod
    def list_remote_models(auth_info):
        return {
            "result": "ok",
            "status_code": 200,
            "response": {
                "data": [{"id": "model-a"}, {"id": "model-b"}],
                "seen_api_key": auth_info.get("api_key", ""),
            },
        }


@unittest.skipIf(lanying_vendor is None, "optional vendor dependencies are not installed in this environment")
class VendorBridgeTests(unittest.TestCase):
    def setUp(self):
        self.old_map = dict(lanying_vendor.vendor_to_module)
        self.old_get_chat_model_config = lanying_vendor.get_chat_model_config
        self.old_get_vendor = lanying_vendor.get_vendor

    def tearDown(self):
        lanying_vendor.vendor_to_module = self.old_map
        lanying_vendor.get_chat_model_config = self.old_get_chat_model_config
        lanying_vendor.get_vendor = self.old_get_vendor

    def _mock_model_config(self, app_id, vendor, model):
        return {"model": model, "vendor": vendor}

    def test_prepare_chat_legacy_module_receives_legacy_shape(self):
        lanying_vendor.vendor_to_module["legacy_test"] = _LegacyModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {
            "model": "m1",
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "foo", "parameters": {"type": "object"}},
                }
            ],
            "tool_choice": {"type": "function", "function": {"name": "foo"}},
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_x",
                            "type": "function",
                            "function": {"name": "foo", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_x", "content": "ok"},
            ],
        }

        out = lanying_vendor.prepare_chat("app", "legacy_test", {"api_key": "k"}, preset)
        seen = out["seen_preset"]
        self.assertIn("functions", seen)
        self.assertEqual(seen["functions"][0]["name"], "foo")
        self.assertIn("function_call", seen)
        self.assertEqual(seen["messages"][0]["function_call"]["name"], "foo")
        self.assertEqual(seen["messages"][1]["role"], "function")

    def test_prepare_chat_native_module_receives_native_shape(self):
        lanying_vendor.vendor_to_module["native_test"] = _NativeModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {
            "model": "m1",
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "foo", "parameters": {"type": "object"}},
                }
            ],
            "messages": [{"role": "user", "content": "hi"}],
        }

        out = lanying_vendor.prepare_chat("app", "native_test", {"api_key": "k"}, preset)
        seen = out["seen_preset"]
        self.assertIn("tools", seen)
        self.assertNotIn("functions", seen)

    def test_chat_normalizes_legacy_function_call_response(self):
        lanying_vendor.vendor_to_module["legacy_test"] = _LegacyModule
        lanying_vendor.get_chat_model_config = self._mock_model_config

        preset = {"model": "m1", "messages": [{"role": "user", "content": "hi"}]}
        out = lanying_vendor.chat("app", "legacy_test", {"api_key": "k"}, preset)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(out["finish_reason"], "tool_calls")
        self.assertIn("tool_calls", out)
        self.assertEqual(out["tool_calls"][0]["function"]["name"], "f1")

    def test_fetch_vendor_remote_models_returns_vendor_raw_response(self):
        lanying_vendor.vendor_to_module["list_models_test"] = _ListModelsModule
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/v1",
            model_config=[],
            config_version=2,
            handler_vendor="list_models_test",
        )

        with mock.patch.object(lanying_vendor, "normalize_vendor_setting_handler_vendor", return_value=vendor_setting), \
             mock.patch.object(lanying_vendor, "is_valid_vendor_api_endpoint", return_value=True), \
             mock.patch.object(lanying_vendor, "get_vendor_handler_vendor", return_value="list_models_test"):
            out = lanying_vendor.fetch_vendor_remote_models(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(out["status_code"], 200)
        self.assertEqual(out["response"]["data"][0]["id"], "model-a")
        self.assertEqual(out["response"]["seen_api_key"], "k")

    def test_fetch_vendor_remote_models_returns_unsupported_when_module_has_no_method(self):
        lanying_vendor.vendor_to_module["native_test"] = _NativeModule
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/v1",
            model_config=[],
            config_version=2,
            handler_vendor="native_test",
        )

        with mock.patch.object(lanying_vendor, "normalize_vendor_setting_handler_vendor", return_value=vendor_setting), \
             mock.patch.object(lanying_vendor, "is_valid_vendor_api_endpoint", return_value=True), \
             mock.patch.object(lanying_vendor, "get_vendor_handler_vendor", return_value="native_test"):
            out = lanying_vendor.fetch_vendor_remote_models(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(out["message"], "vendor_remote_model_list_not_supported")

    def test_get_module_uses_handler_vendor_for_v2_custom_vendor(self):
        lanying_vendor.vendor_to_module["native_test"] = _NativeModule
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openrouter",
            "handler_vendor": "native_test",
            "config_version": 2,
            "model_config": [],
        }

        out = lanying_vendor.get_module("app", "custom_vendor_1")
        self.assertIs(out, _NativeModule)

    def test_v2_disabled_catalog_model_is_filtered(self):
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openai",
            "handler_vendor": "openai",
            "name": "Test",
            "config_version": 2,
            "model_config": [
                {
                    "service": "chatgpt",
                    "model": "gpt-4o-mini",
                    "enabled": False,
                    "source": "catalog",
                    "extra_params": [],
                }
            ],
        }

        out = lanying_vendor.get_chat_model_config("app", "custom_vendor_2", "gpt-4o-mini")
        self.assertIsNone(out)

    def test_v2_custom_model_inherits_service_template(self):
        lanying_vendor.get_vendor = lambda app_id, vendor_id: {
            "vendor_id": vendor_id,
            "vendor_type": "openai",
            "handler_vendor": "openai",
            "name": "Test",
            "config_version": 2,
            "model_config": [
                {
                    "service": "chatgpt",
                    "model": "my-chatgpt-compatible-model",
                    "enabled": True,
                    "source": "custom",
                    "extra_params": [
                        {"key": "temperature", "value": "0.2"},
                        {"key": "reasoning", "value": "true"},
                        {"key": "input", "value": "[\"text\"]"},
                        {"key": "token_limit", "value": "128000"},
                        {"key": "max_output_tokens", "value": "8192"},
                    ],
                }
            ],
        }

        out = lanying_vendor.get_chat_model_config("app", "custom_vendor_3", "my-chatgpt-compatible-model")
        self.assertIsNotNone(out)
        self.assertEqual(out["model"], "my-chatgpt-compatible-model")
        self.assertEqual(out["vendor"], "custom_vendor_3")
        self.assertEqual(out["api_key_type"], "self")
        self.assertEqual(out["temperature"], "0.2")
        self.assertEqual(out["reasoning"], True)
        self.assertEqual(out["input"], ["text"])
        self.assertEqual(out["token_limit"], 128000)
        self.assertEqual(out["max_output_tokens"], 8192)

    def test_check_vendor_valid_normalizes_handler_vendor(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openrouter",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="",
            model_config=[],
            config_version=2,
            handler_vendor="azure",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=True), \
             mock.patch.object(lanying_vendor, "test_vendor_connection", return_value={"result": "ok"}):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(vendor_setting.handler_vendor, "openai")

    def test_check_vendor_valid_rejects_non_public_api_endpoint(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="http://127.0.0.1:5000/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=False):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(out["message"], "api_endpoint_not_valid")

    def test_check_vendor_valid_rejects_failed_vendor_connection(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.openai.com/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=True), \
             mock.patch.object(lanying_vendor, "test_vendor_connection", return_value={"result": "error", "message": "vendor_connection_test_failed"}):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(out["message"], "vendor_connection_test_failed")

    def test_check_vendor_valid_accepts_successful_vendor_connection(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.openai.com/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=True), \
             mock.patch.object(lanying_vendor, "test_vendor_connection", return_value={"result": "ok"}):
            out = lanying_vendor.check_vendor_valid(vendor_setting)

        self.assertEqual(out["result"], "ok")

    def test_check_vendor_valid_skips_connection_test_when_api_key_and_endpoint_unchanged(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.openai.com/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )
        old_vendor_info = {
            "api_key": "k",
            "api_endpoint": "https://api.openai.com/v1",
        }

        with mock.patch.object(lanying_vendor.lanying_utils, "is_valid_public_url", return_value=True), \
             mock.patch.object(lanying_vendor, "test_vendor_connection") as mocked_test:
            out = lanying_vendor.check_vendor_valid(vendor_setting, old_vendor_info)

        self.assertEqual(out["result"], "ok")
        mocked_test.assert_not_called()

    def test_first_handler_chat_model_prefers_cheapest_model(self):
        class _CheapestModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "expensive", "type": "chat", "quota": 10, "order": 2},
                    {"model": "cheap", "type": "chat", "quota": 1, "order": 99},
                    {"model": "middle", "type": "chat", "quota": 2, "order": 1},
                ]

        old_module = lanying_vendor.vendor_to_module.get("cheapest_test")
        lanying_vendor.vendor_to_module["cheapest_test"] = _CheapestModule
        try:
            out = lanying_vendor._first_handler_chat_model("cheapest_test")
        finally:
            if old_module is None:
                del lanying_vendor.vendor_to_module["cheapest_test"]
            else:
                lanying_vendor.vendor_to_module["cheapest_test"] = old_module

        self.assertEqual(out["model"], "cheap")

    def test_chat_models_by_service_puts_default_first_then_model_desc_for_all_vendors(self):
        class _SortRuleModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "alpha-1", "type": "chat", "service": "sort-rule", "is_default": False},
                    {"model": "zeta-9", "type": "chat", "service": "sort-rule", "is_default": True},
                    {"model": "middle-5", "type": "chat", "service": "sort-rule", "is_default": False},
                ]

        old_module = lanying_vendor.vendor_to_module.get("sort_rule_test")
        lanying_vendor.vendor_to_module["sort_rule_test"] = _SortRuleModule
        try:
            grouped = lanying_vendor._chat_models_by_service()
        finally:
            if old_module is None:
                del lanying_vendor.vendor_to_module["sort_rule_test"]
            else:
                lanying_vendor.vendor_to_module["sort_rule_test"] = old_module

        models = [item["model"] for item in grouped["sort-rule"]]
        self.assertEqual(models, ["zeta-9", "middle-5", "alpha-1"])

    def test_chat_models_by_service_preserves_openai_grouped_order(self):
        class _OpenAIOrderModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "gpt-5-mini", "type": "chat", "service": "chatgpt", "is_default": True},
                    {"model": "gpt-4.1", "type": "chat", "service": "chatgpt"},
                    {"model": "gpt-4o", "type": "chat", "service": "chatgpt"},
                    {"model": "o4-mini", "type": "chat", "service": "chatgpt"},
                    {"model": "o1", "type": "chat", "service": "chatgpt"},
                ]

        class _OtherVendorModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "alpha-1", "type": "chat", "service": "other-service"},
                    {"model": "zeta-9", "type": "chat", "service": "other-service", "is_default": True},
                    {"model": "middle-5", "type": "chat", "service": "other-service"},
                ]

        old_openai = lanying_vendor.vendor_to_module.get("openai")
        old_other = lanying_vendor.vendor_to_module.get("other_vendor_test")
        lanying_vendor.vendor_to_module["openai"] = _OpenAIOrderModule
        lanying_vendor.vendor_to_module["other_vendor_test"] = _OtherVendorModule
        try:
            grouped = lanying_vendor._chat_models_by_service()
        finally:
            if old_openai is None:
                del lanying_vendor.vendor_to_module["openai"]
            else:
                lanying_vendor.vendor_to_module["openai"] = old_openai
            if old_other is None:
                del lanying_vendor.vendor_to_module["other_vendor_test"]
            else:
                lanying_vendor.vendor_to_module["other_vendor_test"] = old_other

        self.assertEqual(
            [item["model"] for item in grouped["chatgpt"][:5]],
            ["gpt-5-mini", "gpt-4.1", "gpt-4o", "o4-mini", "o1"]
        )
        self.assertEqual(
            [item["model"] for item in grouped["other-service"]],
            ["zeta-9", "middle-5", "alpha-1"]
        )

    def test_chat_models_by_service_keeps_same_model_name_for_different_handler_vendors(self):
        class _VendorOneModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "shared-model", "type": "chat", "service": "shared-service"},
                ]

        class _VendorTwoModule:
            @staticmethod
            def model_configs():
                return [
                    {"model": "shared-model", "type": "chat", "service": "shared-service"},
                ]

        old_one = lanying_vendor.vendor_to_module.get("vendor_one_test")
        old_two = lanying_vendor.vendor_to_module.get("vendor_two_test")
        lanying_vendor.vendor_to_module["vendor_one_test"] = _VendorOneModule
        lanying_vendor.vendor_to_module["vendor_two_test"] = _VendorTwoModule
        try:
            grouped = lanying_vendor._chat_models_by_service()
        finally:
            if old_one is None:
                del lanying_vendor.vendor_to_module["vendor_one_test"]
            else:
                lanying_vendor.vendor_to_module["vendor_one_test"] = old_one
            if old_two is None:
                del lanying_vendor.vendor_to_module["vendor_two_test"]
            else:
                lanying_vendor.vendor_to_module["vendor_two_test"] = old_two

        shared_models = [item for item in grouped["shared-service"] if item["model"] == "shared-model"]
        self.assertEqual(len(shared_models), 2)
        self.assertEqual(
            sorted([item.get("handler_vendor") for item in shared_models]),
            ["vendor_one_test", "vendor_two_test"]
        )

    def test_sanitize_model_config_hides_internal_pricing_fields(self):
        config = {
            "model": "gpt-test",
            "type": "chat",
            "url": "https://example.com",
            "endpoint": "https://example.com/v1",
            "input_price": 0.25,
            "output_price": 2.0,
            "currency": "USD",
        }

        out = lanying_vendor._sanitize_model_config(config)

        self.assertNotIn("url", out)
        self.assertNotIn("endpoint", out)
        self.assertNotIn("input_price", out)
        self.assertNotIn("output_price", out)
        self.assertNotIn("currency", out)

    def test_test_vendor_connection_keeps_original_endpoint_when_first_probe_succeeds(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/mock/openai/v1",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )

        with mock.patch.object(lanying_vendor, "_get_vendor_validation_chat_model_config", return_value={"model": "gpt-4o-mini"}), \
             mock.patch.object(lanying_vendor, "_test_vendor_connection_once", return_value={"result": "ok"}) as mocked_test:
            out = lanying_vendor.test_vendor_connection(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(vendor_setting.api_endpoint, "https://api.example.com/mock/openai/v1")
        mocked_test.assert_called_once()

    def test_test_vendor_connection_normalizes_full_chat_completions_path(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/mock/openai/v1/chat/completions",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )
        tried_endpoints = []

        def _probe(vendor_setting, handler_vendor, model_config):
            tried_endpoints.append(vendor_setting.api_endpoint)
            if vendor_setting.api_endpoint == "https://api.example.com/mock/openai/v1":
                return {"result": "ok"}
            return {"result": "error", "message": "vendor_connection_test_failed"}

        with mock.patch.object(lanying_vendor, "_get_vendor_validation_chat_model_config", return_value={"model": "gpt-4o-mini"}), \
             mock.patch.object(lanying_vendor, "_test_vendor_connection_once", side_effect=_probe):
            out = lanying_vendor.test_vendor_connection(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(tried_endpoints, [
            "https://api.example.com/mock/openai/v1/chat/completions",
            "https://api.example.com/mock/openai/v1",
        ])
        self.assertEqual(vendor_setting.api_endpoint, "https://api.example.com/mock/openai/v1")

    def test_test_vendor_connection_normalizes_service_prefix_by_appending_v1(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openrouter",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/mock/openai",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )
        tried_endpoints = []

        def _probe(vendor_setting, handler_vendor, model_config):
            tried_endpoints.append(vendor_setting.api_endpoint)
            if vendor_setting.api_endpoint == "https://api.example.com/mock/openai/v1":
                return {"result": "ok"}
            return {"result": "error", "message": "vendor_connection_test_failed"}

        with mock.patch.object(lanying_vendor, "_get_vendor_validation_chat_model_config", return_value={"model": "gpt-4o-mini"}), \
             mock.patch.object(lanying_vendor, "_test_vendor_connection_once", side_effect=_probe):
            out = lanying_vendor.test_vendor_connection(vendor_setting)

        self.assertEqual(out["result"], "ok")
        self.assertEqual(tried_endpoints, [
            "https://api.example.com/mock/openai",
            "https://api.example.com/mock/openai/v1",
        ])
        self.assertEqual(vendor_setting.api_endpoint, "https://api.example.com/mock/openai/v1")

    def test_test_vendor_connection_returns_error_when_all_openai_endpoint_candidates_fail(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="openai",
            name="test",
            api_key="k",
            secret_key="",
            api_group_id="",
            api_endpoint="https://api.example.com/mock/openai/v1/chat/completions",
            model_config=[],
            config_version=2,
            handler_vendor="openai",
        )
        tried_endpoints = []

        def _probe(vendor_setting, handler_vendor, model_config):
            tried_endpoints.append(vendor_setting.api_endpoint)
            return {"result": "error", "message": "vendor_connection_test_failed"}

        with mock.patch.object(lanying_vendor, "_get_vendor_validation_chat_model_config", return_value={"model": "gpt-4o-mini"}), \
             mock.patch.object(lanying_vendor, "_test_vendor_connection_once", side_effect=_probe):
            out = lanying_vendor.test_vendor_connection(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(out["message"], "vendor_connection_test_failed")
        self.assertEqual(vendor_setting.api_endpoint, "https://api.example.com/mock/openai/v1/chat/completions")
        self.assertEqual(tried_endpoints, [
            "https://api.example.com/mock/openai/v1/chat/completions",
            "https://api.example.com/mock/openai/v1",
        ])

    def test_test_vendor_connection_does_not_normalize_non_openai_handler_vendor(self):
        vendor_setting = lanying_vendor.VendorSetting(
            app_id="app",
            tenement_id="tenement",
            vendor_type="aws",
            name="test",
            api_key="k",
            secret_key="s",
            api_group_id="",
            api_endpoint="https://api.example.com/bedrock/chat/completions",
            model_config=[],
            config_version=2,
            handler_vendor="aws",
        )
        tried_endpoints = []

        def _probe(vendor_setting, handler_vendor, model_config):
            tried_endpoints.append(vendor_setting.api_endpoint)
            return {"result": "error", "message": "vendor_connection_test_failed"}

        with mock.patch.object(lanying_vendor, "_get_vendor_validation_chat_model_config", return_value={"model": "m1"}), \
             mock.patch.object(lanying_vendor, "_test_vendor_connection_once", side_effect=_probe):
            out = lanying_vendor.test_vendor_connection(vendor_setting)

        self.assertEqual(out["result"], "error")
        self.assertEqual(tried_endpoints, ["https://api.example.com/bedrock/chat/completions"])


if __name__ == "__main__":
    unittest.main()
