import unittest
from unittest import mock

import lanying_openai_compat
import lanying_vendor
import lanying_vendor_openai
import lanying_vendor_moonshot


class CompletionTokenLimitsTest(unittest.TestCase):
    def test_catalog_limit_caps_large_budget_without_mutating_preset(self):
        for limit in (2048, 4096, 8192, 32768):
            with self.subTest(limit=limit):
                preset = {'model': 'example', 'max_tokens': 8192}
                result = lanying_openai_compat.limit_completion_tokens(
                    preset, {'max_output_tokens': limit})
                self.assertEqual(min(8192, limit), result['max_tokens'])
                self.assertEqual(8192, preset['max_tokens'])

    def test_explicit_smaller_budget_is_preserved(self):
        for budget in (512, 6000):
            result = lanying_openai_compat.limit_completion_tokens(
                {'max_tokens': budget},
                {'max_output_tokens': 8192, 'token_limit': 8192})
            self.assertEqual(budget, result['max_tokens'])

    def test_missing_budget_is_not_invented(self):
        self.assertEqual({'model': 'example'},
                         lanying_openai_compat.limit_completion_tokens(
                             {'model': 'example'}, {'max_output_tokens': 4096}))

    def test_unknown_limit_does_not_invent_a_ceiling(self):
        for config in ({}, None, {'max_output_tokens': 0},
                       {'max_output_tokens': None}):
            self.assertEqual(8192, lanying_openai_compat.limit_completion_tokens(
                {'max_tokens': 8192}, config)['max_tokens'])

    def test_reasoning_budget_takes_precedence_and_supports_legacy_adapters(self):
        preset = {'max_completion_tokens': 8192, 'max_tokens': 1024}
        result = lanying_openai_compat.limit_completion_tokens(
            preset, {'max_output_tokens': 4096})
        self.assertEqual(4096, result['max_tokens'])
        self.assertEqual(4096, result['max_completion_tokens'])
        self.assertEqual(8192, preset['max_completion_tokens'])

    def test_vendor_boundary_applies_actual_model_limit(self):
        module = mock.Mock()
        module.chat.return_value = {'result': 'ok'}
        preset = {'model': 'smaller-backup', 'max_completion_tokens': 8192}
        lanying_vendor.chat_with_same_model_retry(
            module, 'test', {}, preset, {'max_output_tokens': 2048})
        actual = module.chat.call_args.args[1]
        self.assertEqual(2048, actual['max_tokens'])
        self.assertEqual(2048, actual['max_completion_tokens'])
        self.assertEqual({'model': 'smaller-backup', 'max_completion_tokens': 8192},
                         preset)

    def test_openai_reasoning_uses_only_completion_parameter(self):
        result = lanying_vendor_openai.format_preset(
            {'model': 'gpt-5-mini', 'max_tokens': 8192},
            {'max_output_tokens': 128000})
        self.assertEqual(8192, result['max_completion_tokens'])
        self.assertNotIn('max_tokens', result)

    def test_openai_non_reasoning_switch_uses_only_legacy_parameter(self):
        config = next(c for c in lanying_vendor_openai.model_configs()
                      if c['model'] == 'gpt-4')
        result = lanying_vendor_openai.format_preset(
            {'model': 'gpt-4', 'max_completion_tokens': 8192,
             'reasoning_effort': 'low'},
            config)
        self.assertEqual(4096, result['max_tokens'])
        self.assertNotIn('max_completion_tokens', result)
        self.assertNotIn('reasoning_effort', result)

    def test_real_catalog_models_leave_context_for_input(self):
        configs = lanying_vendor_openai.model_configs() + lanying_vendor_moonshot.model_configs()
        for model, expected in [('gpt-4', 4096), ('moonshot-v1-8k', 4000),
                                ('gpt-5-mini', 8192)]:
            config = next(c for c in configs if c['model'] == model)
            for field in ('max_tokens', 'max_completion_tokens'):
                with self.subTest(model=model, field=field):
                    preset = {'model': model, field: 8192}
                    result = lanying_openai_compat.limit_completion_tokens(preset, config)
                    self.assertEqual(expected, result[field])
                    self.assertEqual(expected, result['max_tokens'])
                    self.assertEqual(8192, preset[field])
                    self.assertEqual(result, lanying_openai_compat.limit_completion_tokens(
                        result, config))

    def test_unknown_context_limit_keeps_output_ceiling(self):
        for context_limit in (None, 0, 'invalid'):
            result = lanying_openai_compat.limit_completion_tokens(
                {'max_tokens': 8192},
                {'max_output_tokens': 4096, 'token_limit': context_limit})
            self.assertEqual(4096, result['max_tokens'])

    def test_gpt5_default_passes_low_reasoning_without_unsupported_sampling(self):
        preset = {
            'model': 'gpt-5-mini', 'max_tokens': 8192,
            'reasoning_effort': 'low', 'temperature': 0.9, 'top_p': 1,
            'frequency_penalty': 0, 'presence_penalty': 0.6,
            'stop': [' Human:', ' AI:'], 'stream': True,
            'messages': [{'role': 'system', 'content': 'Be helpful.'}],
        }
        result = lanying_vendor_openai.format_preset(
            preset, {'max_output_tokens': 128000})
        self.assertEqual('low', result['reasoning_effort'])
        self.assertEqual(8192, result['max_completion_tokens'])
        self.assertEqual('developer', result['messages'][0]['role'])
        self.assertEqual({'include_usage': True}, result['stream_options'])
        for field in ('max_tokens', 'temperature', 'top_p', 'frequency_penalty',
                      'presence_penalty', 'stop'):
            self.assertNotIn(field, result)
        self.assertEqual('system', preset['messages'][0]['role'])

    def test_existing_reasoning_preset_without_effort_keeps_provider_default(self):
        result = lanying_vendor_openai.format_preset(
            {'model': 'gpt-5-mini', 'max_tokens': 2048},
            {'max_output_tokens': 128000})
        self.assertNotIn('reasoning_effort', result)


if __name__ == '__main__':
    unittest.main()
