import copy
import json
import unittest
from unittest import mock

import lanying_token_budget as budget


class CharacterEncoding:
    """Deterministic counting without downloading tiktoken dictionaries."""
    def encode(self, text, **_kwargs):
        return list(text)


class TokenBudgetTests(unittest.TestCase):
    def test_complete_nested_schema_is_counted(self):
        function = {'name': 'configure', 'description': '配置', 'parameters': {
            'type': 'object', 'properties': {'nested': {'type': 'array', 'items': {
                'anyOf': [{'type': 'string', 'description': 'short', 'enum': ['a', 'b']},
                          {'$ref': '#/$defs/value'}]}}},
            '$defs': {'value': {'type': 'integer', 'minimum': 0, 'maximum': 10}},
            'additionalProperties': False}}
        before = copy.deepcopy(function)
        first = budget.count_functions([function], CharacterEncoding())
        function['parameters']['properties']['nested']['items']['anyOf'][0]['description'] += '文' * 9000
        second = budget.count_functions([function], CharacterEncoding())
        self.assertEqual(9000, second - first)
        expected = json.dumps([{'type': 'function', 'function': before}],
                              ensure_ascii=False, separators=(',', ':'))
        self.assertEqual(len(expected), first)

    def test_executor_metadata_is_not_counted(self):
        function = {'name': 'query', 'description': 'test', 'parameters': {}}
        tokens = budget.count_functions([function], CharacterEncoding())
        function.update(priority=10, doc_id='doc', function_call={'url': 'not-model-input'})
        self.assertEqual(tokens, budget.count_functions([function], CharacterEncoding()))
        self.assertEqual(0, budget.count_functions([], CharacterEncoding()))

    def test_legacy_calls_and_modern_results_are_counted(self):
        for field in ('function_call', 'tool_calls'):
            value = {'name': 'query', 'arguments': 'x' * 2000}
            message = {'role': 'assistant', field: value if field == 'function_call' else [value]}
            self.assertGreater(budget.count_message(message, CharacterEncoding(), mock.Mock()), 2000)
        self.assertGreater(budget.count_message(
            {'role': 'tool', 'content': 'y' * 3000, 'tool_call_id': 'call1'},
            CharacterEncoding(), mock.Mock()), 3000)

    def test_multimodal_images_keep_image_counter(self):
        counter = mock.Mock(return_value=85)
        message = {'role': 'user', 'content': [
            {'type': 'text', 'text': '你好'},
            {'type': 'image_url', 'image_url': {'url': 'https://example.test/image', 'detail': 'low'}}]}
        self.assertEqual(99, budget.count_message(message, CharacterEncoding(), counter))
        counter.assert_called_once_with('https://example.test/image', 'low')

    def test_old_turn_removal_keeps_call_result_pairs_and_current_turn(self):
        messages = [
            {'role': 'system', 'content': 'policy'},
            {'role': 'user', 'content': 'old'},
            {'role': 'assistant', 'tool_calls': [{'id': 'c'}]},
            {'role': 'tool', 'tool_call_id': 'c', 'content': 'result'},
            {'role': 'developer', 'content': 'additional policy'},
            {'role': 'assistant', 'content': 'answer'},
            {'role': 'user', 'content': 'current'},
            {'role': 'assistant', 'tool_calls': [{'id': 'd'}]},
            {'role': 'tool', 'tool_call_id': 'd', 'content': 'new result'},
        ]
        self.assertTrue(budget.drop_oldest_turn(messages))
        self.assertEqual(['system', 'developer', 'user', 'assistant', 'tool'],
                         [m['role'] for m in messages])
        self.assertEqual('d', messages[-1]['tool_call_id'])
        self.assertFalse(budget.drop_oldest_turn(messages))

    def test_headroom_is_bounded_by_available_input(self):
        self.assertEqual(796, budget.input_budget(1000, 100))
        self.assertEqual(0, budget.input_budget(100, 100))

    def test_separate_input_limit_and_context_both_apply(self):
        self.assertEqual(91367, budget.input_budget(128000, 8192, 96000))
        self.assertEqual(796, budget.input_budget(1000, 100, 2000))
        for value in (None, 0, -1, False, True, 'invalid'):
            with self.subTest(value=value):
                self.assertEqual(796, budget.input_budget(1000, 100, value))


if __name__ == '__main__':
    unittest.main()
