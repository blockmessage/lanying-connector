import contextlib
import types
import unittest
from unittest import mock

from sqlalchemy import text

import test_seenical_materials as fixtures
import lanying_seenical_materials as m
import lanying_material_replacement as replacement


class ReplacementTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.MaterialReferencesTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.engine = self.fixture.engine
        with self.engine.begin() as conn:
            conn.execute(text("ALTER TABLE seenical_material ADD COLUMN index_doc_id TEXT DEFAULT ''"))
            conn.execute(text("ALTER TABLE seenical_material ADD COLUMN replacement_id TEXT DEFAULT ''"))
            conn.execute(text('CREATE TABLE seenical_material_replacement (app_id TEXT,doc_id TEXT,'
                'replacement_id TEXT,physical_id TEXT,previous_physical_id TEXT,object_name TEXT,'
                'filename TEXT,file_size INTEGER,old_usage INTEGER,new_usage INTEGER DEFAULT 0,'
                "content_sha TEXT,status TEXT DEFAULT 'pending',error_code TEXT DEFAULT '',PRIMARY KEY(app_id,doc_id))"))
            conn.execute(text("UPDATE seenical_material SET object_name='old-object',file_size=10"))
        m.set_references('a', 'session', 's', ['1-1'])
        self.usage = {'1-1': 10}
        self.usage_calls = []
        self.limit = 10
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.embedding = types.SimpleNamespace(
            get_doc=lambda _, d: {'storage_file_size': self.usage.get(d, 0)},
            get_embedding_doc_info_key=lambda u, d: d)
        redis = types.SimpleNamespace(hget=lambda key, field: self.usage.get(key))
        redis_module = types.SimpleNamespace(get_redis_stack_connection=lambda: redis)
        self.remove_object = mock.Mock()
        files = types.SimpleNamespace(put_object=mock.Mock(return_value={'result': 'ok'}),
                                      bucket_name='embedding-file',
                                      client=types.SimpleNamespace(remove_object=self.remove_object))
        self.stack.enter_context(mock.patch.dict('sys.modules', lanying_embedding=self.embedding,
                                                lanying_file_storage=files, lanying_redis=redis_module))
        self.stack.enter_context(mock.patch.object(m, 'update_usage', side_effect=self.reserve))
        self.vectors = self.stack.enter_context(mock.patch.object(m, '_clear_vectors'))
        self.fixture.index.side_effect = self.build

    def reserve(self, app, embedding_uuid, doc_id, size, clear=False, release_only=False):
        self.usage_calls.append((doc_id, size, clear, release_only))
        if release_only and size > self.usage.get(doc_id, 0):
            raise m.MaterialError('material_usage_changed')
        if not clear and not release_only and size > self.limit:
            raise m.MaterialError('material_storage_limit')
        self.usage[doc_id] = size

    def build(self, app, space, doc, reserve_usage):
        reserve_usage(self.new_size)
        return self.new_size

    def document(self):
        with self.engine.connect() as conn:
            return m._document(conn, 'a', '1-1')

    def test_same_size_update_at_capacity_preserves_identity_and_references(self):
        self.new_size = 10
        self.assertEqual(replacement.start('a', '1-1', b'new-content', 'a' * 40), 'done')
        doc = self.document()
        self.assertNotEqual(m.index_document_id(doc), '1-1')
        self.assertEqual(doc['doc_id'], '1-1')
        self.assertEqual(m.reference_ids('a', 'session', 's'), ['1-1'])
        self.assertEqual(self.usage['1-1'], 10)
        self.assertEqual(doc['replacement_id'], '')
        self.assertEqual(self.vectors.call_args.args[-1], '1-1')
        self.remove_object.assert_not_called()

    def test_larger_requires_delta_and_failed_candidate_keeps_old(self):
        self.new_size = 11
        self.assertEqual(replacement.start('a', '1-1', b'elevenbytes', 'b' * 40), 'failed')
        self.assertEqual(self.document()['object_name'], 'old-object')
        self.assertEqual(self.document()['index_doc_id'], '')
        self.assertEqual(self.usage['1-1'], 10)
        self.assertNotEqual(self.vectors.call_args.args[-1], '1-1')

    def test_failed_original_without_metadata_can_be_replaced_and_cleaned(self):
        # Validation can fail before create_doc_info. A usage-only Redis hash
        # still has no filename, so the production get_doc returns None.
        self.embedding.get_doc = lambda *_: None
        self.usage.clear()
        self.new_size = 5
        with self.engine.begin() as conn:
            m._set_state(conn, 'a', '1-1', 'failed', 'material_unsupported_format')
        self.assertEqual(replacement.start('a', '1-1', b'valid', '5' * 40), 'done')
        self.assertEqual(self.usage['1-1'], 5)
        self.assertEqual(self.document()['replacement_id'], '')
        self.assertEqual(self.document()['status'], 'ready')
        calls = len(self.usage_calls)
        self.assertEqual(replacement.resume('a', '1-1'), 'done')
        self.assertEqual(len(self.usage_calls), calls)

        # Later replacements must also read the logical capacity hash without
        # requiring metadata that now belongs to the physical document.
        self.new_size = 11
        self.assertEqual(replacement.start('a', '1-1', b'too large', '6' * 40), 'failed')
        self.assertEqual(self.usage['1-1'], 5)
        self.assertEqual(self.document()['replacement_id'], '')
        self.assertEqual(self.document()['file_size'], 5)

        # Removing all references can reclaim the successful physical index.
        m.release_owner('a', 'session', 's')
        self.fixture.clear.side_effect = self.fixture.real_clear_index
        m.process_pending('a', 'cleanup-worker')
        self.assertEqual(self.document()['status'], 'unindexed')
        self.assertEqual(self.usage['1-1'], 0)

    def test_missing_index_metadata_does_not_refund_usage_only_hash(self):
        self.embedding.get_doc = lambda *_: None
        self.new_size = 5
        self.vectors.return_value = False
        with self.assertRaisesRegex(m.MaterialError, 'material_storage_unavailable'):
            replacement.start('a', '1-1', b'valid', '7' * 40)
        self.assertEqual(self.usage['1-1'], 10)
        self.assertTrue(self.document()['replacement_id'])
        self.vectors.return_value = True
        self.assertEqual(replacement.resume('a', '1-1'), 'done')
        self.assertEqual(self.usage['1-1'], 5)
        self.assertEqual(self.document()['replacement_id'], '')

    def test_smaller_releases_only_after_success(self):
        self.new_size = 5
        observed = []
        def build(*args, reserve_usage):
            reserve_usage(5)
            observed.append(self.usage['1-1'])
            return 5
        self.fixture.index.side_effect = build
        self.assertEqual(replacement.start('a', '1-1', b'small', 'c' * 40), 'done')
        self.assertEqual(observed, [10])
        self.assertEqual(self.usage['1-1'], 5)

    def test_parser_failure_restores_reserved_delta_without_clearing_old_vectors(self):
        self.limit = 30
        def fail(*args, reserve_usage):
            reserve_usage(20)
            raise RuntimeError('parser failed')
        self.fixture.index.side_effect = fail
        self.assertEqual(replacement.start('a', '1-1', b'new', 'd' * 40), 'failed')
        self.assertEqual(self.usage['1-1'], 10)
        self.assertEqual(self.document()['object_name'], 'old-object')
        self.assertTrue(all(c.args[-1] != '1-1' for c in self.vectors.call_args_list))
        self.remove_object.assert_called_once()
        self.assertTrue(self.remove_object.call_args.args[1].startswith('seenical/material-updates/a/'))

    def test_active_run_delays_download_and_paid_indexing(self):
        m.set_references('a', 'run', 'run1', ['1-1'])
        self.assertEqual(replacement.start('a', '1-1', b'new', 'e' * 40), 'waiting_runs')
        self.fixture.index.assert_not_called()
        self.assertEqual(self.document()['replacement_id'], '')

    def test_run_started_during_build_defers_switch_until_released(self):
        self.new_size = 8
        def build(*args, **kwargs):
            value = self.build(*args, **kwargs)
            m.set_references('a', 'run', 'run1', ['1-1'])
            return value
        self.fixture.index.side_effect = build
        self.assertEqual(replacement.start('a', '1-1', b'new', 'f' * 40), 'waiting_runs')
        self.assertEqual(self.document()['object_name'], 'old-object')
        self.assertEqual(self.usage['1-1'], 10)
        m.release_owner('a', 'run', 'run1')
        self.assertEqual(replacement.resume('a', '1-1'), 'done')
        self.assertEqual(self.fixture.index.call_count, 1)
        self.assertEqual(self.usage['1-1'], 8)

    def test_cleanup_failure_after_commit_never_discards_active_candidate(self):
        self.new_size = 10
        self.vectors.side_effect = RuntimeError('storage down')
        with self.assertRaises(RuntimeError):
            replacement.start('a', '1-1', b'new', '1' * 40)
        active = self.document()['index_doc_id']
        self.assertTrue(active)
        self.vectors.side_effect = None
        replacement.discard('a', '1-1')
        self.assertEqual(self.document()['index_doc_id'], active)
        self.assertTrue(all(c.args[-1] != active for c in self.vectors.call_args_list))

    def test_retry_and_duplicate_do_not_repeat_success_or_refund(self):
        self.new_size = 7
        replacement.start('a', '1-1', b'new', '2' * 40)
        replacement.resume('a', '1-1')
        replacement.start('a', '1-1', b'new', '2' * 40)
        self.assertEqual(self.fixture.index.call_count, 1)
        self.assertEqual(self.usage['1-1'], 7)

    def test_interrupted_build_discards_only_candidate_and_requires_explicit_retry(self):
        self.new_size = 10
        m.set_references('a', 'run', 'run1', ['1-1'])
        # Build a valid pending row without executing the paid processor.
        m.release_owner('a', 'run', 'run1')
        with mock.patch.object(replacement, 'resume', return_value='pending'):
            replacement.start('a', '1-1', b'new', '3' * 40)
        self.assertEqual(replacement.resume('a', '1-1'), 'failed')
        self.fixture.index.assert_not_called()
        self.assertEqual(self.document()['object_name'], 'old-object')
        replacement.retry_failed('a', '1-1')
        self.assertEqual(replacement.start('a', '1-1', b'new', '3' * 40), 'done')

    def test_reference_cleanup_waits_until_replacement_finishes(self):
        self.new_size = 10
        with mock.patch.object(replacement, 'resume', return_value='pending'):
            replacement.start('a', '1-1', b'new', '4' * 40)
        m.release_owner('a', 'session', 's')
        self.fixture.clear.reset_mock()
        m.process_pending('a', 'normal-worker')
        self.assertNotIn('1-1', [c.args[2]['doc_id'] for c in self.fixture.clear.call_args_list])
        replacement.discard('a', '1-1')
        self.assertEqual(self.document()['replacement_id'], '')


if __name__ == '__main__':
    unittest.main()
