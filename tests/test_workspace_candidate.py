import unittest
from unittest import mock
from sqlalchemy import text

import lanying_seenical_materials as m
import lanying_workspace_candidate as candidate
import test_workspace_material_replacement as fixtures


class CandidateTest(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.ReplacementTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.engine = self.base.engine
        self.base.new_size = 10
        self.workspace = dict(workspace_id='w', skill_id='writing-1')
        self.file = dict(path_hash='h', doc_id='1-1', source_id='s')
        with self.engine.begin() as c:
            c.execute(text('CREATE TABLE seenical_workspace_candidate ('
                'app_id TEXT,workspace_id TEXT,skill_id TEXT,path_hash TEXT,doc_id TEXT,session_id TEXT,'
                'replacement_id TEXT,copy_on_write INTEGER,physical_id TEXT,previous_physical_id TEXT,'
                'source_object TEXT,object_name TEXT,filename TEXT,file_size INTEGER,content_sha TEXT,'
                "old_usage INTEGER,new_usage INTEGER DEFAULT 0,status TEXT DEFAULT 'pending',error_code TEXT DEFAULT '',"
                'PRIMARY KEY(workspace_id,skill_id,path_hash))'))
            c.execute(text('CREATE TABLE seenical_workspace_partition (app_id TEXT,workspace_id TEXT,skill_id TEXT,'
                'seenical_session_id TEXT,active INTEGER,paused INTEGER)'))
            c.execute(text("INSERT INTO seenical_workspace_partition VALUES ('a','w','writing-1','s',1,0)"))
            c.execute(text('CREATE TABLE seenical_workspace_file (app_id TEXT,workspace_id TEXT,skill_id TEXT,'
                'path_hash TEXT,doc_id TEXT,removed INTEGER,base_sha TEXT,server_sha TEXT,source_fingerprint TEXT,'
                'status TEXT,error_code TEXT,decision_json TEXT)'))
            c.execute(text("INSERT INTO seenical_workspace_file VALUES ('a','w','writing-1','h','1-1',0,'','','','','',NULL)"))
            c.execute(text("INSERT INTO seenical_conversation_binding VALUES ('a','bot','s','loop','ACTIVE')"))
        m.set_references('a', 'plan', 'loop', ['1-1'])
        self.base.limit = 30
        def reserve(app, embedding_uuid, doc, size, clear=False, release_only=False):
            if not clear and not release_only and sum(self.base.usage.values()) - self.base.usage.get(doc, 0) + size > self.base.limit:
                raise m.MaterialError('material_storage_limit')
            return self.base.reserve(app, embedding_uuid, doc, size, clear, release_only)
        self.usage_patch = mock.patch.object(m, 'update_usage', side_effect=reserve)
        self.usage_patch.start()
        self.addCleanup(self.usage_patch.stop)

    def start(self):
        return candidate.start('a', self.workspace, self.file, b'new content', 'a' * 40)

    def row(self):
        with self.engine.connect() as c:
            return candidate.load(c, 'a', self.workspace, self.file)

    def test_shared_edit_switches_only_current_session_and_bound_loop(self):
        m.set_references('a', 'session', 'other', ['1-1'])
        m.set_references('a', 'plan', 'independent', ['1-1'])
        m.set_references('a', 'run', 'running', ['1-1'])
        self.assertEqual(self.start(), 'done')
        new = self.row()['physical_id']
        self.assertEqual(m.reference_ids('a', 'session', 's'), [new])
        self.assertEqual(m.reference_ids('a', 'plan', 'loop'), [new])
        for kind, owner in [('session', 'other'), ('plan', 'independent'), ('run', 'running')]:
            self.assertEqual(m.reference_ids('a', kind, owner), ['1-1'])
        self.assertEqual(self.base.document()['object_name'], 'old-object')
        self.assertEqual(sum(self.base.usage.values()), 20)
        self.base.vectors.assert_not_called()
        self.assertEqual(candidate.resume('a', self.workspace, self.file), 'done')
        self.assertEqual(self.base.fixture.index.call_count, 1)

    def test_exclusive_edit_preserves_logical_id_and_checks_only_delta(self):
        self.base.limit = 10
        self.assertEqual(self.start(), 'done')
        self.assertEqual(self.row()['copy_on_write'], 0)
        self.assertEqual(m.reference_ids('a', 'session', 's'), ['1-1'])
        self.assertEqual(sum(self.base.usage.values()), 10)

    def test_two_partitions_copy_the_same_original_independently(self):
        m.set_references('a', 'session', 'other', ['1-1'])
        m.set_references('a', 'plan', 'independent', ['1-1'])
        with self.engine.begin() as c:
            c.execute(text("INSERT INTO seenical_workspace_partition VALUES ('a','w2','writing-2','other',1,0)"))
            c.execute(text("INSERT INTO seenical_workspace_file VALUES ('a','w2','writing-2','h','1-1',0,'','','','','',NULL)"))
        self.assertEqual(self.start(), 'done')
        first = m.reference_ids('a', 'session', 's')[0]
        second_workspace = dict(workspace_id='w2', skill_id='writing-2')
        self.assertEqual(candidate.start('a', second_workspace, dict(self.file, source_id='other'), b'other', 'b' * 40), 'done')
        second = m.reference_ids('a', 'session', 'other')[0]
        self.assertNotEqual(first, second)
        self.assertEqual(m.reference_ids('a', 'plan', 'independent'), ['1-1'])
        self.assertEqual(sum(self.base.usage.values()), 30)
        self.assertEqual(self.base.document()['object_name'], 'old-object')

    def test_exclusive_active_run_waits_without_indexing(self):
        m.set_references('a', 'run', 'running', ['1-1'])
        self.assertEqual(self.start(), 'waiting_runs')
        self.base.fixture.index.assert_not_called()

    def test_copy_at_capacity_fails_without_replacing_original(self):
        m.set_references('a', 'session', 'other', ['1-1'])
        self.base.limit = 10
        self.assertEqual(self.start(), 'failed')
        self.assertEqual(m.reference_ids('a', 'session', 's'), ['1-1'])
        self.assertEqual(sum(self.base.usage.values()), 10)
        self.assertEqual(self.base.document()['object_name'], 'old-object')
        self.assertTrue(all(c.args[-1] != '1-1' for c in self.base.vectors.call_args_list))

    def test_inheritance_during_exclusive_build_invalidates_switch(self):
        def build(*args, **kwargs):
            result = self.base.build(*args, **kwargs)
            m.set_references('a', 'session', 'other', ['1-1'])
            return result
        self.base.fixture.index.side_effect = build
        self.assertEqual(self.start(), 'failed')
        self.assertEqual(self.base.document()['object_name'], 'old-object')
        self.assertEqual(m.reference_ids('a', 'session', 'other'), ['1-1'])
        self.assertEqual(self.row()['error_code'], 'material_update_changed')

    def test_removed_reference_is_not_readded_after_copy(self):
        m.set_references('a', 'session', 'other', ['1-1'])
        def build(*args, **kwargs):
            result = self.base.build(*args, **kwargs)
            m.release_owner('a', 'session', 's')
            return result
        self.base.fixture.index.side_effect = build
        self.assertEqual(self.start(), 'failed')
        self.assertEqual(m.reference_ids('a', 'session', 's'), [])
        self.assertEqual(m.reference_ids('a', 'session', 'other'), ['1-1'])
        self.assertEqual(sum(self.base.usage.values()), 10)

    def test_cleanup_failure_retains_charge_until_success_then_is_idempotent(self):
        m.set_references('a', 'session', 'other', ['1-1'])
        def fail(*args, **kwargs):
            self.base.build(*args, **kwargs)
            raise RuntimeError('parse error')
        self.base.fixture.index.side_effect = fail
        self.base.vectors.side_effect = RuntimeError('storage down')
        with self.assertRaises(RuntimeError):
            self.start()
        self.assertEqual(sum(self.base.usage.values()), 20)
        self.assertEqual(m.reference_ids('a', 'session', 's'), ['1-1'])
        self.base.vectors.side_effect = None
        self.assertEqual(candidate.resume('a', self.workspace, self.file), 'failed')
        self.assertEqual(sum(self.base.usage.values()), 10)
        self.assertEqual(candidate.resume('a', self.workspace, self.file), 'failed')
        self.assertEqual(self.base.fixture.index.call_count, 1)

    def test_forged_partition_cannot_mutate_or_index(self):
        self.workspace['skill_id'] = 'writing-2'
        with self.assertRaises(m.MaterialError):
            self.start()
        self.base.fixture.index.assert_not_called()


if __name__ == '__main__':
    unittest.main()
