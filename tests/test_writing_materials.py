import contextlib
import unittest
from unittest import mock
from sqlalchemy import create_engine, event, text

import lanying_writing_materials as w


class WritingMaterialsTest(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.embedding = mock.Mock()
        self.chatbot = mock.Mock()
        self.grow = mock.Mock()
        self.config = mock.Mock()
        self.redis = mock.Mock()
        self.config.get_service_config.return_value = {
            'embeddings': {'writer': ['kb', 'private', 'functions']}}
        self.chatbot.get_chatbot.return_value = {'name': 'writer'}
        self.grow.get_task.return_value = {'chatbot_id': 'agent'}
        bindings = [
            {'app_id': 'app', 'embedding_uuid': 'kb', 'embedding_name': 'Shared', 'type': 'normal'},
            {'app_id': 'app', 'embedding_uuid': 'private', 'embedding_name': w.materials.INTERNAL_NAME,
             'type': w.materials.INTERNAL_TYPE},
            {'app_id': 'app', 'embedding_uuid': 'functions', 'embedding_name': 'tools', 'type': 'function'}]
        self.names = {info['embedding_name']: dict(info) for info in bindings}
        self.owners = {info['embedding_uuid']: dict(info) for info in bindings}
        self.embedding.get_embedding_name_info.side_effect = lambda app, name: self.names.get(name)
        self.embedding.get_app_embedding_uuid_info.side_effect = lambda app, uuid: self.owners.get(uuid)
        # Export must not invoke either retrieval helper, even for legacy records
        # without model/vendor fields. Test the real export enumeration below.
        self.embedding.get_preset_embedding_infos.side_effect = AssertionError('retrieval enumeration')
        self.embedding.get_embedding_uuid_info.side_effect = KeyError('app_ids')
        self.embedding.get_embedding_doc_id_list.return_value = ['kb-1']
        self.embedding.get_doc.return_value = {
            'filename': 'facts.txt', 'object_name': 'secret-storage-location', 'file_size': 12}
        self.stack.enter_context(mock.patch.dict('sys.modules', {
            'lanying_embedding': self.embedding, 'lanying_chatbot': self.chatbot,
            'lanying_config': self.config, 'lanying_grow_ai': self.grow,
            'lanying_redis': self.redis}))
        self.conversation = self.stack.enter_context(mock.patch.object(
            w.materials, 'authorized_conversation', return_value={
                'chatbot_id': 'agent', 'task_id': 'plan'}))
        self.refs = self.stack.enter_context(mock.patch.object(
            w.materials, 'reference_ids', return_value=['doc']))
        self.stack.enter_context(mock.patch.object(w.materials, '_engine'))
        self.doc = self.stack.enter_context(mock.patch.object(w.materials, '_document', return_value={
            'filename': 'note.txt', 'object_name': 'private-object', 'status': 'ready', 'file_size': 4}))
        self.data = {'seenical_session_id': 'session'}

    def test_manifest_contains_only_safe_metadata_and_bound_knowledge(self):
        result = w.manifest('app', {'im_user_id': 'user'}, self.data)
        self.assertEqual(len(result['documents']), 2)
        self.assertEqual(result['documents'][0]['path'], 'conversation/doc/note.txt')
        self.assertFalse(result['documents'][0]['read_only'])
        self.assertTrue(result['documents'][1]['read_only'])
        self.assertNotIn('object_name', str(result))
        self.assertNotIn('private-object', str(result))
        self.embedding.get_preset_embedding_infos.assert_not_called()
        self.refs.assert_called_once_with('app', 'session', 'session')

    def test_loop_uses_saved_plan_not_current_session_references(self):
        w.manifest('app', {}, dict(self.data, task_id='plan'))
        self.refs.assert_called_once_with('app', 'plan', 'plan')

    def test_legacy_library_without_model_can_be_exported_without_model_repair(self):
        self.owners['kb'] = {
            'app_id': 'app', 'index': 'embedding_kb', 'embedding_name': 'Shared'}
        result = w.manifest('app', {}, self.data)
        self.assertEqual(len(result['documents']), 2)
        request = dict(self.data, source_type='knowledge_base', source_id='kb', doc_id='kb-1')
        self.assertEqual(w.download('app', {}, request), {'embedding_name': 'Shared', 'doc_id': 'kb-1'})
        self.embedding.get_app_embedding_uuid_info.assert_any_call('app', 'kb')
        self.embedding.get_embedding_uuid_info.assert_not_called()
        self.embedding.update_embedding_uuid_info.assert_not_called()

    def test_legacy_name_bindings_use_read_only_export_enumeration(self):
        self.config.get_service_config.return_value = {}
        self.redis.redis_hgetall.return_value = {
            'Shared': 'writer', w.materials.INTERNAL_NAME: 'writer',
            'tools': 'writer', 'Unbound': 'another-agent'}
        result = w.manifest('app', {}, self.data)
        self.assertEqual(len(result['documents']), 2)
        self.assertEqual(result['documents'][1]['source_id'], 'kb')
        self.embedding.get_embedding_name_info.assert_any_call('app', 'Shared')
        self.assertNotIn(mock.call('app', 'Unbound'), self.embedding.get_embedding_name_info.call_args_list)
        self.embedding.get_preset_embedding_infos.assert_not_called()
        self.embedding.get_embedding_uuid_info.assert_not_called()
        self.embedding.update_embedding_uuid_info.assert_not_called()

    def test_stale_name_binding_does_not_export_another_library(self):
        self.names['Shared']['embedding_uuid'] = 'replacement'
        result = w.manifest('app', {}, self.data)
        self.assertEqual([d['source_type'] for d in result['documents']], ['conversation'])
        for uuid in ('kb', 'replacement'):
            with self.assertRaises(w.materials.MaterialError):
                w.download('app', {}, dict(self.data, source_type='knowledge_base',
                                          source_id=uuid, doc_id='kb-1'))
        self.embedding.get_doc.assert_not_called()

    def test_explicit_empty_binding_does_not_fall_back_to_legacy_bindings(self):
        self.config.get_service_config.return_value = {'embeddings': {}}
        result = w.manifest('app', {}, self.data)
        self.assertEqual([d['source_type'] for d in result['documents']], ['conversation'])
        self.redis.get_redis_stack_connection.assert_not_called()

    def test_capsule_links_do_not_expand_export_scope(self):
        self.chatbot.get_chatbot.return_value.update(
            linked_capsule_id='foreign', linked_publish_capsule_id='published',
            preset={'messages': [{'role': 'system', 'content': 'PRIVATE PROMPT'}]})
        result = w.manifest('app', {}, self.data)
        self.assertNotIn('PRIVATE PROMPT', str(result))
        self.embedding.get_preset_embedding_infos_by_capsule_id.assert_not_called()
        self.embedding.get_preset_embedding_infos_by_publish_capsule_id.assert_not_called()

    def test_foreign_or_unknown_library_owner_is_skipped_and_cannot_download(self):
        for binding_owner, actual_owner in [('other', 'other'), ('app', 'other'),
                                             ('app', ''), ('', 'app')]:
            with self.subTest(binding=binding_owner, owner=actual_owner):
                self.config.get_service_config.return_value = {'embeddings': {'writer': ['foreign']}}
                self.names['Private'] = {
                    'app_id': binding_owner, 'embedding_uuid': 'foreign',
                    'embedding_name': 'Private', 'type': 'text'}
                self.owners['foreign'] = {'app_id': actual_owner, 'embedding_name': 'Private'}
                self.embedding.get_embedding_doc_id_list.reset_mock()
                self.embedding.get_doc.reset_mock()
                result = w.manifest('app', {}, self.data)
                self.assertEqual([d['source_type'] for d in result['documents']], ['conversation'])
                with self.assertRaises(w.materials.MaterialError):
                    w.download('app', {}, dict(self.data, source_type='knowledge_base',
                                              source_id='foreign', doc_id='foreign-1'))
                self.embedding.get_embedding_doc_id_list.assert_not_called()
                self.embedding.get_doc.assert_not_called()

    def test_other_agent_or_unbound_plan_is_rejected(self):
        for chatbot, task_id in [('other', 'plan'), ('agent', 'other-plan')]:
            self.grow.get_task.return_value = {'chatbot_id': chatbot}
            with self.assertRaises(w.materials.MaterialError):
                w.manifest('app', {}, dict(self.data, task_id=task_id))

    def test_download_rechecks_conversation_and_active_document_scope(self):
        request = dict(self.data, source_type='conversation', source_id='session', doc_id='doc')
        self.assertEqual(w.download('app', {}, request)['embedding_name'], w.materials.INTERNAL_NAME)
        self.refs.return_value = []
        with self.assertRaises(w.materials.MaterialError):
            w.download('app', {}, request)
        self.conversation.assert_called_with('app', {}, 'session')

    def test_other_conversation_and_unbound_knowledge_cannot_be_downloaded(self):
        for kind, owner in [('conversation', 'other'), ('knowledge_base', 'private'),
                            ('knowledge_base', 'unknown')]:
            with self.assertRaises(w.materials.MaterialError):
                w.download('app', {}, dict(self.data, source_type=kind, source_id=owner, doc_id='doc'))

    def test_lost_membership_is_not_bypassed_by_download(self):
        self.conversation.side_effect = w.materials.MaterialError('denied')
        with self.assertRaises(w.materials.MaterialError):
            w.download('app', {}, dict(self.data, source_type='conversation',
                                      source_id='session', doc_id='doc'))

    def test_skill_id_is_not_an_authorization_bypass(self):
        with mock.patch.object(w, '_skill_id', return_value='writing-12'):
            for operation in (w.manifest, w.download):
                with self.assertRaisesRegex(w.materials.MaterialError, 'material_skill_mismatch'):
                    operation('app', {}, dict(self.data, skill_id='writing-99'))
            result = w.manifest('app', {}, dict(self.data, skill_id='writing-12'))
            self.assertEqual(result['skill_id'], 'writing-12')
            self.conversation.side_effect = w.materials.MaterialError('membership lost')
            with self.assertRaisesRegex(w.materials.MaterialError, 'membership lost'):
                w.identity('app', {}, self.data)

    def test_unavailable_original_remains_visible(self):
        self.doc.return_value['object_name'] = ''
        result = w.manifest('app', {}, self.data)
        self.assertFalse(result['documents'][0]['available'])
        with self.assertRaises(w.materials.MaterialError):
            w.download('app', {}, dict(self.data, source_type='conversation',
                                      source_id='session', doc_id='doc'))

    def test_limit_is_explicit_not_silent_truncation(self):
        self.embedding.get_embedding_doc_id_list.return_value = ['x'] * w.MAX_DOCUMENTS
        with self.assertRaises(w.materials.MaterialError):
            w.manifest('app', {}, self.data)

    def test_names_cannot_escape_workspace(self):
        self.assertEqual(w.filename('../../secret.txt', 'd'), 'secret.txt')
        self.assertNotIn('/', w.component('../../x'))
        self.assertNotIn('\\', w.filename('..\\x.txt', 'd'))
        self.assertNotIn('\n', w.filename('x\n.txt', 'd'))


class WritingSkillIdentityTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        self.addCleanup(self.engine.dispose)
        # Exercise real uniqueness/persistence; translate only MySQL upsert syntax.
        @event.listens_for(self.engine, 'before_cursor_execute', retval=True)
        def sqlite_upsert(conn, cursor, statement, parameters, context, executemany):
            return statement.replace('ON DUPLICATE KEY UPDATE id=id',
                                     'ON CONFLICT(app_id,seenical_session_id) DO NOTHING'), parameters
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE seenical_writing_skill ('
                              'id INTEGER PRIMARY KEY AUTOINCREMENT, app_id TEXT NOT NULL, '
                              'seenical_session_id TEXT NOT NULL, '
                              'created_at DATETIME DEFAULT CURRENT_TIMESTAMP, '
                              'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, '
                              'UNIQUE(app_id,seenical_session_id))'))
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(w.materials, '_engine', return_value=self.engine))
        self.scope = self.stack.enter_context(mock.patch.object(w, 'scope',
            side_effect=lambda app, actor, data: (data['seenical_session_id'], [], [])))

    def test_repeated_download_and_loop_change_keep_id(self):
        original = w.identity('app', {}, {'seenical_session_id': 's'})
        bound = w.identity('app', {}, {'seenical_session_id': 's', 'task_id': '191'})
        self.assertEqual(original['skill_id'], bound['skill_id'])
        self.assertEqual(bound['task_id'], '191')
        self.assertRegex(original['skill_id'], r'^writing-[1-9][0-9]*$')
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text('SELECT count(*) FROM seenical_writing_skill')).scalar(), 1)
            row = conn.execute(text('SELECT created_at,updated_at FROM seenical_writing_skill')).first()
            self.assertTrue(all(row))

    def test_other_apps_and_sessions_get_distinct_ids_and_cannot_resolve_another(self):
        first = w.identity('app', {}, {'seenical_session_id': 's'})
        second = w.identity('other', {}, {'seenical_session_id': 's'})
        third = w.identity('app', {}, {'seenical_session_id': 'other'})
        self.assertEqual(len({x['skill_id'] for x in (first, second, third)}), 3)
        with self.engine.connect() as conn:
            self.assertIsNone(w._skill_id(conn, 'unknown', 's'))
            self.assertEqual(w._skill_id(conn, 'other', 's'), second['skill_id'])

    def test_authorization_failure_does_not_allocate(self):
        self.scope.side_effect = w.materials.MaterialError('denied')
        with self.assertRaises(w.materials.MaterialError):
            w.identity('app', {}, {'seenical_session_id': 's'})
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text('SELECT count(*) FROM seenical_writing_skill')).scalar(), 0)


if __name__ == '__main__':
    unittest.main()
