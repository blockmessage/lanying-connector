import contextlib
import unittest
from unittest import mock

import lanying_writing_materials as w


class WritingMaterialsTest(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.embedding = mock.Mock()
        self.chatbot = mock.Mock()
        self.grow = mock.Mock()
        self.config = mock.Mock()
        self.config.get_service_config.return_value = {'embeddings': {'writer': ['kb']}}
        self.chatbot.get_chatbot.return_value = {'name': 'writer'}
        self.grow.get_task.return_value = {'chatbot_id': 'agent'}
        self.embedding.get_preset_embedding_infos.return_value = [
            {'embedding_uuid': 'kb', 'embedding_name': 'Shared', 'type': 'normal'},
            {'embedding_uuid': 'private', 'embedding_name': w.materials.INTERNAL_NAME,
             'type': w.materials.INTERNAL_TYPE},
            {'embedding_uuid': 'functions', 'embedding_name': 'tools', 'type': 'function'}]
        self.embedding.get_embedding_doc_id_list.return_value = ['kb-1']
        self.embedding.get_doc.return_value = {
            'filename': 'facts.txt', 'object_name': 'secret-storage-location', 'file_size': 12}
        self.stack.enter_context(mock.patch.dict('sys.modules', {
            'lanying_embedding': self.embedding, 'lanying_chatbot': self.chatbot,
            'lanying_config': self.config, 'lanying_grow_ai': self.grow}))
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
        self.assertEqual(result['documents'][0]['path'], 'materials/conversations/session/doc/note.txt')
        self.assertFalse(result['documents'][0]['read_only'])
        self.assertTrue(result['documents'][1]['read_only'])
        self.assertNotIn('object_name', str(result))
        self.assertNotIn('private-object', str(result))
        self.embedding.get_preset_embedding_infos.assert_called_once_with(
            {'writer': ['kb']}, 'app', 'writer')
        self.refs.assert_called_once_with('app', 'session', 'session')

    def test_loop_uses_saved_plan_not_current_session_references(self):
        w.manifest('app', {}, dict(self.data, task_id='plan'))
        self.refs.assert_called_once_with('app', 'plan', 'plan')

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


if __name__ == '__main__':
    unittest.main()
