"""Exercise durable reference/state decisions using SQLite, with external I/O mocked.

SQL dialect adaptation is test-only; deployment still requires MySQL/InnoDB.
"""
import contextlib
import ast
import base64
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine, text
import lanying_seenical_materials as m


class MaterialReferencesTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE seenical_material_space (app_id TEXT PRIMARY KEY, embedding_name TEXT, embedding_uuid TEXT)'))
            conn.execute(text('CREATE TABLE seenical_material (app_id TEXT, doc_id TEXT, message_id TEXT, filename TEXT, source_url TEXT, object_name TEXT DEFAULT \'\', file_size INTEGER DEFAULT 0, status TEXT DEFAULT \'pending\', error_code TEXT DEFAULT \'\', worker_id TEXT DEFAULT \'\', updated_at TEXT, PRIMARY KEY(app_id,doc_id), UNIQUE(app_id,message_id))'))
            conn.execute(text('CREATE TABLE seenical_material_reference (app_id TEXT, owner_type TEXT, owner_id TEXT, doc_id TEXT, active INTEGER, PRIMARY KEY(app_id,owner_type,owner_id,doc_id))'))
            conn.execute(text('CREATE TABLE seenical_conversation_binding (app_id TEXT, chatbot_id TEXT, seenical_session_id TEXT)'))
            conn.execute(text("INSERT INTO seenical_material_space VALUES ('a','internal','1')"))
            for doc_id in ['1-1', '1-2']:
                conn.execute(text("INSERT INTO seenical_material(app_id,doc_id,filename,message_id,source_url,status) VALUES ('a',:d,'a.txt',:d,'https://api.maximtop.com/file','ready')"), {'d': doc_id})
        def dialect(sql):
            sql = sql.replace(' FOR UPDATE', '').replace('INSERT IGNORE', 'INSERT OR IGNORE')
            sql = sql.replace('ON DUPLICATE KEY UPDATE active=1',
                              'ON CONFLICT(app_id,owner_type,owner_id,doc_id) DO UPDATE SET active=1')
            return text(sql)
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(mock.patch.object(m, 'text', side_effect=dialect))
        self.stack.enter_context(mock.patch.object(m.storage, 'get_engine', return_value=self.engine))
        self.stack.enter_context(mock.patch.object(m.storage, 'is_enabled', return_value=True))
        self.queue = self.stack.enter_context(mock.patch.object(m, 'enqueue'))
        self.real_clear_index = m._clear_index
        self.clear = self.stack.enter_context(mock.patch.object(m, '_clear_index'))
        self.index = self.stack.enter_context(mock.patch.object(m, '_index_document'))
        self.store_source = self.stack.enter_context(mock.patch.object(m, '_store_source'))
        self.interrupted = self.stack.enter_context(mock.patch.object(m, 'worker_interrupted', return_value=False))
        self.stack.enter_context(mock.patch.object(m, 'clear_worker_interrupted'))
        self.stack.enter_context(mock.patch.object(m, 'reconcile_deleted_agents'))
        self.real_reconcile_owners = m.reconcile_plan_and_run_references
        self.reconcile_owners = self.stack.enter_context(mock.patch.object(m, 'reconcile_plan_and_run_references'))

    def tearDown(self):
        self.stack.close()
        self.engine.dispose()

    def state(self, doc='1-1'):
        with self.engine.connect() as conn:
            return m._document(conn, 'a', doc)['status']

    def set_state(self, state, doc='1-1'):
        with self.engine.begin() as conn:
            m._set_state(conn, 'a', doc, state)

    def test_live_loop_and_run_references_prevent_cleanup(self):
        for owner in ['session', 'plan', 'run']:
            m.set_references('a', owner, owner, ['1-1'])
        m.release_owner('a', 'session', 'session')
        m.release_owner('a', 'run', 'run')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'ready')
        m.release_owner('a', 'plan', 'plan')
        m.process_pending('a', 'job2')
        self.assertEqual(self.state(), 'unindexed')
        with self.engine.connect() as conn:
            self.assertIsNotNone(m._document(conn, 'a', '1-1'))

    def test_replace_and_clear_reference_sets(self):
        m.set_references('a', 'plan', 'p', ['1-1', '1-2', '1-1'])
        m.set_references('a', 'plan', 'p', ['1-2'])
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-2'])
        self.assertEqual(m.reference_ids('a', 'plan', 'p', False), ['1-1', '1-2'])
        m.release_owner('a', 'plan', 'p')
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), [])

    def test_failed_owner_write_rolls_back_reference_replacement_and_does_not_queue_cleanup(self):
        m.set_references('a', 'plan', 'p', ['1-1'])
        self.queue.reset_mock()
        def fail():
            raise RuntimeError('Redis unavailable')
        with self.assertRaises(RuntimeError):
            m.set_references('a', 'plan', 'p', ['1-2'], persist_owner=fail)
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])
        self.queue.assert_not_called()

    def test_successful_owner_write_commits_references_after_resource_is_persisted(self):
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM seenical_material_reference"))
        events = []
        self.queue.side_effect = lambda app: events.append('queue')
        m.set_references('a', 'plan', 'p', ['1-1'], persist_owner=lambda: events.append('persist'))
        self.assertEqual(events, ['persist', 'queue'])
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])

    def test_empty_references_without_material_space_still_persist_owner(self):
        persist = mock.Mock()
        m.set_references('no-materials-app', 'plan', 'p', [], persist_owner=persist)
        persist.assert_called_once_with()
        self.assertEqual(m.reference_ids('no-materials-app', 'plan', 'p'), [])

    def test_deleted_owners_and_terminal_runs_release_only_their_references(self):
        for owner_type, owner_id in [('plan', 'live-plan'), ('plan', 'deleted-plan'),
                                     ('run', 'running'), ('run', 'finished'), ('run', 'deleted-run')]:
            m.set_references('a', owner_type, owner_id, ['1-1'])
        grow = types.SimpleNamespace(
            get_task=lambda app, owner: {} if owner == 'live-plan' else None,
            get_task_run=lambda app, owner: {'status': 'running'} if owner == 'running' else (
                {'status': 'success'} if owner == 'finished' else None))
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow}):
            self.real_reconcile_owners('a')
        for owner_type, owner_id in [('plan', 'live-plan'), ('run', 'running')]:
            self.assertEqual(m.reference_ids('a', owner_type, owner_id), ['1-1'])
        for owner_type, owner_id in [('plan', 'deleted-plan'), ('run', 'finished'), ('run', 'deleted-run')]:
            self.assertEqual(m.reference_ids('a', owner_type, owner_id), [])

    def test_owner_read_failure_is_not_treated_as_deletion(self):
        m.set_references('a', 'plan', 'p', ['1-1'])
        grow = types.SimpleNamespace(get_task=mock.Mock(side_effect=RuntimeError('Redis unavailable')))
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow}):
            with self.assertRaises(RuntimeError):
                self.real_reconcile_owners('a')
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])

    def test_plan_and_run_delete_can_retry_after_database_or_redis_failure(self):
        for owner_type in ['plan', 'run']:
            for fault in ['mysql', 'redis', 'lost_reply']:
                with self.subTest(owner_type=owner_type, fault=fault):
                    self._assert_delete_retry(owner_type, fault)

    def _assert_delete_retry(self, owner_type, fault):
        from test_grow_ai_patch import load_grow_ai, task
        grow = load_grow_ai()
        owner_id = owner_type + '-' + fault
        m.set_references('a', owner_type, owner_id, ['1-1'])
        resource = task() if owner_type == 'plan' else {'task_id': 'p', 'status': 'success', 'file_size': 100}
        state = {'exists': True, 'usage': 100, 'attempts': 0}
        redis = mock.MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        commands = []
        pipe.incrby.side_effect = lambda key, value: commands.append(('usage', value))
        pipe.delete.side_effect = lambda key: commands.append(('delete', None))
        def execute():
            state['attempts'] += 1
            if fault == 'redis' and state['attempts'] == 1:
                commands.clear()
                raise RuntimeError('Redis unavailable before EXEC')
            for command, value in commands:
                if command == 'delete': state['exists'] = False
                if command == 'usage': state['usage'] += value
            commands.clear()
            if fault == 'lost_reply' and state['attempts'] == 1:
                raise RuntimeError('Redis EXEC reply lost')
        pipe.execute.side_effect = execute
        delete = grow.delete_task if owner_type == 'plan' else grow.delete_task_run
        getter = 'get_task' if owner_type == 'plan' else 'get_task_run'
        with mock.patch.object(grow, getter, side_effect=lambda *args: resource if state['exists'] else None), \
                mock.patch.object(grow, 'get_task_run_list', return_value={'data': {'list': []}}), \
                mock.patch.object(grow, 'delete_loop_conversation_binding'), \
                mock.patch.object(grow, 'get_service_statistic_key_list', return_value=['usage']), \
                mock.patch.object(grow.lanying_redis, 'redis_hgetall', create=True,
                                  side_effect=lambda *args: resource if state['exists'] else {}), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis):
            if fault == 'mysql':
                with mock.patch.object(m, '_engine', side_effect=RuntimeError('MySQL unavailable')):
                    with self.assertRaises(RuntimeError): delete('a', owner_id)
            else:
                with self.assertRaises(RuntimeError): delete('a', owner_id)
            self.assertEqual(m.reference_ids('a', owner_type, owner_id), ['1-1'])
            if fault != 'lost_reply':
                self.assertTrue(state['exists'])
                self.assertEqual(state['usage'], 100)
                self.assertEqual(delete('a', owner_id), {'result': 'ok', 'data': {'success': True}})
            else:
                self.assertFalse(state['exists'])
                # The queued worker recovers references after a lost EXEC reply.
                with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow}):
                    self.real_reconcile_owners('a')
            self.assertEqual(m.reference_ids('a', owner_type, owner_id), [])
            self.assertFalse(state['exists'])
            self.assertEqual(state['usage'], 0 if owner_type == 'run' else 100)
            if owner_type == 'run':
                with mock.patch.object(grow, 'get_task_run', return_value=resource):
                    # A stale initial snapshot must not refund storage again.
                    self.assertEqual(delete('a', owner_id), {'result': 'ok', 'data': {'success': True}})
                self.assertEqual(state['usage'], 0)

    def test_failed_plan_creation_does_not_leave_phantom_references(self):
        from test_grow_ai_patch import load_grow_ai
        grow = load_grow_ai()
        setting = types.SimpleNamespace(app_id='a', file_list=[], reference_document_ids=['1-1'],
                                        to_hmset_fields=lambda: {'cycle_type': 'none'})
        redis = mock.MagicMock()
        pipe = redis.pipeline.return_value.__enter__.return_value
        pipe.execute.side_effect = RuntimeError('Redis unavailable')
        with mock.patch.object(grow, 'check_task_content_security', return_value={'result': 'ok'}), \
                mock.patch.object(grow, 'set_admin_token'), \
                mock.patch.object(grow, 'generate_task_id', return_value='new-plan'), \
                mock.patch.object(grow, 'handle_task_file_list', return_value={'result': 'ok'}), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis):
            with self.assertRaises(RuntimeError):
                grow.create_task(setting, run_immediately=False)
        self.assertEqual(m.reference_ids('a', 'plan', 'new-plan'), [])
        self.queue.assert_not_called()
        pipe.hmset.assert_called_once()
        pipe.rpush.assert_called_once()

    def test_failed_plan_patch_preserves_existing_materials(self):
        from test_grow_ai_patch import load_grow_ai, task
        grow = load_grow_ai()
        m.set_references('a', 'plan', 'p', ['1-1'])
        self.queue.reset_mock()
        redis = mock.Mock()
        redis.hmset.side_effect = RuntimeError('Redis unavailable')
        with mock.patch.object(grow, 'get_task', return_value=task()), \
                mock.patch.object(grow, 'check_task_content_security', return_value={'result': 'ok'}), \
                mock.patch.object(grow.lanying_redis, 'get_redis_connection', return_value=redis):
            with self.assertRaises(RuntimeError):
                grow.patch_task('a', 'p', {'reference_document_ids': ['1-2']})
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])
        self.queue.assert_not_called()

    def test_cross_app_and_missing_documents_do_not_replace_existing_refs(self):
        m.set_references('a', 'plan', 'p', ['1-1'])
        with self.assertRaises(m.MaterialError):
            m.set_references('a', 'plan', 'p', ['other-app-document'])
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])
        with self.assertRaises(m.MaterialError):
            m.set_references('b', 'plan', 'p', ['1-1'])

    def test_pending_document_can_be_saved_but_cannot_run(self):
        self.set_state('pending')
        m.set_references('a', 'plan', 'p', ['1-1'])
        with self.assertRaises(m.MaterialError):
            m.set_references('a', 'run', 'r', ['1-1'], require_ready=True)
        self.assertEqual(m.reference_ids('a', 'run', 'r'), [])

    def test_busy_cleanup_cannot_be_referenced(self):
        for state in ['cleaning', 'cleanup_pending', 'cleanup_failed', 'unindexed']:
            self.set_state(state)
            with self.assertRaises(m.MaterialError):
                m.set_references('a', 'session', 's', ['1-1'])

    def test_restore_cannot_reactivate_queued_cleanup(self):
        m.set_references('a', 'session', 's', ['1-1'])
        m.release_owner('a', 'session', 's')
        self.set_state('cleanup_pending')
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            with self.assertRaises(m.MaterialError):
                m.material_api('a', {}, 'restore', {'seenical_session_id': 's', 'doc_id': '1-1'})
        self.assertEqual(m.reference_ids('a', 'session', 's'), [])
        self.assertEqual(self.state(), 'cleanup_pending')

    def test_removal_during_indexing_defers_cleanup(self):
        self.set_state('pending')
        m.set_references('a', 'session', 's', ['1-1'])
        self.index.side_effect = lambda *args: m.release_owner('a', 'session', 's')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'ready')
        m.process_pending('a', 'job2')
        self.assertEqual(self.state(), 'unindexed')

    def test_cleanup_failure_does_not_claim_success_and_can_be_retried(self):
        m.set_references('a', 'session', 's', ['1-1'])
        m.release_owner('a', 'session', 's')
        self.clear.side_effect = RuntimeError('vector database unavailable')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'cleanup_failed')
        self.clear.side_effect = None
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            m.material_api('a', {}, 'retry', {'seenical_session_id': 's', 'doc_id': '1-1'})
        self.assertEqual(m.reference_ids('a', 'session', 's'), [])
        m.process_pending('a', 'job2')
        self.assertEqual(self.state(), 'unindexed')

    def test_restoring_historical_file_reindexes_without_new_document(self):
        m.set_references('a', 'session', 's', ['1-1'])
        m.release_owner('a', 'session', 's')
        m.process_pending('a', 'job')
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            m.material_api('a', {}, 'restore', {'seenical_session_id': 's', 'doc_id': '1-1'})
        self.assertEqual(self.state(), 'pending')
        m.process_pending('a', 'job2')
        self.assertEqual(self.state(), 'ready')
        self.assertEqual(self.index.call_args.args[2]['doc_id'], '1-1')

    def test_other_session_cannot_restore_or_download_history(self):
        m.set_references('a', 'session', 's', ['1-1'])
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            for operation in ['restore', 'download']:
                with self.assertRaises(m.MaterialError):
                    m.material_api('a', {}, operation, {'seenical_session_id': 'other', 'doc_id': '1-1'})

    def test_worker_redelivery_recovers_only_its_own_claim(self):
        m.set_references('a', 'session', 's', ['1-1'])
        self.set_state('indexing')
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_material SET worker_id='job' WHERE doc_id='1-1'"))
        m.process_pending('a', 'other-job')
        self.assertEqual(self.state(), 'indexing')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'ready')

    def test_model_scope_never_uses_empty_filter_for_whole_library(self):
        grow = types.SimpleNamespace()
        message = {'appId': 'a', 'ext': {}, 'type': 'CHAT'}
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow}), \
                mock.patch.object(m, 'message_conversation', return_value={'seenical_session_id': 's'}):
            self.assertIsNone(m.retrieval_scope(message, {}))

    def test_invalid_file_header_and_source_are_rejected(self):
        for url in ['http://localhost/file', 'https://evilapi.maximtop.com/file',
                    'https://api.maximtop.com@evil.example/file', 'file:///tmp/file']:
            with self.assertRaises(m.MaterialError):
                m.validate_source(url)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fake.pdf'
            path.write_bytes(b'{"code":401}')
            with self.assertRaises(m.MaterialError):
                m.validate_file(str(path), '.pdf')

    def test_run_scope_uses_saved_documents_and_checks_identity(self):
        m.set_references('a', 'run', 'r', ['1-1'], require_ready=True)
        run = {'status': 'running', 'user_id': '9', 'material_input_snapshot': json.dumps(
            {'chatbot_id': '10', 'reference_document_ids': ['1-1']})}
        grow = types.SimpleNamespace(get_task_run=lambda *args: run)
        embedding = types.SimpleNamespace(get_embedding_name_info=lambda *args: {
            'embedding_name': 'internal', 'embedding_max_tokens': '2048', 'embedding_max_blocks': '5'})
        message = {'appId': 'a', 'from': {'uid': '9'}, 'to': {'uid': '11'},
                   'ext': {'seenical_task_run_id': 'r'}}
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow, 'lanying_embedding': embedding}):
            info = m.retrieval_scope(message, {'chatbot_id': '10', 'user_id': '11'})
            self.assertEqual(info['doc_ids'], ['1-1'])
            self.assertEqual(info['embedding_max_tokens'], 2048)
            self.assertEqual(info['embedding_max_blocks'], 5)
            message['from']['uid'] = 'other'
            with self.assertRaises(m.MaterialError):
                m.retrieval_scope(message, {'chatbot_id': '10', 'user_id': '11'})
            message['from']['uid'] = '9'
            m.release_owner('a', 'run', 'r')
            with self.assertRaises(m.MaterialError):
                m.retrieval_scope(message, {'chatbot_id': '10', 'user_id': '11'})

    def test_unready_session_does_not_retrieve_whole_library(self):
        m.set_references('a', 'session', 's', ['1-1'])
        self.set_state('pending')
        embedding = types.SimpleNamespace(get_embedding_name_info=lambda *args: {'embedding_name': 'internal'})
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': types.SimpleNamespace(), 'lanying_embedding': embedding}), \
                mock.patch.object(m, 'message_conversation', return_value={'seenical_session_id': 's'}):
            value = m.retrieval_scope({'appId': 'a', 'type': 'CHAT'}, {})
            self.assertEqual(value['doc_ids'], [])
            self.assertEqual(value['unready_count'], 1)

    def test_group_reader_must_still_be_member(self):
        m.set_references('a', 'session', 's', ['1-1'])
        im = types.SimpleNamespace(filter_group_member_ids=lambda *args: [])
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': types.SimpleNamespace(), 'lanying_im_api': im}), \
                mock.patch.object(m, 'message_conversation', return_value={'seenical_session_id': 's', 'conversation_id': 'g'}):
            self.assertIsNone(m.retrieval_scope({'appId': 'a', 'type': 'GROUPCHAT'}, {'user_id': '11'}))

    def test_listing_includes_cleanup_of_removed_documents_and_app_usage(self):
        m.set_references('a', 'session', 's', ['1-1'])
        m.release_owner('a', 'session', 's')
        self.set_state('cleaning')
        embedding = types.SimpleNamespace(get_embedding_usage=lambda app: {'storage_file_size': 100})
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding}), \
                mock.patch.object(m, 'authorized_conversation', return_value={}):
            value = m.material_api('a', {}, 'list', {'seenical_session_id': 's'})
            self.assertEqual(value['list'], [])
            self.assertTrue(value['processing'])
            self.assertEqual(value['usage']['storage_file_size'], 100)
            with self.assertRaises(m.MaterialError):
                m.material_api('a', {}, 'list', {'seenical_session_id': 's', 'offset': 'invalid'})

    def test_space_model_defaults_and_duplicate_message_do_not_reactivate(self):
        create = mock.Mock(return_value={'result': 'ok', 'embedding_uuid': '2'})
        embedding = types.SimpleNamespace(create_embedding=create, generate_embedding_id=lambda: '2', allow_exts=lambda: ['.txt'])
        config = types.SimpleNamespace(get_lanying_connector=lambda *args: {'product_id': '1'},
                                       get_lanying_connector_deduct_failed=lambda *args: False)
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding, 'lanying_config': config}):
            space = m.ensure_space('new-app')
            m.ensure_space('new-app')
            create.assert_not_called()
            m.initialize_index('new-app', space)
            create.assert_called_once_with('new-app', '__seenical_session_materials__', 350, 'COSINE', [], '', 30,
                                           'openai', 'text-embedding-ada-002', 'seenical_session', reserved_uuid='2')
            m.set_references('a', 'session', 's', ['1-1'])
            m.release_owner('a', 'session', 's')
            msg = {'appId': 'a', 'ctype': 'FILE', 'msgId': '1-1', 'ext': {'seenical': {'material_version': 1}},
                   'attachment': {'dName': 'a.txt', 'fLen': 10, 'url': 'https://api.maximtop.com/file'}}
            with mock.patch.object(m, 'message_conversation', return_value={'seenical_session_id': 's'}), \
                    mock.patch.object(m, 'validate_source'), mock.patch.object(m, 'validate_attachment_identity'):
                self.assertTrue(m.ingest_message(msg))
                self.assertEqual(m.reference_ids('a', 'session', 's'), [])

    def test_attachment_must_match_real_message_and_download_route(self):
        from urllib.parse import quote
        sign = base64.b64encode(b'1|2|file|2|signed-digest').decode()
        url = 'https://api.maximtop.com/file/download/chat?file_sign=' + quote(sign)
        msg = {'type': 'GROUPCHAT', 'from': {'uid': '1'}, 'to': {'uid': '2'}}
        m.validate_source(url)
        m.validate_attachment_identity(url, msg)
        msg['to']['uid'] = 'other-group'
        with self.assertRaises(m.MaterialError):
            m.validate_attachment_identity(url, msg)
        for bad in ['https://api.maximtop.com/user/info?file_sign=x', url + '&access-token=secret',
                    url.replace('https:', 'http:')]:
            with self.assertRaises(m.MaterialError):
                m.validate_source(bad)

    def test_remove_is_independent_of_other_failed_documents_and_collection_limit(self):
        m.set_references('a', 'session', 's', ['1-1', '1-2'])
        self.set_state('cleanup_failed', '1-2')
        with self.engine.begin() as conn:
            for index in range(501):
                conn.execute(text("INSERT INTO seenical_material_reference VALUES ('a','session','s',:d,1)"),
                             {'d': 'historical-' + str(index)})
        m.remove_reference('a', 's', '1-1')
        self.assertNotIn('1-1', m.reference_ids('a', 'session', 's'))
        self.assertIn('1-2', m.reference_ids('a', 'session', 's'))
        m.release_owner('a', 'session', 's')
        self.assertEqual(m.reference_ids('a', 'session', 's'), [])

    def test_pending_removed_file_still_saves_original_without_indexing(self):
        self.set_state('pending')
        m.set_references('a', 'session', 's', ['1-1'])
        m.remove_reference('a', 's', '1-1')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'unindexed')
        self.assertIn('1-1', [c.args[2]['doc_id'] for c in self.store_source.call_args_list])
        self.index.assert_not_called()

    def test_interrupted_worker_can_be_retried_without_stealing_live_worker(self):
        m.set_references('a', 'session', 's', ['1-1'])
        self.set_state('indexing')
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_material SET worker_id='stopped' WHERE doc_id='1-1'"))
        self.interrupted.side_effect = lambda a, w: w == 'stopped'
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            m.material_api('a', {}, 'retry', {'seenical_session_id': 's', 'doc_id': '1-1'})
        m.process_pending('a', 'recovery')
        self.assertEqual(self.state(), 'pending')
        self.index.assert_not_called()
        m.process_pending('a', 'index-again')
        self.assertEqual(self.state(), 'ready')

    def test_delete_agent_releases_its_sessions_but_preserves_plan_and_other_agent(self):
        with self.engine.begin() as conn:
            for session, agent in [('primary:bot', 'bot'), ('child', 'bot'), ('other', 'other-bot')]:
                conn.execute(text('INSERT INTO seenical_conversation_binding VALUES (:a,:b,:s)'),
                             {'a': 'a', 'b': agent, 's': session})
        for session in ['primary:bot', 'child', 'other']:
            m.set_references('a', 'session', session, ['1-1'])
        m.set_references('a', 'plan', 'p', ['1-1'])
        m.release_chatbot_sessions('a', 'bot')
        self.assertEqual(m.reference_ids('a', 'session', 'primary:bot'), [])
        self.assertEqual(m.reference_ids('a', 'session', 'child'), [])
        self.assertEqual(m.reference_ids('a', 'session', 'other'), ['1-1'])
        self.assertEqual(m.reference_ids('a', 'plan', 'p'), ['1-1'])

    def test_failed_validation_is_persisted_and_does_not_require_model(self):
        embedding = types.SimpleNamespace(allow_exts=lambda: ['.txt'])
        msg = {'appId': 'a', 'ctype': 'FILE', 'msgId': 'new-file',
               'ext': {'seenical': {'material_version': 1}},
               'attachment': {'dName': 'archive.zip', 'fLen': 10, 'url': 'https://api.maximtop.com/file'}}
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding}), \
                mock.patch.object(m, 'message_conversation', return_value={'seenical_session_id': 's'}), \
                mock.patch.object(m, 'validate_source'), mock.patch.object(m, 'validate_attachment_identity'):
            self.assertTrue(m.ingest_message(msg))
        ids = m.reference_ids('a', 'session', 's')
        with self.engine.connect() as conn:
            doc = m._document(conn, 'a', ids[0])
        self.assertEqual(doc['status'], 'failed')
        self.assertEqual(doc['error_code'], 'material_unsupported_format')
        self.queue.assert_not_called()

    def test_model_failure_after_ingest_remains_retryable(self):
        self.set_state('pending')
        m.set_references('a', 'session', 's', ['1-1'])
        self.index.side_effect = m.MaterialError('material_model_unavailable')
        m.process_pending('a', 'job')
        self.assertEqual(self.state(), 'failed')
        with self.engine.connect() as conn:
            self.assertEqual(m._document(conn, 'a', '1-1')['error_code'], 'material_model_unavailable')
        with mock.patch.object(m, 'authorized_conversation', return_value={}):
            m.material_api('a', {}, 'retry', {'seenical_session_id': 's', 'doc_id': '1-1'})
        self.assertEqual(self.state(), 'pending')

    def test_first_index_creation_failure_can_retry_after_service_recovers(self):
        from redis.exceptions import ResponseError
        self.set_state('pending')
        self.set_state('unindexed', '1-2')
        m.set_references('a', 'session', 's', ['1-1'])
        embedding = types.SimpleNamespace(
            create_embedding=mock.Mock(side_effect=[RuntimeError('vector service unavailable'), {'result': 'ok'}]),
            get_embedding_uuid_info=lambda u: {'db_type': 'redis', 'index': 'index'},
            search_doc_data_and_delete=mock.Mock(side_effect=ResponseError('Unknown Index name')))
        self.clear.side_effect = self.real_clear_index
        self.index.side_effect = lambda app, space, doc: m.initialize_index(app, space)
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding}), \
                mock.patch.object(m, 'update_usage'), \
                mock.patch.object(m, 'authorized_conversation', return_value={}):
            m.process_pending('a', 'first')
            self.assertEqual(self.state(), 'failed')
            m.material_api('a', {}, 'retry', {'seenical_session_id': 's', 'doc_id': '1-1'})
            m.process_pending('a', 'second')
        self.assertEqual(self.state(), 'ready')
        self.assertEqual(embedding.create_embedding.call_count, 2)

    def test_http_200_business_error_is_not_a_text_attachment(self):
        response = mock.MagicMock(status_code=200, headers={'Content-Type': 'application/json;charset=UTF-8'})
        response.iter_content.return_value = [b'{"code":401,"message":"token expired"}']
        request = types.SimpleNamespace(get=mock.Mock(return_value=response))
        config = types.SimpleNamespace(get_lanying_connector=lambda a: {'lanying_admin_token': 'private'})
        tools = types.SimpleNamespace(get_im_binding_projection=lambda a: {'im_user_id': '1'})
        with mock.patch.dict('sys.modules', {'requests': request, 'lanying_config': config, 'lanying_agent_tools': tools}), \
                mock.patch.object(m, 'validate_source'), tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.txt'
            with self.assertRaisesRegex(m.MaterialError, 'material_download_failed'):
                m._download_source('a', 'https://api.maximtop.com/file/download/chat', str(path))
            self.assertFalse(path.exists())
            response.headers = {'Content-Type': 'text/plain'}
            response.iter_content.return_value = [b'actual document']
            self.assertEqual(m._download_source('a', 'url', str(path)), 15)

    def test_download_metadata_available_before_any_index_was_created(self):
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_material SET object_name='saved/source.txt', file_size=12 WHERE doc_id='1-1'"))
        doc = m.stored_document_metadata('a', '1-1')
        self.assertEqual(doc['object_name'], 'saved/source.txt')
        self.assertEqual(doc['type'], 'file')
        self.assertIsNone(m.stored_document_metadata('other-app', '1-1'))


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def __enter__(self): return self
    def __exit__(self, *args): pass
    def watch(self, *args): pass
    def multi(self): pass
    def hgetall(self, key): return self.redis.data.get(key, {}).copy()
    def hset(self, *args): self.commands.append(('set', args))
    def hincrby(self, *args): self.commands.append(('incr', args))
    def execute(self):
        for command, (key, field, value) in self.commands:
            data = self.redis.data.setdefault(key, {})
            data[field] = value if command == 'set' else int(data.get(field, 0)) + value


class UsageTest(unittest.TestCase):
    def test_repeated_reserve_and_cleanup_are_idempotent(self):
        redis = types.SimpleNamespace(data={})
        redis.pipeline = lambda: FakePipeline(redis)
        embedding = types.SimpleNamespace(
            get_embedding_doc_info_key=lambda u, d: 'doc:' + d,
            get_app_embedding_app_info_key=lambda a: 'app:' + a,
            get_embedding_uuid_key=lambda u: 'library:' + u,
            get_app_config_int=lambda a, key: 1 if key.endswith('storage_limit') else 0)
        redis_module = types.SimpleNamespace(get_redis_stack_connection=lambda: redis,
                                            redis_hgetall=lambda pipe, key: pipe.hgetall(key))
        with mock.patch.dict('sys.modules', {'lanying_embedding': embedding, 'lanying_redis': redis_module}):
            m.update_usage('a', '1', '1-1', 1024)
            m.update_usage('a', '1', '1-1', 1024)
            self.assertEqual(redis.data['app:a']['storage_file_size'], 1024)
            with self.assertRaises(m.MaterialError):
                m.update_usage('a', '1', '1-2', 1024 * 1024)
            self.assertEqual(redis.data['app:a']['storage_file_size'], 1024)
            m.update_usage('a', '1', '1-1', 0, clear=True)
            m.update_usage('a', '1', '1-1', 0, clear=True)
            self.assertEqual(redis.data['app:a']['storage_file_size'], 0)
            self.assertEqual(redis.data['library:1']['storage_file_size'], 0)


class VectorIndexHelpersTest(unittest.TestCase):
    """Load only changed pure helpers: importing the full legacy module starts
    external tokenizer/browser initialization, unrelated to these operations.
    """
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / 'lanying_embedding.py'
        tree = ast.parse(source.read_text())
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name in {'create_embedding', '_create_embedding_index', 'query_by_doc_id', 'query_by_doc_ids'}]
        self.namespace = {}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), 'exec'), self.namespace)

    def test_redis_multiple_document_filter_is_union(self):
        self.assertEqual(self.namespace['query_by_doc_ids'](['1-1', '1-2']), r'@doc_id:{1\-1|1\-2}')

    def test_long_material_and_legacy_document_ids_use_the_same_tag_escaping(self):
        for doc_id in ['1-1', '123-' + 'a' * 32, 'legacy-id-' + 'b' * 30, 'legacyidwithoutpunctuation']:
            self.assertEqual(self.namespace['query_by_doc_id'](doc_id),
                             '@doc_id:{' + doc_id.replace('-', r'\-') + '}')

    def test_internal_index_recovery_does_not_drop_or_reset_existing_index(self):
        redis = mock.Mock()
        redis.execute_command.side_effect = RuntimeError('Index already exists')
        self.namespace['lanying_redis'] = types.SimpleNamespace(get_redis_stack_connection=lambda: redis)
        info = {'db_type': 'redis', 'index': 'index', 'prefix': 'prefix', 'algo': 'COSINE'}
        self.namespace['_create_embedding_index'](info, 1536, idempotent=True)
        with self.assertRaises(RuntimeError):
            self.namespace['_create_embedding_index'](info, 1536)
        redis.execute_command.side_effect = RuntimeError('Connection lost')
        with self.assertRaises(RuntimeError):
            self.namespace['_create_embedding_index'](info, 1536, idempotent=True)
        self.assertTrue(all(call.args[0] == 'FT.CREATE' for call in redis.execute_command.call_args_list))

    def test_internal_initial_metadata_is_atomic_and_retry_preserves_usage(self):
        for commit_before_error in [False, True]:
            with self.subTest(commit_before_error=commit_before_error):
                metadata = {}
                commands = []
                redis = mock.MagicMock()
                pipe = redis.pipeline.return_value.__enter__.return_value
                pipe.hmset.side_effect = lambda key, fields: commands.append((key, dict(fields)))
                attempts = []
                def execute():
                    attempts.append(True)
                    if len(attempts) == 1 and not commit_before_error:
                        commands.clear()
                        raise RuntimeError('Redis unavailable')
                    metadata.update(commands)
                    commands.clear()
                    if len(attempts) == 1:
                        raise RuntimeError('Redis EXEC reply lost')
                pipe.execute.side_effect = execute
                self.namespace.update({
                    'logging': mock.Mock(), 'time': types.SimpleNamespace(time=lambda: 1),
                    'get_embedding_default_db_type': lambda app: 'redis',
                    'get_embedding_name_info': lambda *args: metadata.get('name'),
                    'get_embedding_uuid_info': lambda uid: metadata.get('uuid'),
                    'get_embedding_name_key': lambda *args: 'name', 'get_embedding_uuid_key': lambda uid: 'uuid',
                    'get_embedding_index_key': lambda uid: 'index', 'get_embedding_data_prefix_key': lambda uid: 'prefix',
                    'lanying_vendor': types.SimpleNamespace(get_embedding_model_config=lambda *args: {'model': 'text-embedding-ada-002', 'dim': 1536}),
                    'lanying_redis': types.SimpleNamespace(get_redis_stack_connection=lambda: redis),
                    'update_app_embedding_admin_users': mock.Mock(), 'bind_preset_name': mock.Mock()})
                args = ('a', '__seenical_session_materials__', 350, 'COSINE', [], '', 30,
                        'openai', 'text-embedding-ada-002', 'seenical_session')
                with self.assertRaises(RuntimeError):
                    self.namespace['create_embedding'](*args, reserved_uuid='1')
                self.assertEqual(set(metadata), {'name', 'uuid'} if commit_before_error else set())
                self.assertEqual(self.namespace['create_embedding'](*args, reserved_uuid='1')['result'], 'ok')
                metadata['uuid'].update(storage_file_size=123, embedding_count=7, doc_id_seq=9)
                redis.execute_command.side_effect = RuntimeError('Index already exists')
                self.assertEqual(self.namespace['create_embedding'](*args, reserved_uuid='1')['result'], 'ok')
                self.assertEqual(metadata['uuid']['storage_file_size'], 123)
                self.assertEqual(metadata['uuid']['embedding_count'], 7)
                self.assertEqual(metadata['uuid']['doc_id_seq'], 9)
                redis.hmset.assert_not_called()
                redis.rpush.assert_not_called()


class MissingIndexCleanupTest(unittest.TestCase):
    def test_only_missing_indexes_allow_cleanup_and_refund(self):
        from redis.exceptions import ResponseError, ConnectionError
        from psycopg2.errors import UndefinedTable
        for db_type, error, succeeds in [
                ('redis', ResponseError('Unknown Index name'), True),
                ('pgvector', UndefinedTable('relation does not exist'), True),
                ('redis', ConnectionError('Connection lost'), False),
                ('redis', ResponseError('Syntax error'), False),
                ('pgvector', RuntimeError('database unavailable'), False)]:
            with self.subTest(db_type=db_type, error=type(error).__name__):
                embedding = types.SimpleNamespace(
                    get_embedding_uuid_info=lambda u: {'db_type': db_type, 'index': 'index', 'db_table_name': 'table'},
                    search_doc_data_and_delete=mock.Mock(side_effect=error))
                with mock.patch.dict('sys.modules', {'lanying_embedding': embedding}), \
                        mock.patch.object(m, 'update_usage') as usage:
                    if succeeds:
                        m._clear_index('a', {'embedding_uuid': '1', 'embedding_name': 'internal'}, {'doc_id': '1-1'})
                        usage.assert_called_once_with('a', '1', '1-1', 0, clear=True)
                    else:
                        with self.assertRaises(type(error)):
                            m._clear_index('a', {'embedding_uuid': '1', 'embedding_name': 'internal'}, {'doc_id': '1-1'})
                        usage.assert_not_called()


if __name__ == '__main__':
    unittest.main()
