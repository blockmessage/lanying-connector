"""Unified file-list pagination, public API compatibility and scope checks."""
import copy
import logging
import unittest
from unittest import mock

from test_embedding_submission import load_functions


class FileListTest(unittest.TestCase):
    def setUp(self):
        self.redis = mock.Mock()
        self.tasks = []
        self.docs = {}
        self.ids = []
        self.redis.llen.side_effect = lambda key: len(self.ids)
        self.lookup = mock.Mock(side_effect=lambda app, name: {'embedding_uuid': 'kb'}
                                if (app, name) == ('app', 'knowledge') else None)
        self.ns = {
            'logging': logging,
            'lanying_redis': mock.Mock(get_redis_stack_connection=lambda: self.redis),
            'get_embedding_name_info': self.lookup,
            'get_task_list': lambda kb: copy.deepcopy(self.tasks),
            'get_embedding_doc_list_key': lambda kb: 'docs:' + kb,
            'redis_lrange': lambda redis, key, start, end: self.ids[start:] if end == -1 else self.ids[start:end + 1],
            'get_doc': lambda kb, doc: copy.deepcopy(self.docs.get(doc)),
        }
        load_functions('lanying_embedding.py', {'get_embedding_file_list', 'get_embedding_doc_info_list'}, self.ns)

    def populate(self, uploads, documents):
        self.tasks = [dict(task_id='u' + str(i), submission='1', status='processing',
                           file_name='file-' + str(i)) for i in range(uploads)]
        self.ids = ['d' + str(i) for i in range(documents)]
        self.docs = {doc: dict(filename=doc, status='finish', object_name='objects/' + doc) for doc in self.ids}

    def get(self, page=1, size=6, app='app', name='knowledge'):
        return self.ns['get_embedding_file_list'](app, name, page, size)

    def test_all_pages_have_one_total_and_no_client_side_merge(self):
        for uploads in (0, 1, 6, 8, 13):
            for documents in (0, 1, 6, 15):
                self.populate(uploads, documents)
                records = []
                total = uploads + documents
                for page in range(1, max(1, (total + 5) // 6) + 1):
                    result = self.get(page)
                    self.assertEqual(result['total'], total)
                    self.assertEqual(result['page'], page)
                    self.assertLessEqual(len(result['list']), 6)
                    self.assertEqual(result['tasks'], [])
                    records += [row['record_id'] for row in result['list']]
                expected = ['upload:u' + str(i) for i in reversed(range(uploads))]
                expected += ['document:d' + str(i) for i in reversed(range(documents))]
                self.assertEqual(records, expected)
                self.assertEqual(len(records), len(set(records)))
                self.assertEqual(self.get(999)['page'], max(1, (total + 5) // 6))

    def test_uploads_are_not_document_ids_and_cannot_download_or_index(self):
        self.populate(2, 1)
        self.tasks[0]['status'] = 'error'
        self.tasks[0]['reason'] = 'queue_failed'
        rows = self.get()['list']
        pending, failed, doc = rows
        self.assertEqual(pending['status'], 'preparing')
        self.assertFalse(pending['can_delete'])
        self.assertTrue(failed['can_delete'])
        self.assertEqual(failed['reason'], 'queue_failed')
        for row in (pending, failed):
            self.assertNotIn('doc_id', row)
            self.assertFalse(row['downloadable'])
            self.assertFalse(row['can_edit'])
            self.assertFalse(row['can_regenerate'])
        self.assertEqual(doc['doc_id'], 'd0')
        self.assertTrue(doc['can_delete'])

    def test_replaced_receipt_does_not_add_to_total_and_site_tasks_stay_separate(self):
        self.populate(0, 2)
        self.tasks = [dict(task_id='zip', submission='1', status='adding'),
                      dict(task_id='done', submission='1', status='add_finish'),
                      dict(task_id='site', type='site', status='finish')]
        result = self.get()
        self.assertEqual(result['total'], 2)
        self.assertEqual([t['task_id'] for t in result['tasks']], ['site'])
        self.assertTrue(result['has_pending'], 'An upload still indexing can be off the current page')
        self.tasks[0]['status'] = 'add_finish'
        self.assertFalse(self.get()['has_pending'], 'Crawling finished waits for the existing explicit Add action')

    def test_scope_and_pagination_validation_precede_reads(self):
        self.populate(1, 1)
        self.assertEqual(self.get(app='other')['total'], 0)
        self.assertEqual(self.get(name='other')['total'], 0)
        for page, size in [(0, 6), (-1, 6), (1, 0), (1, 101), (True, 6), ('1', 6)]:
            with self.assertRaises(ValueError):
                self.get(page, size)
        with self.assertRaises(ValueError):
            self.get(name='__seenical_session_materials__')

    def test_original_document_api_keeps_original_order_and_count(self):
        self.populate(3, 2)
        self.redis.llen.return_value = 2
        total, rows = self.ns['get_embedding_doc_info_list']('app', 'knowledge', 0, 1)
        self.assertEqual(total, 2)
        self.assertEqual([row['doc_id'] for row in rows], ['d0', 'd1'])
        self.assertNotIn('record_type', rows[0])

    def test_service_downloadability_and_delete_checks(self):
        embedding = mock.Mock()
        embedding.get_embedding_file_list.return_value = {'list': [
            {'record_type': 'upload'}, {'record_type': 'document', 'object_name': 'x', 'type': 'file'},
            {'record_type': 'document', 'status': 'error'}]}
        ns = {'lanying_embedding': embedding}
        load_functions('services/openai_service.py', {'list_embedding_files', 'is_downloadable_embedding_doc', 'delete_embedding_task'}, ns)
        rows = ns['list_embedding_files']('app', 'knowledge', 1, 6)['list']
        self.assertEqual([row['downloadable'] for row in rows], [False, True, False])
        embedding.get_embedding_name_info.return_value = {'embedding_uuid': 'kb'}
        for status in ('wait', 'processing', 'adding'):
            embedding.get_task.return_value = {'submission': '1', 'status': status}
            with self.assertRaises(ValueError):
                ns['delete_embedding_task']('app', 'knowledge', 'upload')
        embedding.delete_task.assert_not_called()
        for task in ({'submission': '1', 'status': 'error'}, {'type': 'site', 'status': 'processing'}):
            embedding.get_task.return_value = task
            ns['delete_embedding_task']('app', 'knowledge', 'task')
        self.assertEqual(embedding.delete_task.call_count, 2)

    def test_http_route_requires_service_auth_and_valid_scope(self):
        request = mock.Mock(headers={})
        app = mock.Mock(make_response=lambda body: body)
        service = mock.Mock()
        service.list_embedding_files.side_effect = self.ns['get_embedding_file_list']
        get_service = mock.Mock(return_value=service)
        ns = {'request': request, 'app': app, 'accessToken': 'service-secret', 'get_service_module': get_service}
        load_functions('lanying_connector.py', {'list_embedding_files'}, ns)
        route = ns['list_embedding_files']
        self.assertEqual(route('openai')[1], 401)
        get_service.assert_not_called()
        request.headers = {'access-token': 'service-secret'}
        for body in ([], {'app_id': 'app'}, {'app_id': 'app', 'embedding_name': 'knowledge', 'page_size': 101},
                     {'app_id': 'app', 'embedding_name': '__seenical_session_materials__'}):
            request.get_json.return_value = body
            self.assertEqual(route('openai')[1], 400)
        request.get_json.return_value = {'app_id': 'app', 'embedding_name': 'knowledge'}
        self.populate(1, 1)
        result = route('openai')
        self.assertEqual(result['code'], 200)
        self.assertEqual(result['data']['total'], 2)
        self.assertEqual(len(result['data']['list']), 2)


if __name__ == '__main__':
    unittest.main()
