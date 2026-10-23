"""Exercise the upload receipt and worker without booting model clients."""
import ast
import io
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def load_functions(filename, names, namespace):
    nodes = [node for node in ast.parse((ROOT / filename).read_text()).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), filename, 'exec'), namespace)


class SubmissionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tasks = {}
        self.docs = {}
        self.visible = []
        self.embedding = mock.Mock()
        self.embedding.get_embedding_name_info.return_value = {'embedding_uuid': 'kb'}
        self.embedding.get_embedding_uuid_info.return_value = {'vendor': 'openai', 'embedding_name': 'knowledge'}
        self.embedding.parse_file_ext.side_effect = lambda name: Path(name).suffix.lower()
        self.embedding.allow_exts.return_value = ['.txt', '.pdf', '.html']
        self.embedding.generate_doc_id.side_effect = lambda kb: 'doc-' + str(len(self.docs) + 1)
        self.embedding.create_task.side_effect = self.create_task
        self.embedding.get_task.side_effect = lambda kb, task: self.tasks.get(task)
        self.embedding.update_task_field.side_effect = lambda kb, task, key, value: self.tasks.setdefault(task, {}).update({key: value})
        self.embedding.get_doc.side_effect = lambda kb, doc: self.docs.get(doc)
        self.embedding.increase_task_field.side_effect = self.increase
        self.embedding.update_doc_field.side_effect = lambda kb, doc, key, value: self.docs[doc].update({key: value})
        self.embedding.create_doc_info.side_effect = self.create_doc
        self.embedding.add_doc_to_embedding.side_effect = lambda kb, doc: self.visible.append(doc)
        self.storage = mock.Mock()
        self.storage.upload.return_value = {'result': 'ok'}
        self.storage.put_object.return_value = {'result': 'ok'}
        self.storage.download_url.side_effect = self.download
        self.content = b'Example knowledge'
        self.indexer = mock.Mock()
        self.queue = mock.Mock()
        self.config = mock.Mock()
        self.config.get_lanying_connector.return_value = {'lanying_user_id': 1, 'lanying_admin_token': 'test', 'access_token': 'test'}
        self.chatbot = mock.Mock()
        self.chatbot.is_chatbot_mode.return_value = False
        self.ns = {'lanying_embedding': self.embedding, 'lanying_file_storage': self.storage,
                   'os': os, 'uuid': uuid, 'zipfile': zipfile, 'logging': logging,
                   'download_dir': self.temp.name, 'embedding_doc_dir': 'documents',
                   'process_embedding_file': self.indexer}
        load_functions('lanying_tasks.py', {'add_embedding_file', 'prepare_embedding_file',
                                          'maybe_task_finish', 'task_error',
                                          'process_embedding_file_internal'}, self.ns)
        self.service = {'lanying_embedding': self.embedding, 'lanying_config': self.config,
                        'lanying_chatbot': self.chatbot, 'add_embedding_file': self.queue,
                        'logging': logging}
        load_functions('services/openai_service.py', {'add_doc_to_embedding'}, self.service)

    def create_task(self, kb, kind, urls):
        task = 'task-' + str(len(self.tasks) + 1)
        self.tasks[task] = dict(status='wait', type=kind, url=urls[0],
                               processing_success_num=0, processing_fail_num=0)
        return task

    def increase(self, kb, task, key, amount):
        self.tasks[task][key] = int(self.tasks[task].get(key, 0)) + amount

    def create_doc(self, app, kb, name, object_name, doc, size, ext, kind, url, vendor, opts):
        self.docs[doc] = dict(filename=name, object_name=object_name, status='wait')

    def download(self, url, headers, filename):
        Path(filename).write_bytes(self.content)
        return {'result': 'ok'}

    def submit(self, name='example.txt'):
        self.service['add_doc_to_embedding']('app', 'knowledge', name, 'https://files/test',
                                             'file', -1, 0, [], [], False, {})
        return self.queue.apply_async.call_args.kwargs['args']

    def run_upload(self, name='example.txt'):
        args = self.submit(name)
        self.ns['add_embedding_file'](*args)
        return self.tasks['task-1']

    def test_receipt_exists_before_queue_publish_and_contains_no_download_url(self):
        def publish(**kwargs):
            self.assertEqual(self.tasks['task-1']['status'], 'wait')
            self.assertEqual(self.tasks['task-1']['file_name'], 'example.txt')
            self.assertNotIn('https://files', str(self.tasks))
        self.queue.apply_async.side_effect = publish
        self.submit()

    def test_queue_failure_retains_failed_receipt(self):
        self.queue.apply_async.side_effect = RuntimeError('queue unavailable')
        with self.assertRaises(RuntimeError):
            self.submit()
        self.assertEqual(self.tasks['task-1']['status'], 'error')
        self.assertEqual(self.tasks['task-1']['reason'], 'queue_failed')

    def test_regular_file_uses_existing_index_completion_counts(self):
        # A fast worker may finish before the preparation function returns.
        def index(**kwargs):
            doc = kwargs['args'][5]
            self.assertIn(doc, self.visible)
            self.assertEqual(self.docs[doc]['task_id'], 'task-1')
            self.increase('kb', 'task-1', 'processing_success_num', 1)
            self.ns['maybe_task_finish']('kb', 'task-1')
        self.indexer.apply_async.side_effect = index
        task = self.run_upload()
        self.assertEqual(task['processing_total_num'], 1)
        self.assertEqual(task['status'], 'add_finish')
        self.assertEqual(len(self.visible), 1)

    def test_zip_has_one_receipt_and_correct_success_and_failure_counts(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('notes.txt', 'notes')
            archive.writestr('unknown.bad', 'bad')
            archive.writestr('__MACOSX/._notes.txt', 'ignore')
        self.content = buffer.getvalue()
        task = self.run_upload('sources.zip')
        self.assertEqual(len(self.tasks), 1)
        self.assertEqual(task['status'], 'adding')
        self.assertEqual(task['processing_total_num'], 2)
        self.assertEqual(task['processing_fail_num'], 1)
        self.assertEqual(len(self.visible), 2)
        self.assertEqual(self.indexer.apply_async.call_args.kwargs['args'][4], 'notes.txt')
        self.increase('kb', 'task-1', 'processing_success_num', 1)
        self.ns['maybe_task_finish']('kb', 'task-1')
        self.assertEqual(task['status'], 'add_finish')

    def test_download_failure_is_visible_and_terminal(self):
        self.storage.download_url.side_effect = None
        self.storage.download_url.return_value = {'result': 'error', 'message': 'unavailable'}
        task = self.run_upload()
        self.assertEqual(task['status'], 'error')
        self.assertEqual(task['reason'], 'download_failed')
        self.assertEqual(self.docs[self.visible[0]]['status'], 'error')
        self.indexer.apply_async.assert_not_called()

    def test_invalid_zip_and_transfer_failure_do_not_stay_processing(self):
        with self.assertRaises(zipfile.BadZipFile):
            self.run_upload('broken.zip')
        self.assertEqual(self.tasks['task-1']['status'], 'error')
        self.storage.upload.return_value = {'result': 'error', 'message': 'unavailable'}
        with self.assertRaises(Exception):
            args = self.submit()
            self.ns['add_embedding_file'](*args)
        self.assertEqual(self.tasks['task-2']['status'], 'error')
        self.assertTrue(all(doc['status'] == 'error' for doc in self.docs.values()))
        self.indexer.apply_async.assert_not_called()

    def test_empty_zip_is_not_reported_as_indexed(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w'):
            pass
        self.content = buffer.getvalue()
        task = self.run_upload('empty.zip')
        self.assertEqual(task['reason'], 'empty_archive')
        self.assertEqual(task['status'], 'error')

    def test_unsupported_regular_file_has_failed_document(self):
        task = self.run_upload('file.bad')
        self.assertEqual(task['status'], 'add_finish')
        self.assertEqual(task['processing_fail_num'], 1)
        self.assertEqual(self.docs[self.visible[0]]['reason'], 'bad_ext')

    def test_index_dispatch_failure_does_not_leave_a_waiting_document(self):
        self.indexer.apply_async.side_effect = RuntimeError('queue unavailable')
        with self.assertRaises(RuntimeError):
            self.run_upload()
        self.assertEqual(self.tasks['task-1']['status'], 'error')
        self.assertEqual(self.docs[self.visible[0]]['status'], 'error')

    def test_old_queued_jobs_without_receipt_still_index(self):
        self.ns['add_embedding_file']('trace', 'app', 'knowledge', 'https://file', {}, 'test.txt', 'key')
        self.indexer.apply_async.assert_called_once()
        self.assertEqual(self.tasks, {})
        self.assertEqual(len(self.visible), 1)

    def test_cancel_during_download_does_not_create_docs_or_recreate_receipt(self):
        args = self.submit()
        def download(*values):
            result = self.download(*values)
            self.tasks.clear()
            return result
        self.storage.download_url.side_effect = download
        self.ns['add_embedding_file'](*args)
        self.assertEqual(self.tasks, {})
        self.assertEqual(self.docs, {})
        self.indexer.apply_async.assert_not_called()

    def test_cancel_during_storage_stops_dispatch_and_zip_preparation(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            archive.writestr('first.txt', 'first')
            archive.writestr('second.txt', 'second')
        for name, content, method in [('file.txt', b'text', self.storage.upload),
                                      ('files.zip', buffer.getvalue(), self.storage.put_object)]:
            with self.subTest(name=name):
                self.docs.clear()
                self.content = content
                args = self.submit(name)
                def cancel(*values):
                    self.tasks.clear()
                    return {'result': 'ok'}
                method.side_effect = cancel
                self.ns['add_embedding_file'](*args)
                self.assertEqual(self.tasks, {})
                self.assertEqual(len(self.docs), 1)
                self.assertTrue(all(doc['reason'] == 'submission_cancelled' for doc in self.docs.values()))
                self.indexer.apply_async.assert_not_called()

    def test_transfer_exception_after_cancel_does_not_recreate_receipt(self):
        args = self.submit()
        def fail(*values):
            self.tasks.clear()
            raise RuntimeError('storage unavailable')
        self.storage.upload.side_effect = fail
        with self.assertRaises(RuntimeError):
            self.ns['add_embedding_file'](*args)
        self.assertEqual(self.tasks, {})
        self.indexer.apply_async.assert_not_called()

    def test_deleted_queued_document_finishes_without_indexing_or_storage_charge(self):
        task = self.run_upload()
        queued = self.indexer.apply_async.call_args.kwargs
        self.assertEqual(queued['kwargs'], {'submission_task_id': 'task-1'})
        self.docs.clear()  # Deletion also removes the task ID from the doc metadata.
        self.ns['process_embedding_file_internal'](*queued['args'], **queued['kwargs'])
        self.assertEqual(task['status'], 'add_finish')
        self.assertEqual(task['processing_skip_num'], 1)
        self.assertEqual(task['processing_success_num'], 0)
        self.embedding.add_storage_size.assert_not_called()
        self.embedding.process_embedding_file.assert_not_called()

    def test_deleted_legacy_document_does_not_require_submission_id(self):
        self.ns['process_embedding_file_internal']('trace', 'app', 'kb', 'object', 'file.txt', 'missing')
        self.embedding.increase_task_field.assert_not_called()
        self.embedding.add_storage_size.assert_not_called()

    def test_recreated_knowledge_base_cannot_receive_old_upload(self):
        args = self.submit()
        self.embedding.get_embedding_name_info.return_value = {'embedding_uuid': 'another-kb'}
        with self.assertRaises(ValueError):
            self.ns['add_embedding_file'](*args)
        self.assertEqual(self.tasks['task-1']['status'], 'error')
        self.storage.download_url.assert_not_called()


if __name__ == '__main__':
    unittest.main()
