"""Whole-site indexing admission and queue handoff without external services."""
import json
import logging
import os
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, WatchError

from test_embedding_submission import load_functions


class CrawlIndexTest(unittest.TestCase):
    def setUp(self):
        self.redis = mock.MagicMock()
        self.pipe = self.redis.pipeline.return_value.__enter__.return_value
        self.crawler = mock.Mock()
        ns = {
            'lanying_redis': mock.Mock(get_redis_stack_connection=lambda: self.redis),
            'lanying_url_loader': self.crawler,
            'WatchError': WatchError, 'uuid': uuid,
        }
        names = {'claim_site_index', '_update_site_batch', 'take_site_batch', 'update_site_batch', 'delete_task',
                 'get_embedding_task_info_key', 'get_embedding_task_detail_key',
                 'get_embedding_task_list_key', 'redis_hgetall', 'redis_hget',
                 'take_pending_site_doc', 'get_embedding_doc_info_key'}
        ns['notify_workspace_material_change'] = mock.Mock()
        load_functions('lanying_embedding.py', names, ns)
        self.functions = ns
        self.info_key = ns['get_embedding_task_info_key']('kb', 'site')
        self.detail_key = ns['get_embedding_task_detail_key']('kb', 'site')
        self.task_list_key = ns['get_embedding_task_list_key']('kb')
        self.pipe.hlen.return_value = 2

    def task(self, status='finish', **fields):
        row = {'type': 'site', 'status': status, 'site_task_id': 'crawl', **fields}
        self.pipe.hgetall.return_value = {key.encode(): value.encode() for key, value in row.items()}

    def test_claim_only_accepts_usable_finished_crawl(self):
        claim = self.functions['claim_site_index']
        self.task(status='adding')
        self.assertEqual(claim('kb', 'site', 'trace'), 'task_state_changed')
        self.task()
        self.pipe.hlen.return_value = 0
        self.assertEqual(claim('kb', 'site', 'trace'), 'empty_crawl')
        self.task(block_num='0', file_size='1024')
        self.pipe.hlen.return_value = 1
        self.assertEqual(claim('kb', 'site', 'trace'), 'empty_crawl')
        self.pipe.multi.assert_not_called()

        self.pipe.reset_mock()
        self.task(block_num='2')
        self.pipe.hlen.return_value = 2
        self.assertIsNone(claim('kb', 'site', 'trace'))
        self.assertEqual(self.pipe.method_calls, [
            mock.call.watch(self.info_key), mock.call.hgetall(self.info_key), mock.call.hlen(self.detail_key),
            mock.call.multi(), mock.call.hset(self.info_key, mapping={
                'status': 'adding', 'processing_total_num': 2, 'site_batch_id': 'trace', 'site_batch_owner': '',
            }), mock.call.execute(),
        ])

    def test_concurrent_claim_or_discard_loses_when_task_changed(self):
        def other_claim_won():
            self.task(status='adding', site_batch_id='other-trace')
            raise WatchError('watched key changed')

        self.task()
        self.pipe.execute.side_effect = other_claim_won
        self.assertEqual(self.functions['claim_site_index']('kb', 'site', 'trace'), 'task_state_changed')
        self.task()
        self.assertEqual(self.functions['delete_task']('kb', 'site', protect_active=True), 'task_state_changed')
        self.crawler.clean_task.assert_not_called()

    def test_index_claim_recovers_own_committed_batch(self):
        def committed():
            self.task(status='adding', site_batch_id='trace')
            raise WatchError('connection interrupted after EXEC')

        self.task()
        self.pipe.execute.side_effect = committed
        self.assertIsNone(self.functions['claim_site_index']('kb', 'site', 'trace'))
        self.pipe.execute.assert_called_once_with()

    def test_discard_preserves_active_task_and_clears_completed_site(self):
        discard = self.functions['delete_task']
        self.task(status='adding')
        self.assertEqual(discard('kb', 'site', protect_active=True), 'task_is_processing')
        self.task(status='processing', type='file', submission='1')
        self.assertEqual(discard('kb', 'site', protect_active=True), 'upload_is_processing')
        self.pipe.delete.assert_not_called()
        self.task(status='add_finish')
        self.assertIsNone(discard('kb', 'site', protect_active=True))
        self.pipe.delete.assert_called_once_with(self.info_key, self.detail_key)
        self.pipe.lrem.assert_called_once_with(self.task_list_key, 2, 'site')
        self.crawler.clean_task.assert_called_once_with('crawl')

    def test_publish_failure_can_restore_only_unstarted_matching_claim(self):
        update = self.functions['update_site_batch']
        take = self.functions['take_site_batch']
        reset = {'status': 'finish', 'processing_total_num': 0}
        self.task(status='adding', site_batch_id='current')
        self.assertFalse(update('kb', 'site', 'old', reset, only_unclaimed=True))
        self.assertFalse(take('kb', 'site', 'old', 'worker-1', ('adding',)))
        self.pipe.multi.assert_not_called()
        self.assertTrue(update('kb', 'site', 'current', reset, only_unclaimed=True))
        self.pipe.hset.assert_called_once_with(self.info_key, mapping=reset)
        self.task(status='finish', site_batch_id='current', processing_total_num='0')
        self.assertFalse(take('kb', 'site', 'current', 'worker-1', ('adding',)))
        self.assertTrue(update('kb', 'site', 'current', reset, only_unclaimed=True), 'A committed reset is idempotent')

        self.pipe.reset_mock()
        self.task(status='adding')
        self.assertTrue(take('kb', 'site', 'current', 'worker-1', ('adding',)), 'Legacy queued jobs can still start')
        self.task(status='adding', site_batch_id='current', site_batch_owner='worker-1')
        self.assertFalse(update('kb', 'site', 'current', reset, only_unclaimed=True))
        self.assertFalse(take('kb', 'site', 'current', 'worker-2', ('adding',)))
        self.assertTrue(take('kb', 'site', 'current', 'worker-1', ('adding',)), 'Recover after EXEC acknowledgment is lost')

    def test_new_batch_rejects_late_previous_delivery(self):
        take = self.functions['take_site_batch']
        update = self.functions['update_site_batch']
        next_batch = {'site_batch_id': 'next', 'site_batch_owner': ''}
        self.task(status='adding', site_batch_id='first', site_batch_owner='worker-1')
        self.assertTrue(update('kb', 'site', 'first', next_batch))
        self.task(status='adding', site_batch_id='next', site_batch_owner='')
        self.assertTrue(update('kb', 'site', 'first', next_batch), 'Recover after staging acknowledgment is lost')
        self.assertFalse(take('kb', 'site', 'first', 'worker-2', ('adding',)))
        self.assertTrue(take('kb', 'site', 'next', 'worker-2', ('adding',)))

    def test_dispatch_failure_keeps_prior_page_counts(self):
        fields = {'status': 'error', 'reason': 'site_queue_failed'}
        self.task(status='adding', site_batch_id='next', site_batch_owner='worker', processing_success_num='3')
        update = self.functions['update_site_batch']
        self.assertFalse(update('kb', 'site', 'next', fields, only_unclaimed=True))
        self.task(status='adding', site_batch_id='next', processing_success_num='3')
        self.assertTrue(update('kb', 'site', 'next', fields, only_unclaimed=True))
        self.pipe.hset.assert_called_once_with(self.info_key, mapping=fields)

    def test_page_publish_failure_and_worker_admission_are_exclusive(self):
        take = self.functions['take_pending_site_doc']
        doc_key = self.functions['get_embedding_doc_info_key']('kb', 'page')
        self.pipe.hmget.return_value = [b'processing', None, b'worker']
        self.assertFalse(take('kb', 'page', failed_task_id='site'))
        self.pipe.hincrby.assert_not_called()

        self.pipe.hmget.return_value = [b'wait', None, None]
        self.pipe.hget.return_value = b'adding'
        self.assertTrue(take('kb', 'page', failed_task_id='site'))
        self.pipe.hset.assert_called_once_with(doc_key, mapping={
            'status': 'error', 'reason': 'site_queue_failed', 'site_page_owner': 'abandoned'})
        self.pipe.hincrby.assert_called_once_with(self.info_key, 'processing_fail_num', 1)
        self.pipe.hmget.return_value = [b'wait', b'download_error', b'abandoned']
        self.assertFalse(take('kb', 'page'), 'Late original jobs cannot claim a page queued for regeneration')

    def test_page_claim_recovers_after_exec_acknowledgment_is_lost(self):
        def committed():
            owner = self.pipe.hset.call_args.kwargs['mapping']['site_page_owner']
            self.pipe.hmget.return_value = [b'processing', None, owner.encode()]
            raise WatchError('connection interrupted after EXEC')

        self.pipe.hmget.return_value = [b'wait', None, None]
        self.pipe.execute.side_effect = committed
        take = self.functions['take_pending_site_doc']
        self.assertTrue(take('kb', 'page'))
        self.assertFalse(take('kb', 'page'), 'A second delivery has a different owner')
        self.pipe.execute.assert_called_once_with()

    def test_queue_failure_returns_failure_to_connector_api(self):
        embedding = mock.Mock()
        embedding.get_embedding_name_info.return_value = {'embedding_uuid': 'kb'}
        queue = mock.Mock()
        service = {
            'lanying_embedding': embedding, 'continue_site_task': queue, 'uuid': uuid, 'logging': logging,
            'lanying_config': mock.Mock(get_lanying_connector=lambda app: {'access_token': 'test-token'}),
        }
        load_functions('services/openai_service.py', {'continue_embedding_task'}, service)
        route = {
            'app': mock.Mock(make_response=lambda body: body), 'accessToken': 'test-token',
            'request': mock.Mock(headers={'access-token': 'test-token'},
                                 get_data=lambda **kwargs: json.dumps(dict(app_id='app', embedding_name='kb', task_id='site'))),
            'get_service_module': lambda name: mock.Mock(continue_embedding_task=service['continue_embedding_task']),
            'logging': logging, 'json': json,
        }
        load_functions('lanying_connector.py', {'continue_embedding_task'}, route)
        call = route['continue_embedding_task']
        embedding.claim_site_index.return_value = 'empty_crawl'
        self.assertEqual(call('openai')['code'], 409)
        queue.apply_async.assert_not_called()

        embedding.claim_site_index.return_value = None
        embedding.update_site_batch.return_value = True
        queue.apply_async.side_effect = RuntimeError('queue unavailable')
        with self.assertLogs(level='ERROR'):
            self.assertEqual(call('openai')['code'], 503)
        trace_id = queue.apply_async.call_args.kwargs['args'][0]
        self.assertEqual(queue.apply_async.call_args.kwargs['task_id'], trace_id)
        embedding.update_site_batch.assert_called_once_with('kb', 'site', trace_id,
            {'status': 'finish', 'processing_total_num': 0}, only_unclaimed=True)
        embedding.update_site_batch.return_value = False
        with self.assertLogs(level='ERROR'):
            self.assertEqual(call('openai')['code'], 200, 'A worker can have accepted the job before publish raised')
        queue.apply_async.side_effect = None
        self.assertEqual(call('openai')['code'], 200)

    def test_initial_crawl_publish_failure_records_a_discardable_error(self):
        embedding = mock.Mock()
        embedding.get_embedding_name_info.return_value = {'embedding_uuid': 'kb'}
        embedding.create_trace_id.return_value = 'trace'
        embedding.create_task.return_value = 'site'
        queue = mock.Mock()
        queue.apply_async.side_effect = RuntimeError('queue unavailable')
        config = {'lanying_user_id': 1, 'lanying_admin_token': 'test', 'access_token': 'test'}
        ns = dict(lanying_embedding=embedding, lanying_url_loader=mock.Mock(), prepare_site=queue,
                  lanying_chatbot=mock.Mock(is_chatbot_mode=lambda app: False), json=json,
                  lanying_config=mock.Mock(get_lanying_connector=lambda app: config))
        load_functions('services/openai_service.py', {'add_doc_to_embedding'}, ns)
        with self.assertRaisesRegex(RuntimeError, 'queue unavailable'):
            ns['add_doc_to_embedding']('app', 'knowledge', '', '', 'site', 100, 10, [], ['https://example.test'], False, {})
        embedding.update_site_batch.assert_called_once_with('kb', 'site', 'trace',
            {'status': 'error', 'reason': 'queue_failed'}, only_unclaimed=True)


class CrawlIndexWorkerTest(unittest.TestCase):
    def setUp(self):
        self.task = dict(status='adding', url='https://example.test', tags={'_seenical_onboarding_crawl': 'request'},
                         processing_total_num=1, processing_success_num=0, processing_fail_num=0, site_batch_id='trace')
        self.embedding = mock.Mock()
        self.embedding.get_embedding_uuid_info.return_value = {'vendor': 'openai', 'embedding_name': 'kb'}
        self.embedding.take_site_batch.return_value = True
        self.embedding.update_site_batch.return_value = True
        self.embedding.get_task.return_value = self.task
        self.embedding.get_task_details_iterator.return_value = iter([('https://example.test', {})])
        self.embedding.increase_task_field.side_effect = lambda kb, task_id, key, amount: self.task.update({key: self.task[key] + amount})
        self.embedding.update_task_field.side_effect = lambda kb, task_id, key, value: self.task.update({key: value})
        self.storage = mock.Mock()
        self.storage.download_url.return_value = self.storage.upload.return_value = {'result': 'ok'}
        self.page_queue = mock.Mock()
        self.ns = dict(lanying_embedding=self.embedding, lanying_file_storage=self.storage, logging=logging,
                       uuid=uuid, os=SimpleNamespace(path=os.path, stat=lambda path: SimpleNamespace(st_size=10)),
                       embedding_doc_dir='docs', download_dir='/tmp', process_embedding_file_slow=self.page_queue,
                       RedisConnectionError=RedisConnectionError, RedisTimeoutError=RedisTimeoutError, WatchError=WatchError)
        load_functions('lanying_tasks.py', {'continue_site_task', 'maybe_task_finish', 'queue_next_site_batch'}, self.ns)
        self.worker = self.ns['continue_site_task']
        self.worker.apply_async = mock.Mock()
        self.celery = mock.Mock(request=SimpleNamespace(retries=0, id='trace'), max_retries=3)

    def run_worker(self):
        self.worker(self.celery, 'trace', 'app', 'kb', 'crawl')

    def test_last_download_or_upload_failure_finishes_task(self):
        for operation in (self.storage.download_url, self.storage.upload):
            with self.subTest(operation=operation):
                self.task.update(status='adding', processing_fail_num=0)
                self.embedding.get_task_details_iterator.return_value = iter([('https://example.test', {})])
                operation.return_value = {'result': 'error', 'message': 'unavailable'}
                with self.assertLogs(level='ERROR'):
                    self.run_worker()
                self.assertEqual(self.task['processing_fail_num'], 1)
                self.assertEqual(self.task['status'], 'add_finish')
                operation.return_value = {'result': 'ok'}

    def test_failed_page_publish_is_accounted_and_excludes_onboarding_tag(self):
        self.page_queue.apply_async.side_effect = RuntimeError('broker unavailable')
        self.embedding.take_pending_site_doc.side_effect = lambda *args, **kwargs: self.task.update(processing_fail_num=1)
        with self.assertLogs(level='ERROR'):
            self.run_worker()
        self.embedding.take_pending_site_doc.assert_called_once_with('kb', mock.ANY, failed_task_id='crawl')
        self.assertEqual(self.embedding.create_doc_info.call_args.args[-1]['tags'], {})
        self.assertEqual(self.task['status'], 'add_finish')

    def test_failed_batch_publish_stops_without_republishing_existing_pages(self):
        self.task['processing_total_num'] = 21
        urls = [f'https://example.test/{i}' for i in range(21)]
        self.embedding.get_task_details_iterator.return_value = iter((url, {}) for url in urls)
        self.worker.apply_async.side_effect = RuntimeError('broker unavailable')
        with self.assertRaisesRegex(RuntimeError, 'broker unavailable'):
            self.run_worker()
        self.assertEqual(self.page_queue.apply_async.call_count, 20)
        self.embedding.delete_task_details_by_fields.assert_called_once_with('kb', 'crawl', urls[:20])
        next_batch = self.worker.apply_async.call_args.kwargs['task_id']
        self.assertNotEqual(next_batch, 'trace')
        self.assertEqual(self.embedding.update_site_batch.call_args_list, [
            mock.call('kb', 'crawl', 'trace', {'site_batch_id': next_batch, 'site_batch_owner': ''}),
            mock.call('kb', 'crawl', next_batch, {'status': 'error', 'reason': 'site_queue_failed'}, only_unclaimed=True),
        ])

    def test_followup_crawl_failure_checks_next_batch_ownership(self):
        publish = self.ns['queue_next_site_batch']
        job = mock.Mock()
        job.apply_async.side_effect = RuntimeError('broker unavailable')
        self.embedding.update_site_batch.side_effect = [True, False]
        publish(job, ['args'], 'kb', 'crawl', 'previous', 'queue_failed')
        batch_id = job.apply_async.call_args.kwargs['task_id']
        self.assertEqual(self.embedding.update_site_batch.call_args_list, [
            mock.call('kb', 'crawl', 'previous', {'site_batch_id': batch_id, 'site_batch_owner': ''}),
            mock.call('kb', 'crawl', batch_id, {'status': 'error', 'reason': 'queue_failed'}, only_unclaimed=True),
        ])

    def test_legacy_job_keeps_existing_queue_format_during_worker_rollout(self):
        self.task.pop('site_batch_id')
        self.task['processing_total_num'] = 21
        self.embedding.get_task_details_iterator.return_value = iter((f'https://example.test/{i}', {}) for i in range(21))
        self.run_worker()
        self.embedding.take_site_batch.assert_not_called()
        self.embedding.update_site_batch.assert_not_called()
        self.worker.apply_async.assert_called_once_with(args=['trace', 'app', 'kb', 'crawl'])

    def test_transient_redis_error_retries_before_any_page_side_effect(self):
        error = WatchError('connection interrupted')
        self.embedding.take_site_batch.side_effect = error
        self.celery.retry.return_value = RuntimeError('retry scheduled')
        with self.assertRaisesRegex(RuntimeError, 'retry scheduled'):
            self.run_worker()
        self.celery.retry.assert_called_once_with(exc=error, countdown=5)
        self.storage.download_url.assert_not_called()


if __name__ == '__main__':
    unittest.main()
