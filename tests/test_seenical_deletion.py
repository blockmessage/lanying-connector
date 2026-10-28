"""Deletion authorization, durable acceptance and retry boundaries (no external I/O).

SQLite exercises transactions/data decisions. Named-lock ownership is tested
separately with a connection double; real MySQL/IM/preview callbacks need staging.
"""
import contextlib
import types
import unittest
from unittest import mock
from sqlalchemy import create_engine, event, text

import lanying_seenical_deletion as deletion
import lanying_agent_tools_storage as storage


class DeletionTest(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.engine = create_engine('sqlite://')
        self.addCleanup(self.engine.dispose)
        @event.listens_for(self.engine, 'before_cursor_execute', retval=True)
        def dialect(conn, cursor, statement, parameters, context, executemany):
            return statement.replace(' FOR UPDATE', '').replace('CURRENT_TIMESTAMP(3)', 'CURRENT_TIMESTAMP'), parameters
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE seenical_material_space (app_id TEXT PRIMARY KEY)'))
            conn.execute(text('''CREATE TABLE seenical_conversation_binding (
                app_id TEXT, seenical_session_id TEXT, chatbot_id TEXT, agent_user_id TEXT,
                conversation_type TEXT, conversation_id TEXT, conversation_name TEXT,
                task_id TEXT, bound_im_user_id TEXT, status TEXT, revision INTEGER,
                deletion_task_id TEXT, deletion_error TEXT DEFAULT '', updated_at TEXT,
                PRIMARY KEY(app_id,seenical_session_id), UNIQUE(app_id,deletion_task_id))'''))
            conn.execute(text("""INSERT INTO seenical_conversation_binding
                (app_id,seenical_session_id,chatbot_id,agent_user_id,conversation_type,
                conversation_id,conversation_name,task_id,bound_im_user_id,status,revision)
                VALUES ('a','s','b','agent','GROUPCHAT','123','Child','p','u','ACTIVE',1)"""))
        self.stack.enter_context(mock.patch.object(storage, '_get_engine', return_value=self.engine))
        self.locked = set()
        @contextlib.contextmanager
        def lock(app, kind, identity):
            yield (app, kind, identity) not in self.locked
        self.stack.enter_context(mock.patch.object(deletion, 'operation_lock', side_effect=lock))
        self.tools = types.SimpleNamespace(get_im_binding_projection=mock.Mock(
            return_value={'status': 'BOUND', 'im_user_id': 'u'}))
        self.queue = types.SimpleNamespace(seenical_conversation_cleanup=mock.Mock())
        self.run = {'task_run_id': 'r', 'task_id': 'p', 'status': 'running'}
        self.grow = types.SimpleNamespace(
            get_task=mock.Mock(return_value={'task_id': 'p', 'schedule': 'on'}),
            get_task_run=mock.Mock(side_effect=lambda a, r: self.run),
            get_task_run_list=mock.Mock(side_effect=lambda a, p: {'data': {'list': [self.run] if self.run else []}}),
            set_task_schedule=mock.Mock(), update_task_run_field=mock.Mock(),
            get_site_list=mock.Mock(return_value={'data': {'list': []}}), preview_pr_set_key=lambda: 'prs',
            get_preview=mock.Mock(return_value=None), preview_discard=mock.Mock(return_value={'result': 'ok'}),
            delete_task_run=mock.Mock(side_effect=self.delete_run),
            delete_task=mock.Mock(return_value={'result': 'ok'}),
            get_task_run_result_key=lambda a, r: 'result:' + r)
        self.materials = types.SimpleNamespace(release_owner=mock.Mock())
        self.im = types.SimpleNamespace(destroy_seenical_group=mock.Mock(return_value=True))
        self.redis = mock.Mock()
        self.redis.smembers.return_value = []
        self.stack.enter_context(mock.patch.dict('sys.modules', {
            'lanying_agent_tools': self.tools, 'lanying_tasks': self.queue,
            'lanying_grow_ai': self.grow, 'lanying_seenical_materials': self.materials,
            'lanying_im_api': self.im,
            'lanying_redis': types.SimpleNamespace(get_redis_connection=lambda: self.redis)}))
        self.body = {'seenical_session_id': 's', 'conversation_id': '123', 'task_ids': ['p']}

    def delete_run(self, app, run):
        self.run = None
        return {'result': 'ok'}

    def request(self, body=None, app='a', user='u'):
        return deletion.request(app, {'im_user_id': user}, body or self.body)

    def row(self):
        with self.engine.connect() as conn:
            return dict(conn.execute(text('SELECT * FROM seenical_conversation_binding')).mappings().one())

    def test_acceptance_is_durable_before_queue_and_idempotent_after_lost_reply(self):
        def lost(*args):
            self.assertEqual(self.row()['status'], 'DELETING')
            raise TimeoutError()
        self.queue.seenical_conversation_cleanup.delay.side_effect = lost
        self.assertTrue(self.request()['data']['accepted'])
        self.assertEqual(self.request()['result'], 'ok')
        self.assertEqual(self.row()['revision'], 2)
        self.assertEqual(deletion.deleting_task_ids('a'), {'p'})
        self.assertEqual(deletion.deleting_task_ids('other'), set())

    def test_authorization_scope_and_changed_loop_are_rejected(self):
        for arguments in ({'app': 'other'}, {'user': 'other'},
                          {'body': dict(self.body, conversation_id='456')},
                          {'body': dict(self.body, task_ids=['other'])},
                          {'body': dict(self.body, task_ids=[])}):
            self.assertEqual(self.request(**arguments)['result'], 'error')
        self.assertEqual(self.row()['status'], 'ACTIVE')
        self.queue.seenical_conversation_cleanup.delay.assert_not_called()

    def test_primary_chat_cannot_be_deleted(self):
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE seenical_conversation_binding SET conversation_type='CHAT'"))
        self.assertEqual(self.request()['result'], 'error')

    def test_active_worker_does_not_block_acceptance_or_group_removal(self):
        self.locked.add(('a', 'run', 'r'))
        self.assertEqual(self.request()['result'], 'ok')
        deletion.cleanup('a', 's')
        self.im.destroy_seenical_group.assert_called_once_with('a', '123')
        self.grow.delete_task_run.assert_not_called()
        self.materials.release_owner.assert_any_call('a', 'session', 's')
        self.materials.release_owner.assert_any_call('a', 'plan', 'p')
        self.assertFalse(any(c.args[1] == 'run' for c in self.materials.release_owner.call_args_list))
        self.assertEqual(self.row()['status'], 'DELETING')
        self.assertEqual(self.row()['deletion_error'], 'waiting_for_run_or_preview')
        self.locked.clear()
        deletion.cleanup('a', 's')
        self.assertEqual(self.row()['status'], 'DELETED')
        self.materials.release_owner.assert_any_call('a', 'session', 's')
        self.assertEqual(deletion.deleting_task_ids('a'), {'p'}, 'Keep tombstone after deleting task')
        deletion.cleanup('a', 's')
        self.grow.delete_task_run.assert_called_once()

    def test_failed_group_cleanup_retries_without_losing_job(self):
        self.request()
        self.im.destroy_seenical_group.return_value = False
        deletion.cleanup('a', 's')
        self.assertEqual(self.row()['status'], 'DELETING')
        self.grow.delete_task.assert_not_called()
        self.im.destroy_seenical_group.return_value = True
        deletion.cleanup('a', 's')
        self.assertEqual(self.row()['status'], 'DELETED')

    def test_preview_waits_for_existing_cleanup_callback(self):
        self.request()
        self.run['preview_id'] = 'preview'
        self.grow.get_preview.return_value = {'status': 'clearing', 'task_run_id': 'r'}
        deletion.cleanup('a', 's')
        self.grow.preview_discard.assert_called_once_with('a', 'preview')
        self.grow.delete_task_run.assert_not_called()
        self.grow.get_preview.return_value = None
        deletion.cleanup('a', 's')
        self.assertEqual(self.row()['status'], 'DELETED')

    def test_previous_active_preview_is_cleaned_but_other_run_is_untouched(self):
        self.request()
        self.run['preview_id'] = 'latest'
        self.grow.get_site_list.return_value = {'data': {'list': [
            {'active_preview_id': 'earlier', 'pending_preview_id': 'unrelated'}]}}
        self.redis.smembers.return_value = [b'a:pr', b'other:private']
        previews = {key: {'task_run_id': 'r'} for key in ('latest', 'earlier', 'pr')}
        previews['unrelated'] = {'task_run_id': 'other-run'}
        self.grow.get_preview.side_effect = lambda a, p: previews.get(p)
        def discard(app, preview):
            previews.pop(preview)
            return {'result': 'ok'}
        self.grow.preview_discard.side_effect = discard
        deletion.cleanup('a', 's')
        self.assertEqual(set(previews), {'unrelated'})
        self.assertEqual(self.row()['status'], 'DELETED')

    def test_missing_plan_still_releases_its_references(self):
        self.request()
        self.grow.get_task.return_value = None
        deletion.cleanup('a', 's')
        self.materials.release_owner.assert_any_call('a', 'plan', 'p')
        self.assertEqual(self.row()['status'], 'DELETED')

    def test_stale_run_delivery_and_retry_cannot_write_after_acceptance(self):
        work = mock.Mock()
        guarded = deletion.guarded('run')(work)
        self.request()
        self.assertFalse(guarded('a', 'r')['retry'])
        work.assert_not_called()
        deletion.cleanup('a', 's')
        self.assertFalse(guarded('a', 'r')['retry'])
        work.assert_not_called()

    def test_cancel_between_model_calls_is_terminal(self):
        @deletion.guarded('run')
        def work(app, run):
            self.request()  # Simulate acceptance while the model call is in flight.
            deletion.check_run(app, run)
            self.fail('Must not persist/publish a cancelled result')
        self.assertEqual(work('a', 'r')['message'], 'conversation_deleting')

    def test_cached_group_and_task_identity_remain_available_to_new_clients(self):
        self.request()
        deletion.cleanup('a', 's')
        self.assertEqual(deletion.tombstones('a', 'u'), [{
            'seenical_session_id': 's', 'conversation_id': '123', 'task_id': 'p', 'status': 'DELETED'}])
        self.assertEqual(deletion.tombstones('a', 'other'), [])

    def test_register_and_old_unregister_cannot_remove_tombstone(self):
        self.request()
        binding = self.row()
        self.assertEqual(storage.save_seenical_conversation_binding(binding)['message'], 'conversation_deleting')
        deletion.cleanup('a', 's')
        storage.deactivate_seenical_conversation_binding('a', 's', 'GROUPCHAT', '123', 'u')
        self.assertEqual(self.row()['status'], 'DELETED')

    def test_generation_cancellation_is_not_converted_to_celery_retry(self):
        from test_grow_ai_patch import load_grow_ai
        grow = load_grow_ai()
        grow.conversation_deletion = deletion
        grow.get_task_run = self.grow.get_task_run
        grow.update_task_run_field = mock.Mock()
        grow.increase_task_run_field = mock.Mock()
        grow.send_task_run_notification = mock.Mock()
        grow.do_run_task_internal = mock.Mock(side_effect=deletion.Cancelled())
        with mock.patch.dict('sys.modules', {'lanying_grow_ai': grow}):
            result = deletion.guarded('run')(grow.do_run_task)('a', 'r', True)
        self.assertFalse(result['retry'])
        grow.update_task_run_field.assert_called_once_with('a', 'r', 'status', 'running')
        grow.increase_task_run_field.assert_not_called()

    def test_delayed_release_and_deployment_check_do_not_publish_deleted_loop(self):
        from test_grow_ai_patch import load_grow_ai
        grow = load_grow_ai()
        grow.conversation_deletion = deletion
        grow.get_github_site_id_list = mock.Mock(return_value={'site': 'a'})
        grow.get_site = mock.Mock(return_value={'github_url': 'url', 'update_time': 1})
        grow.parse_github_url = mock.Mock(return_value={'result': 'ok', 'github_owner': 'owner', 'github_repo': 'repo'})
        grow.resolve_release_commit = mock.Mock(return_value='sha')
        grow.start_deploy_github_action = mock.Mock()
        grow.lanying_redis.redis_get = mock.Mock(return_value='p')
        grow.get_deploy_code = mock.Mock(return_value={'app_id': 'a', 'task_id': 'p', 'site_id': 'site'})
        self.request()
        self.assertEqual(grow.release_finish('owner/repo', 'release')['message'], 'conversation_deleting')
        self.assertEqual(grow.check_deploy('code', 10)['message'], 'conversation_deleting')
        grow.start_deploy_github_action.assert_not_called()


class PreviewDeletionRegressionTest(unittest.TestCase):
    def setUp(self):
        from test_grow_ai_patch import load_grow_ai
        self.grow = load_grow_ai()
        self.preview = {'app_id': 'a', 'preview_id': 'preview', 'site_id': 'site',
                        'task_run_id': 'run', 'branch_name': 'preview-branch',
                        'preview_commit_sha': 'original', 'status': 'ready'}
        self.run = {'task_run_id': 'run', 'task_id': 'plan', 'preview_id': 'preview'}
        self.site = {'active_preview_id': 'preview', 'pending_preview_id': '',
                     'site_name': 'site', 'github_url': 'https://github.com/org/repo',
                     'update_time': 1}
        self.context = {'result': 'ok', 'site': self.site, 'repository': 'org/repo',
                        'api_url': 'https://api.github.com/repos/org/repo', 'headers': {}}
        self.values = {}
        self.redis = mock.MagicMock()
        self.redis.set.side_effect = self.values.__setitem__
        self.redis.delete.side_effect = lambda key: self.values.pop(key, None)
        self.redis.hset.side_effect = lambda key, mapping: self.preview.update(mapping)
        self.grow.lanying_redis.get_redis_connection = lambda: self.redis
        self.grow.lanying_redis.redis_get = lambda client, key: self.values.get(key)
        self.grow.get_preview = lambda app, key: self.preview
        self.grow.get_task_run = lambda app, key: self.run
        self.grow.get_site = lambda app, key: self.site
        self.grow.get_preview_github_context = lambda preview: self.context
        self.grow.update_preview_field = lambda app, key, field, value: self.preview.update({field: value})
        self.grow.update_site_field = lambda app, key, field, value: self.site.update({field: value})
        self.grow.update_task_run_field = lambda app, key, field, value: self.run.update({field: value})
        self.grow.requests = types.SimpleNamespace(
            get=mock.Mock(return_value=mock.Mock(status_code=404)),
            post=mock.Mock(return_value=mock.Mock(status_code=204)))
        self.grow.maybe_add_site_url = mock.Mock()
        self.grow.set_preview_callback = mock.Mock()
        self.grow.delete_preview_callback = mock.Mock()
        self.grow.get_grow_ai_workflow_headers = lambda: {}
        self.grow.lanying_utils.get_internet_connector_server = lambda: 'https://connector.example'
        self.grow.consume_preview_callback = lambda code: {
            'type': 'clear', 'app_id': 'a', 'preview_id': 'preview',
            'attempt': self.preview.get('clear_attempt', '')}

    def assert_deleted_release_blocked(self):
        self.grow.conversation_deletion = types.SimpleNamespace(
            Cancelled=deletion.Cancelled, deleting_task_ids=lambda app: {'plan'})
        self.grow.get_github_site_id_list = lambda owner, repo: {'site': 'a'}
        self.grow.parse_github_url = lambda url: {
            'result': 'ok', 'github_owner': 'org', 'github_repo': 'repo'}
        self.grow.resolve_release_commit = lambda *args: 'merged'
        self.grow.start_deploy_github_action = mock.Mock()
        self.run = None  # The cleanup has removed the run as well as preview metadata.
        result = self.grow.release_finish('org/repo', 'release')
        self.assertEqual(result['message'], 'conversation_deleting')
        self.grow.start_deploy_github_action.assert_not_called()

    def test_cleanup_records_merge_even_before_reconcile_and_blocks_late_release(self):
        self.preview.update(pr_number=1, status='pr_open')
        self.grow.requests.get.side_effect = [
            mock.Mock(status_code=200, json=lambda: {'merged': True, 'merge_commit_sha': 'merged'}),
            mock.Mock(status_code=404)]
        self.assertTrue(self.grow.cleanup_preview_without_site('a', 'preview'))
        self.assertEqual(self.values['lanying_connector:grow_ai:commit_task:a:merged'], 'plan')
        self.assert_deleted_release_blocked()

    def test_reconciled_merge_survives_removal_of_temporary_mapping(self):
        self.preview.update(pr_number=1, status='pr_open')
        self.redis.smembers.return_value = ['a:preview']
        self.grow.requests.get.return_value = mock.Mock(
            status_code=200, json=lambda: {'merged': True, 'merge_commit_sha': 'merged'})
        self.grow.get_github_commit_tree_sha = lambda *args: 'same-tree'
        self.grow.reconcile_preview_pull_requests()
        self.grow.requests.get.side_effect = [
            mock.Mock(status_code=200, json=lambda: {'merged': True, 'merge_commit_sha': 'merged'}),
            mock.Mock(status_code=404)]
        self.assertTrue(self.grow.cleanup_preview_without_site('a', 'preview'))
        self.assertNotIn(self.grow.preview_publish_commit_key('org/repo', 'merged'), self.values)
        self.assert_deleted_release_blocked()

    def test_failed_pr_lookup_keeps_metadata_for_retry(self):
        self.preview['pr_number'] = 1
        self.grow.requests.get.return_value = mock.Mock(status_code=503)
        self.assertFalse(self.grow.cleanup_preview_without_site('a', 'preview'))
        self.redis.delete.assert_not_called()

    def test_repeated_discard_waits_for_clear_callback(self):
        for _ in range(3):
            self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'ok')
        self.assertEqual(self.grow.requests.post.call_count, 1)
        self.assertEqual(self.preview['clear_status'], 'pending')

    def test_pending_clear_keeps_nonactive_preview_until_callback(self):
        self.grow.preview_discard('a', 'preview')
        self.site['active_preview_id'] = ''
        self.grow.preview_discard('a', 'preview')
        self.redis.delete.assert_not_called()
        self.assertEqual(self.grow.requests.post.call_count, 1)

    def test_failed_clear_can_be_retried(self):
        self.grow.preview_discard('a', 'preview')
        self.grow.preview_clear_finish('callback', 'failed')
        self.grow.preview_discard('a', 'preview')
        self.assertEqual(self.grow.requests.post.call_count, 2)
        self.assertEqual(self.preview['clear_status'], 'pending')

    def test_detached_failed_clear_retries_and_finishes(self):
        self.grow.preview_discard('a', 'preview')
        self.site.update(active_preview_id='', pending_preview_id='')
        self.grow.preview_clear_finish('callback', 'failed')
        self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'ok')
        self.assertEqual(self.grow.requests.post.call_count, 2)
        self.assertEqual(self.preview['clear_status'], 'pending')
        self.assertEqual(self.grow.preview_clear_finish('callback', 'ok')['result'], 'ok')
        self.redis.delete.assert_called_with(self.grow.preview_key('a', 'preview'))

    def test_failed_clear_preserves_replacement_preview(self):
        self.preview.update(clear_status='failed', clear_attempt='old', status='error')
        for field in ('active_preview_id', 'pending_preview_id'):
            with self.subTest(field=field):
                self.site.update(active_preview_id='', pending_preview_id='')
                self.site[field] = 'new-preview'
                self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'error')
                self.grow.requests.post.assert_not_called()
                self.redis.delete.assert_not_called()
                self.assertEqual(self.site[field], 'new-preview')

    def test_failed_clear_supports_manual_recovery_and_recovery_after_failure(self):
        self.preview.update(clear_status='failed', clear_attempt='old', status='error')
        self.site.update(active_preview_id='new-preview', pending_preview_id='')
        self.assertEqual(self.grow.recover_preview_clear(
            'a', 'preview', 'old', 'failed', workflow_stopped=True)['result'], 'ok')
        attempt = self.preview['clear_attempt']
        self.assertTrue(attempt)
        self.assertNotEqual(attempt, 'old')
        self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'error')
        self.grow.consume_preview_callback = lambda code: {
            'type': 'clear', 'app_id': 'a', 'preview_id': 'preview', 'attempt': 'old'}
        self.grow.preview_clear_finish('late', 'ok')
        self.assertEqual(self.preview['clear_status'], 'failed')
        self.assertEqual(self.grow.recover_preview_clear(
            'a', 'preview', attempt, 'complete', workflow_stopped=True)['result'], 'ok')
        self.assertEqual(self.site['active_preview_id'], 'new-preview')
        self.grow.requests.post.assert_not_called()
        self.redis.delete.assert_called_with(self.grow.preview_key('a', 'preview'))

    def test_discard_reports_metadata_cleanup_failure(self):
        self.site['active_preview_id'] = ''
        self.grow.cleanup_preview_without_site = mock.Mock(return_value=False)
        result = self.grow.preview_discard('a', 'preview')
        self.assertEqual(result['result'], 'error')
        self.assertFalse(result['data']['success'])

    def test_successful_clear_retries_only_metadata_cleanup(self):
        self.grow.preview_discard('a', 'preview')
        self.grow.cleanup_preview_without_site = mock.Mock(side_effect=[False, True])
        self.assertEqual(self.grow.preview_clear_finish('callback', 'ok')['result'], 'error')
        self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'ok')
        self.assertEqual(self.grow.requests.post.call_count, 1)
        self.assertEqual(self.grow.cleanup_preview_without_site.call_count, 2)

    def test_ordinary_merged_pr_resolves_after_run_deletion(self):
        self.values[self.grow.pull_request_task_key('a', 'org/repo', 7)] = 'plan'
        self.grow.requests.get.return_value = mock.Mock(status_code=200, links={}, json=lambda: [{
            'number': 7, 'merged_at': '2026-10-28', 'merge_commit_sha': 'merged',
            'head': {'sha': 'different-head'}}])
        self.assert_deleted_release_blocked()
        self.assertEqual(self.values['lanying_connector:grow_ai:commit_task:a:merged'], 'plan')

    def test_ordinary_pr_lost_creation_response_resolves_from_head(self):
        self.values['lanying_connector:grow_ai:commit_task:a:original'] = 'plan'
        self.grow.requests.get.return_value = mock.Mock(status_code=200, links={}, json=lambda: [{
            'number': 7, 'merged_at': '2026-10-28', 'merge_commit_sha': 'merged',
            'head': {'sha': 'original'}}])
        self.assert_deleted_release_blocked()

    def test_release_lookup_failure_does_not_dispatch(self):
        self.grow.get_github_site_id_list = lambda *args: {'site': 'a'}
        self.grow.parse_github_url = lambda url: {
            'result': 'ok', 'github_owner': 'org', 'github_repo': 'repo'}
        self.grow.resolve_release_commit = lambda *args: 'merged'
        self.grow.start_deploy_github_action = mock.Mock()
        self.grow.requests.get.return_value = mock.Mock(status_code=503)
        self.assertEqual(self.grow.release_finish('org/repo', 'r')['result'], 'error')
        self.grow.start_deploy_github_action.assert_not_called()

    def test_release_commit_uses_tag_even_when_target_has_moved(self):
        self.grow.get_github_token = lambda: 'test-token'
        for target in ('main', 'b' * 40):
            with self.subTest(target=target):
                self.grow.requests.get.reset_mock()
                self.grow.requests.get.side_effect = [
                    mock.Mock(status_code=200, json=lambda: {
                        'tag_name': 'releases/v1', 'target_commitish': target}),
                    mock.Mock(status_code=200, json=lambda: {'sha': 'a' * 40})]
                self.assertEqual(self.grow.resolve_release_commit('org/repo', 'releases/v1'), 'a' * 40)
                self.assertEqual([c.args[0] for c in self.grow.requests.get.call_args_list], [
                    'https://api.github.com/repos/org/repo/releases/tags/releases%2Fv1',
                    'https://api.github.com/repos/org/repo/commits/tags%2Freleases%2Fv1'])

    def test_unresolvable_release_tag_never_falls_back_to_branch(self):
        self.grow.get_github_token = lambda: 'test-token'
        for release_info, tag_status, expected_calls in (
                ({'target_commitish': 'main'}, 200, 1),
                ({'tag_name': 'v1', 'target_commitish': 'main'}, 404, 2)):
            with self.subTest(release_info=release_info):
                self.grow.requests.get.reset_mock()
                self.grow.requests.get.side_effect = [
                    mock.Mock(status_code=200, json=lambda: release_info),
                    mock.Mock(status_code=tag_status)]
                self.assertEqual(self.grow.resolve_release_commit('org/repo', 'v1'), '')
                self.assertEqual(self.grow.requests.get.call_count, expected_calls)

    def test_unrelated_pr_is_not_attributed_to_deleted_plan(self):
        self.values[self.grow.pull_request_task_key('a', 'org/repo', 7)] = 'plan'
        self.grow.requests.get.return_value = mock.Mock(status_code=200, links={}, json=lambda: [{
            'number': 7, 'merged_at': '2026-10-28', 'merge_commit_sha': 'other',
            'head': {'sha': 'original'}}])
        result = self.grow.resolve_release_task('a', 'org/repo', 'manual-commit', self.site)
        self.assertEqual(result, {'result': 'ok', 'task_id': ''})

    def test_timeout_requires_manual_recovery_instead_of_repeated_dispatch(self):
        self.grow.preview_discard('a', 'preview')
        attempt = self.preview['clear_attempt']
        self.preview['clear_started_at'] = 0
        self.assertEqual(self.grow.preview_discard('a', 'preview')['result'], 'error')
        self.assertEqual(self.preview['clear_status'], 'unknown')
        self.grow.preview_discard('a', 'preview')
        self.assertEqual(self.grow.requests.post.call_count, 1)
        self.assertEqual(self.grow.recover_preview_clear('a', 'preview', attempt, 'failed')['result'], 'error')
        self.assertEqual(self.grow.recover_preview_clear(
            'a', 'preview', 'wrong', 'failed', workflow_stopped=True)['result'], 'error')
        self.assertEqual(self.grow.recover_preview_clear(
            'a', 'preview', attempt, 'failed', workflow_stopped=True)['result'], 'ok')
        self.grow.preview_discard('a', 'preview')
        self.assertEqual(self.grow.requests.post.call_count, 2)
        new_attempt = self.preview['clear_attempt']
        self.grow.consume_preview_callback = lambda code: {
            'type': 'clear', 'app_id': 'a', 'preview_id': 'preview', 'attempt': attempt}
        self.grow.preview_clear_finish('late', 'ok')
        self.assertEqual(self.preview['clear_status'], 'pending')
        self.assertEqual(self.preview['clear_attempt'], new_attempt)

    def test_manual_success_only_cleans_metadata(self):
        self.grow.preview_discard('a', 'preview')
        self.grow.cleanup_preview_without_site = mock.Mock(return_value=True)
        self.assertEqual(self.grow.recover_preview_clear(
            'a', 'preview', self.preview['clear_attempt'], 'complete', workflow_stopped=True)['result'], 'ok')
        self.grow.cleanup_preview_without_site.assert_called_once_with('a', 'preview')
        self.assertEqual(self.grow.requests.post.call_count, 1)

    def test_dispatch_timeout_does_not_allow_automatic_resend(self):
        self.grow.requests.post.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            self.grow.preview_discard('a', 'preview')
        self.grow.preview_discard('a', 'preview')
        self.assertEqual(self.grow.requests.post.call_count, 1)
        self.assertEqual(self.preview['clear_status'], 'pending')

    def test_deploy_callback_with_stale_preview_does_not_resend(self):
        stale = dict(self.preview)
        self.grow.preview_discard('a', 'preview')
        self.grow.dispatch_clear_preview(stale)
        self.assertEqual(self.grow.requests.post.call_count, 1)
        self.preview['clear_started_at'] = 0
        self.grow.preview_discard('a', 'preview')
        self.assertEqual(self.grow.dispatch_clear_preview(stale)['result'], 'error')
        self.assertEqual(self.grow.requests.post.call_count, 1)

    def test_deploy_callback_preserves_unfinished_clear_and_recovery_state(self):
        self.grow.consume_preview_callback = lambda code: {
            'type': 'deploy_finish', 'app_id': 'a', 'preview_id': 'preview'}
        self.grow.restore_active_preview = mock.Mock()
        for clear_status in ('pending', 'unknown', 'failed'):
            for deploy_status in ('ok', 'failed'):
                with self.subTest(clear=clear_status, deploy=deploy_status):
                    self.preview.update(clear_status=clear_status, clear_attempt='attempt',
                                        status='clearing' if clear_status == 'pending' else 'error')
                    before = dict(self.preview)
                    self.assertEqual(self.grow.preview_deploy_finish('code', deploy_status)['result'], 'ok')
                    self.assertEqual(self.preview, before)
                    self.redis.delete.assert_not_called()
                    self.grow.restore_active_preview.assert_not_called()
                    self.grow.requests.post.assert_not_called()

    def test_other_cleanup_callers_preserve_unfinished_clear(self):
        for state in ('pending', 'unknown', 'failed'):
            with self.subTest(state=state):
                self.preview['clear_status'] = state
                self.assertFalse(self.grow.cleanup_preview_without_site('a', 'preview'))
                self.redis.delete.assert_not_called()
                self.grow.requests.get.assert_not_called()
        self.preview['clear_status'] = 'complete'
        self.assertTrue(self.grow.cleanup_preview_without_site('a', 'preview'))
        self.redis.delete.assert_called_with(self.grow.preview_key('a', 'preview'))

    def test_deploy_callback_rechecks_clear_state_after_site_lock(self):
        self.grow.consume_preview_callback = lambda code: {
            'type': 'deploy_finish', 'app_id': 'a', 'preview_id': 'preview'}
        stale = dict(self.preview)
        self.preview.update(status='clearing', clear_status='pending', clear_attempt='attempt')
        self.grow.get_preview = mock.Mock(side_effect=[stale, self.preview])
        self.assertEqual(self.grow.preview_deploy_finish('code', 'ok')['result'], 'ok')
        self.redis.delete.assert_not_called()
        self.assertEqual(self.preview['clear_status'], 'pending')

    def test_normal_deploy_callback_still_activates_preview(self):
        self.grow.consume_preview_callback = lambda code: {
            'type': 'deploy_finish', 'app_id': 'a', 'preview_id': 'preview'}
        self.site.update(active_preview_id='', pending_preview_id='preview')
        self.preview['status'] = 'deploying'
        self.assertEqual(self.grow.preview_deploy_finish('code', 'ok')['result'], 'ok')
        self.assertEqual(self.site['active_preview_id'], 'preview')
        self.assertEqual(self.site['pending_preview_id'], '')
        self.assertEqual(self.preview['status'], 'ready')


class LockTest(unittest.TestCase):
    def test_lock_is_nonblocking_and_released_even_when_work_fails(self):
        engine = mock.Mock()
        conn = mock.MagicMock()
        engine.connect.return_value = conn
        conn.__enter__.return_value = conn
        conn.execute.return_value.scalar.return_value = 1
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            with self.assertRaises(ValueError):
                with deletion.operation_lock('a', 'run', 'r') as acquired:
                    self.assertTrue(acquired)
                    raise ValueError()
        sql = [str(c.args[0]) for c in conn.execute.call_args_list]
        self.assertEqual(sql, ['SELECT GET_LOCK(:name, 0)', 'SELECT RELEASE_LOCK(:name)'])

    def test_failed_release_drops_connection_instead_of_leaking_named_lock(self):
        engine, conn = mock.Mock(), mock.MagicMock()
        engine.connect.return_value = conn
        conn.__enter__.return_value = conn
        result = mock.Mock()
        result.scalar.return_value = 1
        conn.execute.side_effect = [result, ConnectionError()]
        with mock.patch.object(storage, '_get_engine', return_value=engine):
            with self.assertRaises(ConnectionError):
                with deletion.operation_lock('a', 'run', 'r'):
                    pass
        conn.invalidate.assert_called_once()


if __name__ == '__main__':
    unittest.main()
