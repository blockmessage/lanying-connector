import unittest
from unittest.mock import MagicMock, call, patch

from flask import Flask

import lanying_grow_ai
from services import grow_ai_service


class DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class GrowAIPreviewTest(unittest.TestCase):
    def test_auto_deploy_uses_latest_task_setting(self):
        with patch.object(lanying_grow_ai, 'get_task', return_value={
            'app_id': 'app', 'task_id': 'task', 'site_id_list': ['site'],
            'auto_deploy': 'off'
        }) as get_task, patch.object(lanying_grow_ai, 'get_task_site_list', return_value=[{
            'site_id': 'site'
        }]):
            sites = lanying_grow_ai.get_auto_deploy_site_list('app', 'task')

        self.assertEqual([], sites)
        get_task.assert_called_once_with('app', 'task')

    def test_configure_without_auto_deploy_preserves_current_value(self):
        app = Flask(__name__)
        payload = {
            'app_id': 'app', 'task_id': 'task', 'name': 'name', 'note': '',
            'chatbot_id': 'chatbot', 'prompt': 'prompt', 'keywords': 'keyword',
            'word_count_min': 500, 'word_count_max': 1200, 'image_count': 0,
            'article_count': 1, 'cycle_type': 'none', 'cycle_interval': 3600,
            'file_list': [], 'site_id_list': ['site']
        }
        task_setting = MagicMock()
        with app.test_request_context(json=payload), \
                patch.object(grow_ai_service, 'check_access_token_valid', return_value=True), \
                patch.object(lanying_grow_ai, 'get_task', return_value={
                    'auto_deploy': 'off', 'article_prompt': 'keep this prompt'
                }), \
                patch.object(lanying_grow_ai, 'TaskSetting', return_value=task_setting) as setting_class, \
                patch.object(lanying_grow_ai, 'configure_task', return_value={
                    'result': 'ok', 'data': {'success': True}
                }):
            response = grow_ai_service.configure_task()

        self.assertEqual(200, response.status_code)
        self.assertEqual('off', setting_class.call_args.kwargs['auto_deploy'])
        self.assertEqual('keep this prompt', setting_class.call_args.kwargs['article_prompt'])

    def test_generate_article_uses_task_article_prompt(self):
        task = {
            'task_id': 'task', 'image_count': 0, 'word_count_min': 500,
            'word_count_max': 1200, 'embedding_condition': {},
            'prompt': 'product subject', 'article_prompt': 'Use a conversational tone.'
        }
        task_run = {'task_run_id': 'run', 'user_id': 'user'}
        with patch.object(lanying_grow_ai, 'get_task_site_list', return_value=[]), \
                patch.object(lanying_grow_ai, 'clean_user_message_count'), \
                patch.object(lanying_grow_ai, 'generate_article', return_value={
                    'result': 'error'
                }) as generate_article, \
                patch.object(lanying_grow_ai, 'handle_ai_response_error', return_value={
                    'result': 'error'
                }):
            lanying_grow_ai.do_run_task_article(
                'app', task_run, task, 'article', 'chatbot', 'Article title'
            )

        text_prompt = generate_article.call_args.args[6]
        self.assertIn('生成文章时还需要遵循以下要求：\nUse a conversational tone.\n', text_prompt)
        self.assertIn('文章标题必须为：Article title\n', text_prompt)

    def test_preview_workflow_uses_deploy_token(self):
        with patch.dict('os.environ', {
            'GITHUB_HOSTING_TOKEN': 'content-token',
            'GROW_AI_GITHUB_TOKEN': 'workflow-token'
        }):
            headers = lanying_grow_ai.get_grow_ai_workflow_headers()

        self.assertEqual('token workflow-token', headers['Authorization'])

    def test_preview_workflow_dispatch_retries_transient_failure(self):
        failed_response = MagicMock(status_code=503, text='temporarily unavailable')
        success_response = MagicMock(status_code=204, text='')
        preview = {
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1',
            'preview_commit_sha': 'a' * 40
        }
        context = {
            'result': 'ok', 'repository': 'maxim-top/site',
            'site': {'site_name': 'example'}
        }
        with patch.object(lanying_grow_ai, 'get_preview_github_context', return_value=context), \
                patch.object(lanying_grow_ai, 'maybe_add_site_url'), \
                patch.object(lanying_grow_ai, 'set_preview_callback'), \
                patch.object(lanying_grow_ai.lanying_utils, 'get_internet_connector_server', return_value='https://connector.test'), \
                patch.object(lanying_grow_ai.requests, 'post', side_effect=[failed_response, success_response]) as post, \
                patch.object(lanying_grow_ai.time, 'sleep') as sleep:
            result = lanying_grow_ai.dispatch_preview_workflow(preview)

        self.assertEqual('ok', result['result'])
        self.assertEqual(2, post.call_count)
        sleep.assert_called_once_with(1)

    def test_preview_uses_first_site_like_direct_deploy(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        preview_task = MagicMock()
        first_site = {'site_id': 'site1', 'github_hosting': 'on'}
        with patch.object(lanying_grow_ai, 'get_task_run', return_value={
            'task_id': 'task', 'zip_file': 'book.zip'
        }), patch.object(lanying_grow_ai, 'get_task', return_value={'task_id': 'task'}), \
                patch.object(lanying_grow_ai, 'get_task_site_list', return_value=[
                    first_site, {'site_id': 'site2', 'github_hosting': 'on'}
                ]), patch.object(lanying_grow_ai, 'get_site', return_value=first_site), \
                patch.object(lanying_grow_ai, 'generate_preview_id', return_value='preview1'), \
                patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'update_site_field') as update_site, \
                patch.object(lanying_grow_ai, 'update_task_run_field'), patch.dict('sys.modules', {
                    'lanying_tasks': MagicMock(grow_ai_preview_task=preview_task)
                }):
            result = lanying_grow_ai.task_run_preview('app', 'run')

        self.assertEqual('ok', result['result'])
        update_site.assert_called_with('app', 'site1', 'pending_preview_id', 'preview1')
        preview_task.apply_async.assert_called_once_with(args=['app', 'preview1'])

    def test_preview_callback_is_consumed_once(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        callback = {'type': 'deploy_check', 'app_id': 'app', 'preview_id': 'p1'}
        with patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'get_preview_callback', side_effect=[callback, None]), \
                patch.object(lanying_grow_ai, 'delete_preview_callback') as delete:
            first = lanying_grow_ai.consume_preview_callback('code')
            second = lanying_grow_ai.consume_preview_callback('code')

        self.assertEqual(callback, first)
        self.assertIsNone(second)
        delete.assert_called_once_with('code')

    def test_check_preview_deploy_rejects_superseded_preview(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        with patch.object(lanying_grow_ai, 'consume_preview_callback', return_value={
            'type': 'deploy_check', 'app_id': 'app', 'preview_id': 'p1'
        }), patch.object(lanying_grow_ai, 'get_preview', return_value={
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1'
        }), patch.object(lanying_grow_ai, 'get_site', return_value={
            'pending_preview_id': 'p2'
        }), patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'update_preview_field') as update:
            result = lanying_grow_ai.check_preview_deploy('code', 100)

        self.assertEqual('error', result['result'])
        self.assertEqual('preview superseded', result['message'])
        update.assert_not_called()

    def test_preview_deploy_finish_activates_pending_preview(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        preview = {'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1'}
        with patch.object(lanying_grow_ai, 'consume_preview_callback', return_value={
            'type': 'deploy_finish', 'app_id': 'app', 'preview_id': 'p1'
        }), \
                patch.object(lanying_grow_ai, 'get_preview', return_value=preview), \
                patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'get_site', return_value={
                    'pending_preview_id': 'p1', 'active_preview_id': ''
                }), patch.object(lanying_grow_ai, 'update_site_field') as update_site, \
                patch.object(lanying_grow_ai, 'update_preview_field') as update_preview:
            result = lanying_grow_ai.preview_deploy_finish('code', 'ok')

        self.assertEqual('ok', result['result'])
        update_site.assert_has_calls([
            call('app', 'site', 'active_preview_id', 'p1'),
            call('app', 'site', 'pending_preview_id', '')
        ])
        update_preview.assert_called_with('app', 'p1', 'status', 'ready')

    def test_preview_publish_rejects_changed_base_branch(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        base_response = MagicMock(status_code=200)
        base_response.json.return_value = {'object': {'sha': 'new-base'}}
        branch_response = MagicMock(status_code=200)
        branch_response.json.return_value = {'object': {'sha': 'preview-sha'}}
        preview = {
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1', 'status': 'ready',
            'branch_name': 'preview-branch', 'base_commit_sha': 'old-base',
            'preview_commit_sha': 'preview-sha'
        }
        context = {
            'result': 'ok', 'repository': 'org/repo', 'api_url': 'https://api.github.test/repos/org/repo',
            'headers': {}, 'site': {'github_base_branch': 'master', 'commit_type': 'branch'}
        }
        with patch.object(lanying_grow_ai, 'get_preview', return_value=preview), \
                patch.object(lanying_grow_ai, 'get_preview_github_context', return_value=context), \
                patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai.requests, 'get', side_effect=[base_response, branch_response]), \
                patch.object(lanying_grow_ai, 'update_preview_field') as update:
            result = lanying_grow_ai.preview_publish('app', 'p1')

        self.assertEqual('error', result['result'])
        self.assertEqual('base branch changed', result['message'])
        update.assert_any_call('app', 'p1', 'status', 'error')

    def test_failed_production_deploy_does_not_clear_preview(self):
        with patch.object(lanying_grow_ai, 'get_deploy_code', return_value={
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1'
        }), patch.object(lanying_grow_ai, 'get_site', return_value={}), \
                patch.object(lanying_grow_ai, 'update_site_field') as update, \
                patch.object(lanying_grow_ai, 'dispatch_clear_preview') as clear:
            result = lanying_grow_ai.deploy_finish('code', 'error')

        self.assertFalse(result['data']['success'])
        update.assert_any_call('app', 'site', 'deploy_result', 'failed')
        clear.assert_not_called()

    def test_direct_deploy_discards_site_preview(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        deploy_task = MagicMock()
        task_run = {
            'status': 'success', 'deploy_status': 'wait', 'zip_file': 'book.zip',
            'preview_id': 'p1', 'task_id': 'task'
        }
        with patch.object(lanying_grow_ai, 'get_task_run', return_value=task_run), \
                patch.object(lanying_grow_ai, 'get_task', return_value={'task_id': 'task'}), \
                patch.object(lanying_grow_ai, 'get_task_site_list', return_value=[{'site_id': 'site'}]), \
                patch.object(lanying_grow_ai, 'get_site', return_value={
                    'site_id': 'site', 'active_preview_id': 'p1', 'pending_preview_id': ''
                }), patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'discard_site_previews_for_direct_publish', return_value={
                    'result': 'ok', 'data': {'success': True}
                }) as discard, patch.dict('sys.modules', {
                    'lanying_tasks': MagicMock(grow_ai_deply_task_run=deploy_task)
                }):
            result = lanying_grow_ai.deploy_task_run('app', 'run')

        self.assertEqual('ok', result['result'])
        discard.assert_called_once()
        deploy_task.apply_async.assert_called_once_with(args=['app', 'run'])

    def test_active_preview_discard_rejects_pending_preview_before_closing_pr(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        preview = {
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1',
            'status': 'pr_open', 'pr_number': 1
        }
        context = {
            'result': 'ok', 'site': {}, 'api_url': 'https://api.github.test/repos/org/repo',
            'headers': {}
        }
        with patch.object(lanying_grow_ai, 'get_preview', return_value=preview), \
                patch.object(lanying_grow_ai, 'get_preview_github_context', return_value=context), \
                patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'get_site', return_value={
                    'active_preview_id': 'p1', 'pending_preview_id': 'p2'
                }), patch.object(lanying_grow_ai.requests, 'patch') as close_pr:
            result = lanying_grow_ai.preview_discard('app', 'p1')

        self.assertEqual('error', result['result'])
        self.assertEqual('another preview is building', result['message'])
        close_pr.assert_not_called()

    def test_clear_dispatch_failure_marks_preview_error(self):
        redis = MagicMock()
        redis.lock.return_value = DummyLock()
        preview = {'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1'}
        site = {'active_preview_id': 'p1', 'pending_preview_id': ''}
        with patch.object(lanying_grow_ai, 'get_deploy_code', return_value={
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1'
        }), patch.object(lanying_grow_ai, 'get_site', return_value=site), \
                patch.object(lanying_grow_ai, 'get_preview', return_value=preview), \
                patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'dispatch_clear_preview', return_value={
                    'result': 'error', 'message': 'preview clear workflow dispatch failed'
                }), patch.object(lanying_grow_ai, 'update_preview_field') as update:
            result = lanying_grow_ai.deploy_finish('code', 'ok')

        self.assertTrue(result['data']['success'])
        update.assert_any_call('app', 'p1', 'status', 'error')
        update.assert_any_call('app', 'p1', 'error', 'preview clear workflow dispatch failed')

    def test_merged_pr_with_changed_tree_is_not_associated(self):
        redis = MagicMock()
        redis.smembers.return_value = {'app:p1'}
        preview = {
            'app_id': 'app', 'site_id': 'site', 'preview_id': 'p1', 'pr_number': 1,
            'preview_commit_sha': 'a' * 40
        }
        context = {
            'result': 'ok', 'repository': 'org/repo',
            'api_url': 'https://api.github.test/repos/org/repo', 'headers': {}, 'site': {}
        }
        pr_response = MagicMock(status_code=200)
        pr_response.json.return_value = {'merged': True, 'merge_commit_sha': 'b' * 40}
        with patch.object(lanying_grow_ai.lanying_redis, 'get_redis_connection', return_value=redis), \
                patch.object(lanying_grow_ai, 'get_preview', return_value=preview), \
                patch.object(lanying_grow_ai, 'get_preview_github_context', return_value=context), \
                patch.object(lanying_grow_ai.requests, 'get', return_value=pr_response), \
                patch.object(lanying_grow_ai, 'get_github_commit_tree_sha', side_effect=['merge-tree', 'preview-tree']), \
                patch.object(lanying_grow_ai, 'update_preview_field') as update:
            lanying_grow_ai.reconcile_preview_pull_requests()

        update.assert_any_call('app', 'p1', 'status', 'error')
        update.assert_any_call('app', 'p1', 'error', 'published content changed')
        redis.set.assert_not_called()


if __name__ == '__main__':
    unittest.main()
