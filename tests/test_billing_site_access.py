import ast
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from flask import Flask, make_response, request
from redis.exceptions import ConnectionError

import lanying_billing_site_access as access
from lanying_billing_hosting import _state_hash


class SiteAccessTest(unittest.TestCase):
    def setUp(self):
        self.rows = {
            'lanying_connector:grow_ai:github_site_name': {'abcdef': 'owner/book'},
            'lanying_connector:grow_ai:github_repo_site:owner:book': {'17': 'appa'},
            'lanying_connector:grow_ai:site:appa:17': {
                'type': 'gitbook', 'github_url': 'https://github.com/owner/book', 'domain_id': '9'},
            'lanying_connector:custom_domain_name:docs.example.com': {
                'app_id': 'appa', 'site_id': '17', 'domain_id': '9'},
            'lanying_connector:custom_domain_info:appa:17:9': {
                'app_id': 'appa', 'site_id': '17', 'domain_name': 'docs.example.com', 'cdn_status': 'online'},
        }
        self.redis = Mock()
        self.redis.hgetall.side_effect = lambda key: {
            k.encode(): v.encode() for k, v in self.rows.get(key, {}).items()}
        self.redis.hget.side_effect = lambda key, field: self.rows.get(key, {}).get(field, '').encode()

    def period(self, app='appa', end=200, product='9801', **updates):
        state = dict(app_id=app, status='normal', billing_order_id='a' * 32,
                     billing_writer='ORDER', billing_period_end_epoch=str(end), product_id=product)
        state.update(updates)
        state['billing_state_hash'] = _state_hash(state)
        self.rows['lanying-connector:grow_ai:service_status:' + app] = state
        return state

    def allowed(self, host='abcdef.site.chatai101.com', now=100):
        return access.allowed(host, redis=self.redis, now=now)

    def test_paid_period_expiry_and_restore_cover_both_hosts(self):
        for host in ('abcdef.site.chatai101.com', 'docs.example.com'):
            with self.subTest(host=host):
                self.period(end=200)
                self.assertTrue(self.allowed(host, 199))
                self.assertFalse(self.allowed(host, 200))
                self.period(end=300)
                self.assertTrue(self.allowed(host, 200))

    def test_legacy_and_free_are_not_subject_to_new_paid_expiry(self):
        self.assertTrue(self.allowed())
        self.rows['lanying-connector:grow_ai:service_status:appa'] = {'status': 'normal', 'product_id': '9801'}
        self.assertTrue(self.allowed())
        self.period(end=10, product='9851')
        self.assertTrue(self.allowed())

    def test_unrelated_app_and_credit_failure_do_not_stop_site(self):
        self.period()
        self.period('appb', end=10)
        self.rows['embedding:app_config:appa'] = {'lanying_connector.new_deduct_failed': 'true'}
        self.assertTrue(self.allowed())

    def test_invalid_or_unknown_host_never_allows(self):
        for host in ('', None, 'user@docs.example.com', 'docs.example.com:443', 'https://docs.example.com',
                     'docs.example.com,evil.com', '../abcdef.site.chatai101.com', 'missing.site.chatai101.com'):
            with self.subTest(host=host):
                self.assertFalse(self.allowed(host))
        self.assertTrue(self.allowed('DOCS.EXAMPLE.COM.'))

    def test_stale_repository_registration_cannot_authorize_deleted_or_changed_site(self):
        site = self.rows['lanying_connector:grow_ai:site:appa:17']
        site['github_url'] = 'https://github.com/other/book'
        self.assertFalse(self.allowed())
        del self.rows['lanying_connector:grow_ai:site:appa:17']
        self.assertFalse(self.allowed())

    def test_current_repository_forms(self):
        for url in ('git@github.com:owner/book.git', 'https://github.com/owner/book.git',
                    'https://github.com/owner/book/', 'https://github.com/owner/book/tree/main'):
            self.rows['lanying_connector:grow_ai:site:appa:17']['github_url'] = url
            self.assertTrue(self.allowed())

    def test_same_app_duplicate_registration_is_not_cross_app_ambiguity(self):
        self.rows['lanying_connector:grow_ai:github_repo_site:owner:book']['18'] = 'appa'
        self.rows['lanying_connector:grow_ai:site:appa:18'] = dict(self.rows['lanying_connector:grow_ai:site:appa:17'])
        self.assertTrue(self.allowed())

    def test_cross_app_shared_host_requires_ownership_resolution(self):
        self.rows['lanying_connector:grow_ai:github_repo_site:owner:book']['18'] = 'appb'
        self.rows['lanying_connector:grow_ai:site:appb:18'] = dict(self.rows['lanying_connector:grow_ai:site:appa:17'])
        with self.assertRaises(access.SiteAccessUnavailable):
            self.allowed()
        # A stale reverse entry does not count as a current owner.
        self.rows['lanying_connector:grow_ai:site:appb:18']['github_url'] = 'https://github.com/elsewhere/book'
        self.assertTrue(self.allowed())

    def test_custom_domain_reassignment_or_disabled_domain_is_denied(self):
        self.rows['lanying_connector:grow_ai:site:appa:17']['domain_id'] = '10'
        self.assertFalse(self.allowed('docs.example.com'))
        self.rows['lanying_connector:grow_ai:site:appa:17']['domain_id'] = '9'
        self.rows['lanying_connector:custom_domain_info:appa:17:9']['cdn_status'] = 'offline'
        self.assertFalse(self.allowed('docs.example.com'))

    def test_legacy_write_cannot_resurrect_paid_site(self):
        state = self.period()
        state['billing_period_end_epoch'] = '999'
        with self.assertRaises(access.SiteAccessUnavailable):
            self.allowed()
        state = self.period(billing_writer='LEGACY')
        with self.assertRaises(access.SiteAccessUnavailable):
            self.allowed()

    def test_invalid_paid_period_is_not_legacy(self):
        self.period(end='not-a-timestamp')
        with self.assertRaises(access.SiteAccessUnavailable):
            self.allowed()

    def test_access_is_read_only(self):
        self.period()
        self.allowed()
        self.assertTrue(all(call[0] in ('hgetall', 'hget') for call in self.redis.mock_calls))

    def test_mutation_gate_blocks_expired_paid_plan_but_allows_legacy(self):
        with patch.object(access.lanying_redis, 'get_redis_connection', return_value=self.redis), patch.object(access.time, 'time', return_value=100):
            access.require_active('appa')
            self.period(end=90)
            with self.assertRaisesRegex(ValueError, 'plan_payment_required'):
                access.require_active('appa')
            self.period(end=120)
            access.require_active('appa')
            self.redis.hgetall.side_effect = ConnectionError('unavailable')
            with self.assertRaises(ConnectionError):
                access.require_active('appa')

    def test_publish_and_storage_reject_before_writes(self):
        # Exercise the actual call sites; an expired plan stops before Redis,
        # GitHub, task scheduling or storage increments can be reached.
        targets = [('lanying_embedding.py', 'add_storage_size', ('appa', 'kb', 'doc', 1)),
                   ('lanying_grow_ai.py', 'preview_publish', ('appa', 'preview')),
                   ('lanying_grow_ai.py', 'deploy_task_run', ('appa', 'run'))]
        for path, name, args in targets:
            node = next(n for n in ast.parse(Path(path).read_text()).body if isinstance(n, ast.FunctionDef) and n.name == name)
            node.decorator_list = []
            context = {}
            exec(compile(ast.Module(body=[node], type_ignores=[]), path, 'exec'), context)
            with patch.object(access, 'require_active', side_effect=ValueError('plan_payment_required')):
                with self.assertRaisesRegex(ValueError, 'plan_payment_required'):
                    context[name](*args)

    def test_expired_material_upload_stops_before_space_record_or_source_write(self):
        import sys
        import types
        module = ast.parse(Path('lanying_seenical_materials.py').read_text())
        names = {'ingest_message', '_require_material_service'}
        nodes = [n for n in module.body if isinstance(n, ast.FunctionDef) and n.name in names]
        space = Mock(side_effect=AssertionError('must not create library'))
        engine = Mock(side_effect=AssertionError('must not write material'))
        context = dict(os=os, MaterialError=ValueError, MAX_FILE_BYTES=30*1024*1024,
                       message_ext=lambda msg: {'seenical': {'material_version': 1}},
                       message_conversation=lambda msg: {'seenical_session_id': 'session'},
                       validate_source=Mock(), validate_attachment_identity=Mock(), ensure_space=space, _engine=engine)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'lanying_seenical_materials.py', 'exec'), context)
        embedding = types.SimpleNamespace(allow_exts=lambda: ['.txt'])
        message = dict(ctype='FILE', appId='appa', msgId='123', attachment={'dName': 'test.txt', 'fLen': 10, 'url': 'https://example.com/file'})
        with patch.dict(sys.modules, {'lanying_embedding': embedding}), patch.object(access, 'app_active', return_value=False):
            with self.assertRaisesRegex(ValueError, 'plan_payment_required'):
                context['ingest_message'](message)
        space.assert_not_called()
        engine.assert_not_called()



class SiteAccessRouteTest(unittest.TestCase):
    def setUp(self):
        # Full grow_ai_service starts unrelated storage/browser dependencies;
        # execute the actual route body with Flask and the real access module.
        path = Path(__file__).resolve().parents[1] / 'services/grow_ai_service.py'
        node = next(n for n in ast.parse(path.read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'billing_site_access')
        node.decorator_list = []
        context = dict(request=request, make_response=make_response, os=os)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), context)
        app = Flask(__name__)
        app.add_url_rule('/grow_ai/site-access', view_func=context['billing_site_access'], methods=['GET', 'HEAD'])
        self.client = app.test_client()

    def test_disabled_never_queries_state(self):
        with patch.dict(os.environ, {'SEENICAL_SITE_ACCESS_ENABLED': 'off'}), patch.object(access, 'allowed') as check:
            self.assertEqual(503, self.client.get('/grow_ai/site-access').status_code)
            check.assert_not_called()

    def test_actual_http_status_not_json_code_and_no_app_override(self):
        with patch.dict(os.environ, {'SEENICAL_SITE_ACCESS_ENABLED': 'on'}), patch.object(access, 'allowed') as check:
            for allowed, expected in ((True, 200), (False, 403)):
                check.return_value = allowed
                response = self.client.get('/grow_ai/site-access?app_id=other', headers={'X-Seenical-Site-Host': 'docs.example.com'})
                self.assertEqual(expected, response.status_code)
                self.assertEqual(b'', response.data)
                self.assertEqual('no-store', response.headers['Cache-Control'])
                check.assert_called_with('docs.example.com')
            for error in (ConnectionError('local test'), access.SiteAccessUnavailable('ambiguous')):
                check.side_effect = error
                self.assertEqual(503, self.client.head('/grow_ai/site-access').status_code)
