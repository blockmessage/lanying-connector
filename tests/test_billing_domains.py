import ast
import logging
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


def domain_function(**values):
    path=Path(__file__).resolve().parents[1]/'lanying_grow_ai.py'
    node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='domain_num_limit_changed')
    scope=dict(logging=logging, **values)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
    return scope[node.name]


class DomainActivationTest(unittest.TestCase):
    def setUp(self):
        self.domains={'a':{'domain_name':'a.example','cdn_status':'online'},
                      'b':{'domain_name':'b.example','cdn_status':'online'}}
        self.cdn=Mock();self.update=Mock(side_effect=lambda app,site,domain,field,value:self.domains[domain].update({field:value}))
        self.call=domain_function(get_site_list=lambda app:{'data':{'list':[
            {'site_id':'a','domain_id':'a'},{'site_id':'b','domain_id':'b'}]}},
            get_custom_domain_info=lambda app,site,domain:self.domains[domain],
            update_custom_domain_info=self.update,lanying_cdn=self.cdn,lanying_slack=Mock())

    def test_replay_does_not_restart_successful_domains(self):
        self.call('app',1,42,strict=True)
        self.call('app',1,42,strict=True)
        self.cdn.stop_cdn.assert_called_once_with('b.example')
        self.cdn.start_cdn.assert_not_called()
        self.assertEqual('online',self.domains['a']['cdn_status'])

    def test_strict_failure_is_visible_and_original_operation_can_recover(self):
        self.cdn.stop_cdn.side_effect=RuntimeError('remote unavailable')
        with self.assertRaises(RuntimeError):self.call('app',1,42,strict=True)
        self.assertEqual('online',self.domains['b']['cdn_status'])
        self.cdn.stop_cdn.side_effect=None
        self.call('app',1,42,strict=True)
        self.assertEqual('offline',self.domains['b']['cdn_status'])

    def test_legacy_behavior_is_unchanged(self):
        self.cdn.stop_cdn.side_effect=RuntimeError('remote unavailable')
        with self.assertLogs(level='ERROR'):
            self.assertEqual('ok',self.call('app',1,42)['result'])

class DomainRouteTest(unittest.TestCase):
    def test_requires_service_authority_and_current_paid_hosting_identity(self):
        from flask import Flask, request, make_response
        from unittest.mock import patch
        import lanying_redis
        import lanying_billing_hosting
        path=Path(__file__).resolve().parents[1]/'services/grow_ai_service.py'
        node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='billing_domains')
        node.decorator_list=[]
        grow=Mock()
        scope=dict(request=request,make_response=make_response,lanying_grow_ai=grow,
                   check_access_token_valid=lambda:request.headers.get('access-token')=='local')
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        app=Flask(__name__);app.add_url_rule('/domains',view_func=scope[node.name],methods=['POST'])
        client=app.test_client()
        value=dict(app_id='app',order_id='a'*32,order_sequence=1,environment='TEST',max_domain_num=1,tenement_id=42)
        headers={'access-token':'local'}
        with patch.object(lanying_redis,'get_redis_connection',return_value=Mock()), \
             patch.object(lanying_redis,'redis_hgetall',return_value={}) as read:
            self.assertEqual(401,client.post('/domains',json=value).status_code)
            self.assertEqual(400,client.post('/domains',headers=headers,json=dict(value,max_domain_num=-1)).status_code)
            self.assertEqual(409,client.post('/domains',headers=headers,json=value).status_code)
            grow.domain_num_limit_changed.assert_not_called()
            old=dict(billing_order_id='a'*32,billing_order_sequence='1',billing_environment='TEST',billing_writer='ORDER',status='normal')
            old['billing_state_hash']=lanying_billing_hosting._state_hash(old);read.return_value=old
            self.assertEqual(200,client.post('/domains',headers=headers,json=value).status_code)
            grow.domain_num_limit_changed.assert_called_once_with('app',1,42,strict=True)
            grow.domain_num_limit_changed.side_effect=RuntimeError('CDN failure')
            with self.assertLogs(app.logger,level='ERROR'):
                self.assertEqual(500,client.post('/domains',headers=headers,json=value).status_code)
