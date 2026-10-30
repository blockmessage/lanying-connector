import ast
import json
import os
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from redis import Redis
from flask import Flask, request, make_response
import lanying_billing_storage as storage
from lanying_billing_hosting import _state_hash


class StorageRouteTest(unittest.TestCase):
    def test_auth_and_unverified_response(self):
        p=Path('services/grow_ai_service.py')
        route=next(n for n in ast.parse(p.read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='billing_storage_close')
        route.decorator_list=[]
        ns=dict(request=request,make_response=make_response,check_access_token_valid=lambda:request.headers.get('access-token')=='test')
        exec(compile(ast.Module(body=[route],type_ignores=[]),str(p),'exec'),ns)
        app=Flask(__name__);app.add_url_rule('/close',view_func=ns['billing_storage_close'],methods=['POST'])
        client=app.test_client()
        with patch.object(storage,'close',return_value={'status':'unverified','storage_bytes':None}) as close:
            self.assertEqual(401,client.post('/close',json={}).status_code);close.assert_not_called()
            self.assertIsNone(client.post('/close',json={},headers={'access-token':'test'}).json['data']['storage_bytes'])


@unittest.skipUnless(os.getenv('BILLING_TEST_LOCAL_REDIS')=='1','disposable localhost Redis only')
class StorageRedisTest(unittest.TestCase):
    def setUp(self):
        self.r=Redis(host='127.0.0.1',port=36379);self.app=uuid.uuid4().hex;self.order=uuid.uuid4().hex
        self.site=f'lanying_connector:grow_ai:site:{self.app}:1'
        self.code=uuid.uuid4().hex;self.code_key='lanying_connector:grow_ai:deploy_code:'+self.code
        self.state_key='lanying-connector:grow_ai:service_status:'+self.app
        self.list_key='lanying_connector:grow_ai:site_list:'+self.app
        self.receipt_key=f'lanying_connector:billing:storage:{self.app}:{self.order}'
        self.end=int(time.time())-10
        state={'app_id':self.app,'billing_writer':'ORDER','billing_order_id':self.order,'billing_environment':'LIVE','billing_period_end_epoch':self.end}
        state['billing_state_hash']=_state_hash(state)
        self.r.hset(self.state_key,mapping=state)
        self.r.hset(self.site,mapping={'create_time':1,'website_storage':10,'billing_storage_bytes':10,'billing_storage_verified':'1'})
        self.r.rpush(self.list_key,'1')
        self.r.setex(self.code_key,3600,json.dumps({'app_id':self.app,'site_id':'1'}))
        self.terms=dict(app_id=self.app,order_id=self.order,environment='LIVE',period_end_epoch=self.end)
    def tearDown(self):
        self.r.delete(self.site,self.code_key,self.state_key,self.list_key,self.receipt_key);self.r.close()
    def test_failed_deploy_preserves_last_confirmed_size(self):
        storage.stage(self.app,'1',self.code,200,self.r)
        self.r.hset(self.site,'website_storage',200)  # legacy provisional counter
        storage.finish(self.app,'1',self.code,False,self.r)
        self.assertEqual(10,storage.close(self.terms,self.r)['storage_bytes'])
    def test_success_and_snapshot_replay_are_immutable(self):
        storage.stage(self.app,'1',self.code,200,self.r);storage.finish(self.app,'1',self.code,True,self.r)
        storage.finish(self.app,'1',self.code,True,self.r)
        first=storage.close(self.terms,self.r);self.assertEqual(200,first['storage_bytes'])
        self.r.hset(self.site,'billing_storage_bytes',400)
        self.assertEqual(first,storage.close(self.terms,self.r))
    def test_historical_and_pending_are_unverified_not_zero(self):
        self.r.hdel(self.site,'billing_storage_verified')
        first=storage.close(self.terms,self.r);self.assertEqual('unverified',first['status']);self.assertIsNone(first['storage_bytes'])
        self.r.delete(self.receipt_key);self.r.hset(self.site,'billing_storage_verified','1')
        storage.stage(self.app,'1',self.code,200,self.r)
        self.assertIsNone(storage.close(self.terms,self.r)['storage_bytes'])
    def test_changed_size_or_outcome_is_rejected(self):
        storage.stage(self.app,'1',self.code,200,self.r)
        with self.assertRaises(ValueError):storage.stage(self.app,'1',self.code,201,self.r)
        storage.finish(self.app,'1',self.code,False,self.r)
        with self.assertRaises(ValueError):storage.finish(self.app,'1',self.code,True,self.r)
    def test_identity_environment_and_period_cannot_be_forged(self):
        for key,value in [('order_id','a'*32),('environment','TEST'),('period_end_epoch',self.end-1)]:
            with self.subTest(key=key),self.assertRaises(ValueError):storage.close(dict(self.terms,**{key:value}),self.r)
        self.assertFalse(self.r.exists(self.receipt_key))
    def test_unverified_legacy_callback_cannot_certify_storage(self):
        storage.finish(self.app,'1',self.code,True,self.r)
        self.assertIsNone(storage.close(self.terms,self.r)['storage_bytes'])

if __name__=='__main__':unittest.main()
