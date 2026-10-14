import unittest
import ast
from pathlib import Path
from flask import Flask, request
from unittest import mock
from sqlalchemy import create_engine, select
import lanying_gift_quota as credits
import lanying_message_credits as settings


class MessageCreditTest(unittest.TestCase):
    def setUp(self):
        self.db = create_engine('sqlite://')
        credits.metadata.create_all(self.db)
        for patch in (mock.patch.object(credits, 'engine', return_value=self.db),
                      mock.patch.object(credits, 'gift_enabled', return_value=True),
                      mock.patch.object(credits, 'purchases_enabled', return_value=True)):
            patch.start(); self.addCleanup(patch.stop)
        self.addCleanup(self.db.dispose)

    def grant(self, app='a', points=1000, expiry=200, now=100):
        return credits.grant_purchase('seenical-message-credit:'+'a'*32, app, points, expiry, 'manual', now)

    def use(self, app='a', points=50, when=110, usage='one'):
        return credits.settle(dict(usage_id=usage, app_id=app, quota=points, occurred_epoch=when,
                                   keys=['shared-period', app+'-period'], api_key_type='share'))

    def test_purchase_identity_and_expiry_are_frozen(self):
        self.assertEqual(self.grant(), self.grant(now=150))
        for change in ({'app':'b'}, {'points':2000}, {'expiry':201}):
            with self.assertRaises(ValueError): self.grant(**change)
        self.assertTrue(self.grant(now=300)['granted'])
        with self.assertRaises(ValueError):
            credits.grant_purchase('seenical-message-credit:'+'b'*32,'a',1000,200,'auto',300)

    def test_invitation_then_purchase_then_normal_allowance(self):
        credits.grant('seenical-invitation:1','a',10,now=100)
        self.grant(points=20)
        result=self.use(points=50)
        self.assertEqual(10,result['gift_used']); self.assertEqual(20,result['purchase_used'])
        self.assertEqual(20,credits.billable_used('shared-period',50))
        self.assertEqual(result,self.use(points=50))
        self.assertEqual(0,credits.status('a',now=120,purchased=True)['remaining'])

    def test_other_app_cannot_spend_purchase_or_consume_shared_quota_for_it(self):
        self.grant()
        self.assertEqual(0,self.use(app='b')['purchase_used'])
        self.assertEqual(1000,credits.status('a',now=120,purchased=True)['remaining'])
        self.assertEqual(50,self.use(usage='two')['purchase_used'])
        self.assertEqual(50,credits.billable_used('shared-period',100))
        self.assertEqual(0,credits.billable_used('a-period',50))

    def test_expiry_retains_discount_but_stops_future_consumption(self):
        self.grant(); self.use()
        self.assertEqual(0,credits.status('a',now=200,purchased=True)['remaining'])
        self.assertEqual(0,credits.billable_used('a-period',50))
        self.assertEqual(0,self.use(when=200,usage='later')['purchase_used'])

    def test_purchase_is_not_reported_as_invitation_gift(self):
        self.grant()
        self.assertEqual(0,credits.status('a',now=110)['remaining'])
        self.assertEqual(1000,credits.status('a',now=110,purchased=True)['remaining'])

    def test_explicit_settings_and_legacy_defaults(self):
        for raw, product, expected in ((None,7001,False),(None,7002,True),('false',7002,False),('true',7001,True)):
            with mock.patch.object(settings.lanying_redis,'redis_hget',return_value=raw), \
                 mock.patch.object(settings.lanying_redis,'get_redis_connection',return_value=mock.Mock()):
                self.assertEqual(expected,settings.auto_recharge('a',{'product_id':product,'enable_extra_price':product>7001}))

    def test_credit_delivery_retry_does_not_duplicate_consumption(self):
        self.grant(); self.use()
        self.grant(now=190)
        self.assertEqual(950,credits.status('a',now=190,purchased=True)['remaining'])
        with self.db.connect() as conn:
            self.assertEqual(1,len(conn.execute(select(credits.usage)).all()))

    def test_internal_grant_auth_and_expiry_envelope(self):
        tree=ast.parse(Path('lanying_connector.py').read_text())
        node=next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name=='message_credits')
        node.decorator_list=[]
        namespace={'request':request, 'accessToken':'service-secret'}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'message_credits_route','exec'),namespace)
        app=Flask(__name__)
        app.add_url_rule('/credits/<operation>',view_func=namespace['message_credits'],methods=['POST'])
        client=app.test_client()
        body={'app_id':'a', 'business_id':'seenical-message-credit:'+'c'*32,
              'points':1000, 'expires_epoch':1, 'source':'manual'}
        self.assertEqual(401,client.post('/credits/grant',json=body).status_code)
        response=client.post('/credits/grant',json=body,headers={'access-token':'service-secret'})
        self.assertEqual(200,response.status_code)
        self.assertEqual('MESSAGE_CREDIT_EXPIRED',response.json['error_code'])


if __name__ == '__main__': unittest.main()
