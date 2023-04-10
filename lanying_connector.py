import os
from flask import Flask, request, render_template
import requests
import logging
import json
from concurrent.futures import ThreadPoolExecutor
import importlib
import sys
import lanying_config
import copy
import time
import lanying_redis

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
executor = ThreadPoolExecutor(8)
sys.path.append("services")
lanying_config.init()
app = Flask(__name__)
if os.environ.get("FLASK_DEBUG"):
    app.debug = True

accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')

@app.route("/", methods=["GET"])
def index():
    service = lanying_config.get_lanying_connector_service('')
    return render_template("index.html", msgReceivedCnt=getMsgReceivedCnt(), msgSentCnt=getMsgSentCnt(), service=service)

@app.route("/messages", methods=["POST"])
def messages():
    addMsgReceivedCnt(1)
    text = request.get_data(as_text=True)
    data = json.loads(text)
    logging.debug(data)
    fromUserId = data['from']['uid']
    toUserId = data['to']['uid']
    type = data['type']
    ctype = data['ctype']
    appId = data['appId']
    now = time.time()
    config = lanying_config.get_lanying_connector(appId)
    productId = 0
    if config and 'product_id' in config:
        productId = config['product_id']
    ExpireTime = lanying_config.get_lanying_connector_expire_time(appId)
    if productId == 0 and (ExpireTime == None or (ExpireTime > 0 and now > ExpireTime)):
        logging.debug(f"service is expired: appId={appId}")
        resp = app.make_response('service is expired')
        return resp
    callbackSignature = lanying_config.get_lanying_callback_signature(appId)
    if callbackSignature and len(callbackSignature) > 0:
        headSignature = request.headers.get('signature')
        if callbackSignature != headSignature:
            logging.info(f'callback signature not match: appId={appId}')
            resp = app.make_response('callback signature not match')
            return resp
    myUserId = lanying_config.get_lanying_user_id(appId)
    logging.debug(f'lanying_user_id:{myUserId}')
    if myUserId != None and toUserId == myUserId and fromUserId != myUserId and type == 'CHAT' and ctype == 'TEXT':
        executor.submit(queryAndSendMessage, data)
    resp = app.make_response('')
    return resp

@app.route("/config", methods=["POST"])
def saveConfig():
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        appId = data['app_id']
        key = data.get('key', 'lanying_connector')
        value = data['value']
        if key.startswith('lanying_connector'):
            lanying_config.save_config(appId, key, value)
            resp = app.make_response('success')
            return resp
        else:
            resp = app.make_response('not_allowed')
            return resp
    resp = app.make_response('fail')
    return resp

@app.route("/config", methods=["GET"])
def getConfig():
    showConfigAppId = os.getenv('LANYING_CONNECTOR_SHOW_CONFIG_APP_ID')
    if showConfigAppId:
        config = lanying_config.get_lanying_connector(showConfigAppId)
        resp = app.make_response(json.dumps(config['preset']['messages'], ensure_ascii=False))
        return resp
    resp = app.make_response('')
    return resp

@app.route("/service/<string:service>/buy_message_quota", methods=["POST"])
def buy_message_quota(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        type = data['type']
        value = data['value']
        service_module = importlib.import_module(f"{service}_service")
        result = service_module.buy_message_quota(app_id, type, value)
        if result > 0:
            resp = app.make_response({'code':200, 'data':result})
            return resp
        else:
            resp = app.make_response({'code':400, 'message':'bad request'})
            return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/get_message_limit_state", methods=["POST"])
def get_message_limit_state(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        service_module = importlib.import_module(f"{service}_service")
        result = service_module.get_message_limit_state(app_id)
        resp = app.make_response({'code':200, 'data':result})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

def queryAndSendMessage(data):
    appId = data['appId']
    fromUserId = data['from']['uid']
    toUserId = data['to']['uid']
    content = data['content']
    try:
        service = lanying_config.get_lanying_connector_service(appId)
        if service:
            service_module = importlib.import_module(f"{service}_service")
            config = lanying_config.get_lanying_connector(appId)
            if config:
                newConfig = copy.deepcopy(config)
                newConfig['from_user_id'] = fromUserId
                newConfig['to_user_id'] = toUserId
                newConfig['ext'] = data['ext']
                newConfig['app_id'] = data['appId']
                newConfig['msg_id'] = data['msgId']
                responseText = service_module.handle_chat_message(data, newConfig)
                logging.debug(f"responseText:{responseText}")
                if len(responseText) > 0:
                    sendMessage(appId, toUserId, fromUserId, responseText)
                addMsgSentCnt(1)
    except Exception as e:
        logging.exception(e)
        message_404 = lanying_config.get_message_404(appId)
        sendMessage(appId, toUserId, fromUserId, message_404)
        addMsgSentCnt(1)

def sendMessage(appId, fromUserId, toUserId, content):
    adminToken = lanying_config.get_lanying_admin_token(appId)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1, 'from_user_id':fromUserId,'targets':[toUserId],'content_type':0, 'content': content, 'config': json.dumps({'antispam_prompt':message_antispam}, ensure_ascii=False)})
        logging.debug(sendResponse)

def sendReadAck(appId, fromUserId, toUserId, relatedMid):
    adminToken = lanying_config.get_lanying_admin_token(appId)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1, 'from_user_id':fromUserId,'targets':[toUserId],'content_type':9, 'content': '', 'config': json.dumps({'antispam_prompt':message_antispam}, ensure_ascii=False),'related_mid':relatedMid})
        logging.debug(sendResponse)

def addMsgSentCnt(num):
    redis = lanying_redis.get_redis_connection()
    if redis:
        redis.incrby(msgSentCntKey(), num)

def addMsgReceivedCnt(num):
    redis = lanying_redis.get_redis_connection()
    if redis:
        redis.incrby(msgReceivedCntKey(), num)

def getMsgSentCnt():
    redis = lanying_redis.get_redis_connection()
    if redis:
        str = redis.get(msgSentCntKey())
        if str:
            return int(str)
    return 0

def getMsgReceivedCnt():
    redis = lanying_redis.get_redis_connection()
    if redis:
        str = redis.get(msgReceivedCntKey())
        if str:
            return int(str)
    return 0

def msgSentCntKey():
    return "lanying:connector:msg:sent:cnt"

def msgReceivedCntKey():
    return "lanying:connector:msg:received:cnt"
