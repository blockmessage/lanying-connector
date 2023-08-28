import os
from flask import Flask, Response, request, render_template
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
import socket
import uuid
import lanying_embedding
def init_logging():
    logdir = f"log/{socket.gethostname()}"
    os.makedirs(logdir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(f'{logdir}/info.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)
init_logging()
executor = ThreadPoolExecutor(8)
sys.path.append("services")
lanying_config.init()
accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
def create_app():
    app = Flask(__name__)
    app_upload_dir = '/data/upload/'
    os.makedirs(app_upload_dir, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = app_upload_dir
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    app.config["timeout"] = 120
    if os.environ.get("FLASK_DEBUG"):
        app.debug = True
    return app
app = create_app()
import wechat_official_account_service
app.register_blueprint(wechat_official_account_service.bp)

@app.route("/", methods=["GET"])
def index():
    if lanying_config.is_show_info_page():
        service = lanying_config.get_lanying_connector_service('')
        return render_template("index.html", msgReceivedCnt=getMsgReceivedCnt(), msgSentCnt=getMsgSentCnt(), service=service)
    else:
        resp = app.make_response('')
        return resp

@app.route("/messages", methods=["POST"])
def messages():
    addMsgReceivedCnt(1)
    text = request.get_data(as_text=True)
    data = json.loads(text)
    logging.info(data)
    appId = data['appId']
    headSignature = request.headers.get('signature','')
    service_list = lanying_config.get_service_list()
    for service in service_list:
        config = lanying_config.get_service_config(appId, service)
        if config:
            callbackSignature = config.get('lanying_callback_signature','')
            if callbackSignature == '' or callbackSignature == headSignature:
                logging.info(f'callback signature match: appId={appId}, service={service}')
                executor.submit(handle_lanying_messages, (config, service, data))
            else:
                logging.info(f'callback signature not match: appId={appId}, service={service}')
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
            logging.info(f"update config:appId:{appId}, key:{key}")
            lanying_config.save_config(appId, key, value)
            lanying_embedding.save_app_config(appId, key, value)
            maybeSyncConfig(appId, key, value, accessToken, data.get('sync_all', False))
            resp = app.make_response('success')
            return resp
        else:
            resp = app.make_response('not_allowed')
            return resp
    resp = app.make_response('fail')
    return resp

def maybeSyncConfig(appId, key, value, accessToken, syncAll):
    server = os.getenv("SYNC_ETCD_CONFIG_TO_SERVER", '')
    if len(server) > 0:
        if syncAll:
            for k,v in lanying_config.get_all_config().items():
                now_app_id,now_key = lanying_config.parse_key(k)
                now_value = json.dumps(v, ensure_ascii=False)
                syncConfig(server, now_app_id, now_key, now_value, accessToken)
        syncConfig(server, appId, key, value, accessToken)

def syncConfig(server, appId, key, value, accessToken):
    headers = {
        'access-token': accessToken
    }
    body = {
        'app_id': appId,
        'key': key,
        'value': value
    }
    logging.info(f"sync config to server start: server:{server}, app_id:{appId}, key:{key}")
    url = server + "/config"
    response = requests.post(url, headers=headers, json=body)
    logging.info(f"sync config to server finish: server:{server}, app_id:{appId}, key:{key}, response:{response.text}")

@app.route("/list_models", methods=["POST"])
def list_models():
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        service = "openai"
        service_module = get_service_module(service)
        result = service_module.list_models()
        resp = app.make_response({'code':200, 'data':result})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
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
        service_module = get_service_module(service)
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
        service_module = get_service_module(service)
        result = service_module.get_message_limit_state(app_id)
        resp = app.make_response({'code':200, 'data':result})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp
@app.route("/v1/chat/completions", methods=["POST"])
@app.route("/v1/embeddings", methods=["POST"])
@app.route("/v1/engines/text-embedding-ada-002/embeddings", methods=["POST"])
def openai_request():
    try:
        service = "openai"
        service_module = get_service_module(service)
        res = service_module.handle_request(request)
        if res['result'] == 'error':
            code = res.get('code', 401)
            resp = app.make_response({"error":{"type": "invalid_request_error","code":code, "message":res['msg']},"data":[]})
            return resp
        else:
            response = res['response']
            iter = res.get('iter')
            if iter:
                return Response(iter(), status=response.status_code, headers=response.headers.items())
            else:
                response.headers['Content-Encoding'] = 'identity'
                resp = Response(response.content, status=response.status_code, headers=response.headers.items())
                return resp
    except Exception as e:
        logging.exception(e)
        resp = app.make_response({"error":{"type": "internal_server_error","code":500, "message":"Internal Server Error"}})
        return resp

@app.route("/fetch_embeddings", methods=["POST"])
def embedding_request():
    try:
        service = "openai"
        service_module = get_service_module(service)
        res = service_module.handle_embedding_request(request)
        resp = app.make_response(res)
        return resp
    except Exception as e:
        logging.exception(e)
        resp = app.make_response({"result":"error", "reason":"exception"})
        return resp

@app.route("/trace_finish", methods=["POST"])
def trace_finish():
    try:
        service = "openai"
        service_module = get_service_module(service)
        service_module.trace_finish(request)
        resp = app.make_response({'code':200, 'data':True})
        return resp
    except Exception as e:
        logging.exception(e)
        resp = app.make_response({"result":"error", "reason":"exception"})
        return resp

@app.route("/service/<string:service>/create_embedding", methods=["POST"])
def create_embedding(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        algo = data.get('algo', "COSINE")
        admin_user_ids = data.get('admin_user_ids',[])
        max_block_size = data.get('max_block_size', 500)
        preset_name = data.get('preset_name', '')
        overlapping_size = data.get('overlapping_size', 0)
        vendor = data.get('vendor', 'openai')
        service_module = get_service_module(service)
        result = service_module.create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor)
        if result['result'] == 'error':
            resp = app.make_response({'code':400, 'message':result['message']})
        else:
            resp = app.make_response({'code':200, 'data':result["embedding_uuid"]})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/configure_embedding", methods=["POST"])
def configure_embedding(service):
    logging.info(f"configure_embedding | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        admin_user_ids = data.get('admin_user_ids',[])
        preset_name = data.get('preset_name','')
        embedding_max_tokens = data.get('embedding_max_tokens','2048')
        embedding_max_blocks = data.get('embedding_max_blocks','5')
        embedding_content = data.get('embedding_content', '')
        new_embedding_name = data['new_embedding_name']
        max_block_size = data.get('max_block_size', 0)
        overlapping_size = data.get('overlapping_size', 0)
        vendor = data.get('vendor', 'openai')
        logging.info(f"configure_embedding | {data}")
        service_module = get_service_module(service)
        result = service_module.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor)
        if result['result'] == 'error':
            resp = app.make_response({'code':400, 'message':result['message']})
        else:
            resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/list_embeddings", methods=["POST"])
def list_embeddings(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        service_module = get_service_module(service)
        result = service_module.list_embeddings(app_id)
        resp = app.make_response({'code':200, 'data':{'total':len(result), 'list':result}})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/list_embedding_docs", methods=["POST"])
def list_embedding_docs(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        start = data.get('start', 0)
        end = data.get('end', 20)
        service_module = get_service_module(service)
        total, doc_list = service_module.get_embedding_doc_info_list(app_id, embedding_name, start, end)
        resp = app.make_response({'code':200, 'data':{'total':total, 'list':doc_list}})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/list_embedding_tasks", methods=["POST"])
def list_embedding_tasks(service):
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        logging.info(f"list_embedding_tasks | data:{data}")
        service_module = get_service_module(service)
        task_list = service_module.list_embedding_tasks(app_id, embedding_name)
        resp = app.make_response({'code':200, 'data':{'list':task_list}})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp


@app.route("/service/<string:service>/continue_embedding_task", methods=["POST"])
def continue_embedding_task(service):
    logging.info(f"continue_embedding_task | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        task_id = data['task_id']
        logging.info(f"continue_embedding_task | data:{data}")
        service_module = get_service_module(service)
        service_module.continue_embedding_task(app_id, embedding_name, task_id)
        resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/delete_embedding_task", methods=["POST"])
def delete_embedding_task(service):
    logging.info(f"delete_embedding_task | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        task_id = data['task_id']
        logging.info(f"delete_embedding_task | data:{data}")
        service_module = get_service_module(service)
        service_module.delete_embedding_task(app_id, embedding_name, task_id)
        resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/add_doc_to_embedding", methods=["POST"])
def add_doc_to_embedding(service):
    logging.info(f"add_doc_to_embedding | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        type = data.get('type', 'file')
        if type in ["file", "url", "site"]:
            limit = data.get('limit', -1)
            urls = data.get('urls', [])
            max_depth = data.get('max_depth', 0)
            filters = data.get('filters', [])
            if type == 'url':
                content = data.get('url', '')
                name = 'url.html'
            elif type == 'site':
                content = data.get('url', '')
                name = 'site.html'
                if len(urls) == 0 and len(filters) == 0 : # for old
                    urls.append(content)
                    filters.append(filters)
                    max_depth = 100000000
            else:
                name = data.get('file_name','')
                content = data.get('file_url','')
            logging.info(f"add_doc_to_embedding | {data}")
            service_module = get_service_module(service)
            service_module.add_doc_to_embedding(app_id, embedding_name, name, content, type, limit, max_depth, filters, urls)
        resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/delete_doc_from_embedding", methods=["POST"])
def delete_doc_from_embedding(service):
    logging.info(f"delete_doc_from_embedding | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        doc_id = data['doc_id']
        logging.info(f"delete_doc_from_embedding | {data}")
        service_module = get_service_module(service)
        service_module.delete_doc_from_embedding(app_id, embedding_name, doc_id)
        resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/re_run_doc_to_embedding", methods=["POST"])
def re_run_doc_to_embedding(service):
    logging.info(f"re_run_doc_to_embedding | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        doc_id = data['doc_id']
        logging.info(f"re_run_doc_to_embedding | {data}")
        service_module = get_service_module(service)
        result = service_module.re_run_doc_to_embedding(app_id, embedding_name, doc_id)
        if result['result'] == 'error':
            resp = app.make_response({'code':400, 'message':result['message']})
        else:
            resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/re_run_all_doc_to_embedding", methods=["POST"])
def re_run_all_doc_to_embedding(service):
    logging.info(f"re_run_all_doc_to_embedding | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        embedding_name = data['embedding_name']
        logging.info(f"re_run_all_doc_to_embedding | {data}")
        service_module = get_service_module(service)
        result = service_module.re_run_all_doc_to_embedding(app_id, embedding_name)
        if result['result'] == 'error':
            resp = app.make_response({'code':400, 'message':result['message']})
        else:
            resp = app.make_response({'code':200, 'data':True})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

@app.route("/service/<string:service>/get_embedding_usage", methods=["POST"])
def get_embedding_usage(service):
    logging.info(f"get_embedding_usage | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        logging.info(f"get_embedding_usage | {data}")
        service_module = get_service_module(service)
        data = service_module.get_embedding_usage(app_id)
        resp = app.make_response({'code':200, 'data':data})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp


@app.route("/service/<string:service>/set_embedding_usage", methods=["POST"])
def set_embedding_usage(service):
    logging.info(f"set_embedding_usage | start")
    headerToken = request.headers.get('access-token', "")
    if accessToken and accessToken == headerToken:
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = data['app_id']
        storage_file_size_max = data['storage_file_size_max']
        logging.info(f"set_embedding_usage | {data}")
        service_module = get_service_module(service)
        data = service_module.set_embedding_usage(app_id, storage_file_size_max)
        resp = app.make_response({'code':200, 'data':data})
        return resp
    resp = app.make_response({'code':401, 'message':'bad authorization'})
    return resp

def handle_lanying_messages(data):
    config, service, message = data
    appId = message['appId']
    fromUserId = message['from']['uid']
    toUserId = message['to']['uid']
    try:
        service_module = get_service_module(service)
        newConfig = copy.deepcopy(config)
        newConfig['from_user_id'] = fromUserId
        newConfig['to_user_id'] = toUserId
        newConfig['ext'] = message['ext']
        newConfig['app_id'] = message['appId']
        newConfig['msg_id'] = message['msgId']
        responseText = service_module.handle_chat_message(newConfig, message)
        logging.info(f"handle_lanying_messages | service={service}, appId={appId}, responseText:{responseText}")
        if len(responseText) > 0:
            sendMessageAsync(appId, toUserId, fromUserId, responseText)
        addMsgSentCnt(1)
    except Exception as e:
        logging.exception(e)
        if service == 'openai':
            message_404 = lanying_config.get_message_404(appId)
            sendMessageAsync(appId, toUserId, fromUserId, message_404)
            addMsgSentCnt(1)

def sendMessageAsync(appId, fromUserId, toUserId, content, ext = {}):
    executor.submit(sendMessageAsyncInternal, (appId, fromUserId, toUserId, content, ext))
def sendMessageAsyncInternal(data):
    appId, fromUserId, toUserId, content, ext = data
    sendMessage(appId, fromUserId, toUserId, content, ext)

def sendMessage(appId, fromUserId, toUserId, content, ext = {}):
    adminToken = lanying_config.get_lanying_admin_token(appId)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1,
                                          'from_user_id':fromUserId,
                                          'targets':[toUserId],
                                          'content_type':0,
                                          'content': content, 
                                          'config': json.dumps({'antispam_prompt':message_antispam}, ensure_ascii=False),
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else ''})
        logging.info(f"Send message, from={fromUserId} to={toUserId} content={content}")
        logging.info(sendResponse)

def sendReadAckAsync(appId, fromUserId, toUserId, relatedMid):
    executor.submit(sendReadAckAsyncInternal, (appId, fromUserId, toUserId, relatedMid))

def sendReadAckAsyncInternal(data):
    appId, fromUserId, toUserId, relatedMid = data
    sendReadAck(appId, fromUserId, toUserId, relatedMid)

def sendReadAck(appId, fromUserId, toUserId, relatedMid):
    adminToken = lanying_config.get_lanying_admin_token(appId)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1, 'from_user_id':fromUserId,'targets':[toUserId],'content_type':9, 'content': '', 'config': json.dumps({'antispam_prompt':message_antispam}, ensure_ascii=False),'related_mid':relatedMid})
        logging.info(sendResponse)

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

def get_service_module(service):
    return importlib.import_module(f"{service}_service")
