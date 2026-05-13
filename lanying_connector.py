import os
from flask import Flask, Response, request, render_template
import requests
import logging
import json
import importlib
import sys
import lanying_config
import copy
import time
import lanying_redis
import uuid
import lanying_embedding
from lanying_async import executor
import lanying_logging

lanying_logging.init_logging()
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
import openai_service
app.register_blueprint(openai_service.bp)
import wechat_service
app.register_blueprint(wechat_service.bp)
import grow_ai_service
app.register_blueprint(grow_ai_service.bp)
import bing_search_service
app.register_blueprint(bing_search_service.bp)
import tavily_service
app.register_blueprint(tavily_service.bp)
import openclaw_service
app.register_blueprint(openclaw_service.bp)

def to_openai_error_response(res):
    internal_code = str(res.get('code', 'invalid_request'))
    message = res.get('msg', res.get('message', 'Request failed'))
    message = str(message)
    status_code = 400
    error_type = "invalid_request_error"
    error_code = internal_code

    if internal_code in ['deduct_failed', 'message_per_month_per_user_limit_reached', 'service_is_expired', 'daily_quota_fuse_limit_reached']:
        # Map quota/billing errors to OpenAI-compatible code for better client compatibility.
        status_code = 429
        error_type = "insufficient_quota"
        error_code = "insufficient_quota"
    elif internal_code in ['rate_limit_reached', 'rate_limit_reached_error', 'engine_overloaded_error']:
        status_code = 429
        error_type = "rate_limit_error"
        error_code = "rate_limit_exceeded"
    elif internal_code in ['bad_authorization', 'invalid_api_key']:
        status_code = 401
        error_type = "authentication_error"
        error_code = "invalid_api_key"
    elif internal_code in ['invalid_model', 'model_not_support']:
        status_code = 404
        error_type = "invalid_request_error"
        error_code = "model_not_found"

    payload = {
        "error": {
            "type": error_type,
            "code": error_code,
            "message": message,
            "internal_code": internal_code
        },
        "data": []
    }
    resp = app.make_response(payload)
    resp.status_code = status_code
    return resp


def _mock_openai_response_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:24]}"


def _mock_openai_model_list():
    now = int(time.time())
    return {
        "object": "list",
        "data": [
            {
                "id": "gpt-4o-mini",
                "object": "model",
                "created": now,
                "owned_by": "lanying-mock"
            },
            {
                "id": "mock-gpt",
                "object": "model",
                "created": now,
                "owned_by": "lanying-mock"
            }
        ]
    }


def _mock_openai_reply_text(body):
    model = str(body.get("model", "mock-gpt") or "mock-gpt")
    messages = body.get("messages", [])
    last_user_content = ""
    if isinstance(messages, list):
        for message in reversed(messages):
            if not isinstance(message, dict) or message.get("role") != "user":
                continue
            content = message.get("content", "")
            if isinstance(content, list):
                text_parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(str(item.get("text", "")))
                content = " ".join([x for x in text_parts if x != ""])
            last_user_content = str(content or "").strip()
            break
    if last_user_content == "":
        last_user_content = "hello"
    return f"[mock-openai] model={model}; echo={last_user_content}"


def _mock_openai_unauthorized_response():
    payload = {
        "error": {
            "message": "Incorrect API key provided.",
            "type": "invalid_request_error",
            "param": None,
            "code": "invalid_api_key"
        }
    }
    return Response(json.dumps(payload, ensure_ascii=False), status=401, content_type="application/json; charset=utf-8")


@app.route("/mock/openai/v1/models", methods=["GET"])
def mock_openai_models():
    return Response(json.dumps(_mock_openai_model_list(), ensure_ascii=False), content_type="application/json; charset=utf-8")


@app.route("/mock/openai/v1/chat/completions", methods=["POST"])
def mock_openai_chat_completions():
    auth_header = str(request.headers.get("Authorization", "") or "")
    body = request.get_json(silent=True) or {}
    if auth_header.startswith("Bearer "):
        api_key = auth_header[7:].strip()
        if api_key in ["bad", "invalid", "invalid_api_key", "test-invalid-key"]:
            return _mock_openai_unauthorized_response()
    reply_text = _mock_openai_reply_text(body)
    model = str(body.get("model", "mock-gpt") or "mock-gpt")
    created = int(time.time())
    if body.get("stream") is True:
        def generate():
            first_payload = {
                "id": _mock_openai_response_id("chatcmpl"),
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": reply_text
                        },
                        "finish_reason": None
                    }
                ]
            }
            yield f"data: {json.dumps(first_payload, ensure_ascii=False)}\n\n"
            final_payload = {
                "id": _mock_openai_response_id("chatcmpl"),
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }
                ],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 6,
                    "total_tokens": 11
                }
            }
            yield f"data: {json.dumps(final_payload, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"

        headers = {
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
        return Response(generate(), status=200, headers=headers)

    payload = {
        "id": _mock_openai_response_id("chatcmpl"),
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": reply_text
                },
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 5,
            "completion_tokens": 6,
            "total_tokens": 11
        }
    }
    return Response(json.dumps(payload, ensure_ascii=False), content_type="application/json; charset=utf-8")

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
        text = request.get_data(as_text=True)
        data = json.loads(text)
        app_id = str(data.get('app_id', ''))
        result = service_module.list_models(app_id)
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
        value = data['value']
        service_module = get_service_module(service)
        result = service_module.buy_message_quota(app_id, value)
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
@app.route("/v1/models", methods=["GET"])
@app.route("/v1/embeddings", methods=["POST"])
@app.route("/v1/engines/text-embedding-ada-002/embeddings", methods=["POST"])
@app.route("/v1/images/generations", methods=["POST"])
@app.route("/v1/audio/speech", methods=["POST"])
def openai_request():
    try:
        service = "openai"
        service_module = get_service_module(service)
        if request.path == "/v1/models":
            res = service_module.list_models_openai_api(request)
        else:
            res = service_module.handle_request(request, "json")
        if res['result'] == 'error':
            return to_openai_error_response(res)
        else:
            response = res['response']
            iter = res.get('iter')
            if iter:
                if isinstance(response, dict):
                    headers = {
                        'Content-Type': 'text/event-stream; charset=utf-8',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                    return Response(iter(), status=200, headers=headers)
                else:
                    return Response(iter(), status=response.status_code, headers=response.headers.items())
            else:
                if isinstance(response, dict):
                    response_json = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                    new_response = Response(response_json, content_type="application/json; charset=utf-8")
                    resp = app.make_response(new_response)
                else:
                    response.headers['Content-Encoding'] = 'identity'
                    resp = Response(response.content, status=response.status_code, headers=response.headers.items())
                return resp
    except Exception as e:
        logging.exception(e)
        resp = app.make_response({"error":{"type": "internal_server_error","code":500, "message":"Internal Server Error"}})
        return resp
    
@app.route("/v1/images/edits", methods=["POST"])
@app.route("/v1/images/variations", methods=["POST"])
@app.route("/v1/audio/transcriptions", methods=["POST"])
def openai_form_request():
    try:
        service = "openai"
        service_module = get_service_module(service)
        res = service_module.handle_request(request, "form")
        if res['result'] == 'error':
            return to_openai_error_response(res)
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
        model = data.get('model', '')
        service_module = get_service_module(service)
        result = service_module.create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor, model)
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
        vendor = str(data.get('vendor', 'openai'))
        model = str(data.get('model', ''))
        tags = data.get('tags', [])
        logging.info(f"configure_embedding | {data}")
        service_module = get_service_module(service)
        result = service_module.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor, model, tags)
        if result['result'] == 'error':
            resp = app.make_response({'code':400, 'message':result['message']})
        else:
            resp = app.make_response({'code':200, 'data': result['data']})
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
        raw_tags = data.get('tags', {})
        tags = {}
        if isinstance(raw_tags, dict):
            for tag_name,tag_value in raw_tags.items():
                if isinstance(tag_name, str) and isinstance(tag_value, str):
                    tags[tag_name] = tag_value
                else:
                    resp = app.make_response({'code':400, 'message':'tag name and value must be string'})
                    return resp
        else:
            resp = app.make_response({'code':400, 'message':'tags must be string dict'})
            return resp
        generate_lanying_links = data.get('generate_lanying_links', False)
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
            logging.info(f"add_doc_to_embedding | {data}, tags:{tags}")
            service_module = get_service_module(service)
            service_module.add_doc_to_embedding(app_id, embedding_name, name, content, type, limit, max_depth, filters, urls, generate_lanying_links, tags)
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
        service_module.handle_chat_message(newConfig, message)
        addMsgSentCnt(1)
    except Exception as e:
        logging.exception(e)

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
        logging.info(f"Send message, from={fromUserId} to={toUserId} content={content}, ext:{ext}")
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1,
                                          'from_user_id':fromUserId,
                                          'targets':[toUserId],
                                          'content_type':0,
                                          'content': content, 
                                          'config': json.dumps({'antispam_prompt':message_antispam}, ensure_ascii=False),
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else ''})
        logging.info(sendResponse)
        try:
            res = sendResponse.json()
            if 'msg_ids' in res:
                msg_ids = res['msg_ids']
                if len(msg_ids) > 0:
                    return msg_ids[0]
        except Exception as e:
            pass
        return 0

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

def sendMessageOperAsync(appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    executor.submit(sendMessageOperAsyncInternal, (appId, fromUserId, toUserId, relatedMid, ctype, content, ext, msg_config, online_only))

def sendMessageOperAsyncInternal(data):
    appId, fromUserId, toUserId, relatedMid, ctype, content, ext, msg_config, online_only = data
    sendMessageOper(appId, fromUserId, toUserId, relatedMid, ctype, content, ext, msg_config, online_only)

def sendMessageOper(appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    adminToken = lanying_config.get_lanying_admin_token(appId)
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        logging.info(f"Send message oper, from={fromUserId} to={toUserId} ctype={ctype}, content={content}, ext:{ext}, msg_config:{msg_config}, online_only:{online_only}")
        msg_config['antispam_prompt'] = message_antispam
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1,
                                          'from_user_id':fromUserId,
                                          'targets':[toUserId],
                                          'content_type':ctype,
                                          'content': content,
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else '',
                                          'config': json.dumps(msg_config, ensure_ascii=False),
                                          'related_mid':relatedMid,
                                          'online_only': online_only})
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
