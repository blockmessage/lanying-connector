import uuid
import openai
import time
import logging
import lanying_connector
import json
import tiktoken
import lanying_config
import lanying_redis
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import os
import copy
from lanying_tasks import add_embedding_file, delete_doc_data, re_run_doc_to_embedding_by_doc_ids, prepare_site,continue_site_task
import lanying_embedding
import re
import lanying_command
import lanying_url_loader
import lanying_vendor
import lanying_utils
from flask import Blueprint, request, make_response
import lanying_ai_plugin
import random
import lanying_file_storage
import lanying_chatbot
import lanying_ai_capsule
import lanying_im_api
from requests.auth import HTTPDigestAuth
from requests.auth import HTTPBasicAuth
import lanying_message
from urllib.parse import urlparse
import uuid
from pydub import AudioSegment
import math
from lanying_async import executor

service = 'openai_service'
bp = Blueprint(service, __name__)

global_lanying_connector_server = os.getenv("EMBEDDING_LANYING_CONNECTOR_SERVER", "https://lanying-connector.lanyingim.com")

expireSeconds = 86400 * 3
presetNameExpireSeconds = 86400 * 3
using_embedding_expire_seconds = 86400 * 3
maxUserHistoryLen = 20
MaxTotalTokens = 4000

def handle_embedding_request(request):
    auth_result = check_embedding_authorization(request)
    if auth_result['result'] == 'error':
        logging.info(f"check_authorization deny, msg={auth_result['msg']}")
        return auth_result
    app_id = auth_result['app_id']
    config = auth_result['config']
    rate_res = check_message_rate(app_id, request.path)
    if rate_res['result'] == 'error':
        logging.info(f"check_message_rate deny: app_id={app_id}, msg={rate_res['msg']}")
        return rate_res
    deduct_res = check_message_deduct_failed(app_id, config)
    if deduct_res['result'] == 'error':
        logging.info(f"check_message_deduct_failed deny: app_id={app_id}, msg={deduct_res['msg']}")
        return deduct_res
    request_text = request.get_data(as_text=True)
    data = json.loads(request_text)
    vendor = data.get('vendor')
    model = data.get('model', '')
    text = data.get('text')
    limit_res = check_message_limit(app_id, config, vendor, False)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}, vendor={vendor}, model={model}")
    auth_info = get_preset_auth_info(config, openai_key_type, vendor)
    prepare_info = lanying_vendor.prepare_embedding(vendor,auth_info, 'db')
    response = lanying_vendor.embedding(vendor, prepare_info, model, text)
    return response

def trace_finish(request):
    auth_result = check_embedding_authorization(request)
    if auth_result['result'] == 'error':
        logging.info(f"check_authorization deny, msg={auth_result['msg']}")
        return auth_result
    app_id = auth_result['app_id']
    config = auth_result['config']
    request_text = request.get_data(as_text=True)
    data = json.loads(request_text)
    trace_id = data.get('trace_id')
    status = data.get('status')
    message = data.get('message', '')
    doc_id = data.get('doc_id','')
    embedding_name = data.get('embedding_name', '')
    notify_user = lanying_embedding.get_trace_field(trace_id, "notify_user")
    if notify_user:
        notify_from = int(lanying_embedding.get_trace_field(trace_id, "notify_from"))
        lanying_embedding.delete_trace_field(trace_id, "notify_user")
        user_id = int(notify_user)
        if status == "success":
            sendMessageAsync(app_id, notify_from, user_id, f"文章（ID：{doc_id}）已加入知识库 {embedding_name}，有用的知识又增加了，谢谢您 ♪(･ω･)ﾉ",{'ai':{'role': 'ai'}})
        else:
            sendMessageAsync(app_id, notify_from, user_id, f"文章（ID：{doc_id}）加入知识库 {embedding_name}失败：{message}",{'ai':{'role': 'ai'}})

def handle_request(request, request_type):
    path = request.path
    if request_type == 'json':
        text = request.get_data(as_text=True)
        logging.info(f"receive api json request: path: {path}, text:{text}")
        preset = json.loads(text)
    else:
        preset = {}
        if path == "/v1/audio/transcriptions":
            for key in ["model", "language", "prompt", "response_format", "temperature", "timestamp_granularities"]:
                if key in request.form:
                    preset[key] = request.form[key]
        else:
            for key in ["prompt", "model", "n", "size", "response_format", "user"]:
                if key in request.form:
                    preset[key] = request.form[key]
        logging.info(f"receive api form request: path: {path}, preset:{preset}")
    auth_result = check_authorization(request)
    if auth_result['result'] == 'error':
        logging.info(f"check_authorization deny, msg={auth_result['msg']}")
        return auth_result
    app_id = auth_result['app_id']
    config = copy.deepcopy(auth_result['config'])
    rate_res = check_message_rate(app_id, request.path)
    if rate_res['result'] == 'error':
        logging.info(f"check_message_rate deny: app_id={app_id}, msg={rate_res['msg']}")
        return rate_res
    deduct_res = check_message_deduct_failed(app_id, config)
    if deduct_res['result'] == 'error':
        logging.info(f"check_message_deduct_failed deny: app_id={app_id}, msg={deduct_res['msg']}")
        return deduct_res
    maybe_init_preset_default_model(preset, path)
    vendor = request.headers.get('vendor')
    model = preset['model']
    model_config = lanying_vendor.get_chat_model_config(vendor, model)
    force_no_stream = False
    forward_file_info = None
    if model_config is None:
        vendor = 'openai'
        model_config = lanying_vendor.get_embedding_model_config(vendor, model)
    if model_config is None:
        vendor = 'openai'
        model_config = lanying_vendor.get_image_model_config(vendor, model)
        if model_config is not None:
            image_quota_res = check_image_quota(model_config, preset)
            if image_quota_res['result'] == 'error':
                logging.info(f"handle_openai_request failed with:{image_quota_res}")
                return {'result':'error', 'msg':'The size is not supported by this model.', 'code':'invalid_size'}
            else:
                logging.info(f"handle_openai_request image_quota_res:{image_quota_res}")
                model_config['quota'] = image_quota_res['quota']
                model_config['image_summary'] = image_quota_res['image_summary']
                config['quota_pre_check'] = image_quota_res['quota']
                force_no_stream = True
    if model_config is None and path == '/v1/audio/speech':
        vendor = 'openai'
        model_config = lanying_vendor.get_text_to_speech_model_config(vendor, model)
        if model_config is not None:
            quota_res = check_text_to_speech_quota(model_config, preset)
            if quota_res['result'] == 'error':
                logging.info(f"handle_openai_request text_to_speech failed with:{quota_res}")
                return {'result':'error', 'msg': quota_res['message'], 'code':quota_res['code']}
            else:
                logging.info(f"handle_openai_request text_to_speech quota_res:{quota_res}")
                model_config['quota'] = quota_res['quota']
                config['quota_pre_check'] = quota_res['quota']
                force_no_stream = True
    if model_config is None and path == '/v1/audio/transcriptions':
        vendor = 'openai'
        model_config = lanying_vendor.get_speech_to_text_model_config(vendor, model)
        if model_config is not None:
            quota_res = check_speech_to_text_quota(model_config, preset, request)
            if quota_res['result'] == 'error':
                logging.info(f"handle_openai_request speech_to_text failed with:{quota_res}")
                return {'result':'error', 'msg': quota_res['message'], 'code':quota_res['code']}
            else:
                logging.info(f"handle_openai_request speech_to_text quota_res:{quota_res}")
                model_config['quota'] = quota_res['quota']
                config['quota_pre_check'] = quota_res['quota']
                config['speech_to_text_duration'] = quota_res['duration']
                forward_file_info = quota_res['file_info']
                force_no_stream = True
    if model_config is None:
        return {'result':'error', 'msg':'model not exist.', 'code':'invalid_model'}
    vendor = model_config['vendor']
    model_res = check_model_allow(model_config, model)
    if model_res['result'] == 'error':
        logging.info(f"check_model_allow deny: app_id={app_id}, msg={model_res['msg']}")
        return model_res
    limit_res = check_message_limit(app_id, config, vendor, False)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}, vendor={vendor}, model:{model}")
    auth_info = get_preset_auth_info(config, openai_key_type, vendor)
    if vendor == 'openai':
        stream,response = forward_request(app_id, request, auth_info, force_no_stream, request_type, forward_file_info)
        if response.status_code == 200:
            if stream:
                def generate_response():
                    contents = []
                    try:
                        for line in response.iter_lines():
                            line_str = line.decode('utf-8')
                            # logging.info(f"stream got line:{line_str}|")
                            if line_str.startswith('data:'):
                                try:
                                    data = json.loads(line_str[5:])
                                    content = data['choices'][0]['delta']['content']
                                    if content is not None:
                                        contents.append(content)
                                except Exception as e:
                                    pass
                            yield line_str + '\n'
                    finally:
                        reply = ''.join(contents)
                        response_json = stream_lines_to_response(preset, reply, vendor, {}, "", "")
                        logging.info(f"forward request: stream response | status_code: {response.status_code}, response_json:{response_json}")
                        add_message_statistic(app_id, config, preset, response_json, openai_key_type, model_config)
                return {'result':'ok', 'response':response, 'iter': generate_response}
            else:
                if path == '/v1/audio/speech':
                    logging.info(f"forward request: not stream response , got file response| status_code: {response.status_code}")
                    response_content = {}
                else:
                    logging.info(f"forward request: not stream response | status_code: {response.status_code}, response_content:{response.content}")
                    response_content = json.loads(response.content)
                add_message_statistic(app_id, config, preset, response_content, openai_key_type, model_config)
        else:
            logging.info(f"forward request: bad response | status_code: {response.status_code}, response_content:{response.content}")
        return {'result':'ok', 'response':response}
    else:
        prepare_info = lanying_vendor.prepare_chat(vendor, auth_info, preset)
        response = lanying_vendor.chat(vendor, prepare_info, preset)
        stream = 'reply_generator' in response
        logging.info(f"forward request other vendor: vendor:{vendor}, stream:{stream}, response:{response}")
        if response.get('result', '') == 'error':
            reason = response.get('reason', '')
            if reason == '':
                reason =  response.get('code', '')
            logging.info(f"forward request reply error: vendor:{vendor}, stream:{stream}, reason:{reason}, response:{response}")
            return {'result':'error', 'msg':reason, 'code': reason}
        if stream:
            def generate_response():
                contents = []
                id = f'chatcmpl-{int(time.time()*1000000)}{random.randint(1,100000000)}'
                created = int(time.time())
                usage = {}
                role_count = 0
                try:
                    reply_generator = response.get('reply_generator')
                    for delta in reply_generator:
                        logging.info(f"forward request other vendor: vendor:{vendor}, delta:{delta}")
                        if 'content' in delta:
                            content = delta['content']
                        else:
                            content = ''
                        contents.append(content)
                        if 'usage' in delta:
                            usage = delta['usage']
                        delta_response = {
                            'id': id,
                            'object': 'chat.completion.chunk',
                            'created': created,
                            'model': model,
                            'choices': [
                                {
                                    'index': 0,
                                    'delta':{
                                        'content': content
                                    }
                                }
                            ]
                        }
                        if role_count == 0:
                            role_count += 1
                            delta_response['choices'][0]['delta']['role'] = 'assistant'
                        if 'function_call' in delta:
                            delta_response['choices'][0]['delta']['function_call'] = delta.get('function_call')
                        delta_line = f"data: {json.dumps(delta_response, ensure_ascii=False)}\n"
                        logging.info(f"delta_line:{delta_line}")
                        yield delta_line
                finally:
                    delta_response = {
                            'id': id,
                            'object': 'chat.completion.chunk',
                            'created': created,
                            'model': model,
                            'choices': [
                                {
                                    'index': 0,
                                    'delta':{
                                    },
                                    'finish_reason': 'stop'
                                }
                            ],
                            'usage': usage
                        }
                    delta_line = f"data: {json.dumps(delta_response, ensure_ascii=False)}\n"
                    logging.info(f"delta_line:{delta_line}")
                    yield delta_line
                    yield 'data: [DONE]\n'
                    reply = ''.join(contents)
                    response_json = stream_lines_to_response(preset, reply, vendor, usage, "", "")
                    logging.info(f"forward request: stream response | response: {response}, response_json:{response_json}")
                    add_message_statistic(app_id, config, preset, response_json, openai_key_type, model_config)
            return {'result':'ok', 'response':response, 'iter': generate_response}
        else:
            response_body = {
                'id': f'chatcmpl-{int(time.time()*1000000)}{random.randint(1,100000000)}',
                'object': 'chat.completion',
                'created': int(time.time()),
                'vendor': vendor,
                'model': model,
                'choices': [
                    {
                        'index': 0,
                        'message':{
                            'role': 'assistant',
                            'content': response.get('reply', '')
                        },
                        'finish_reason': 'stop'
                    }
                ]
            }
            if 'usage' in response:
                response_body['usage'] = response.get('usage')
            if 'function_call' in response and response.get('function_call') is not None:
                response_body['choices'][0]['message']['function_call'] = response.get('function_call')
                response_body['choices'][0]['finish_reason'] = 'function_call'
            add_message_statistic(app_id, config, preset, response, openai_key_type, model_config)
            return {'result':'ok', 'response':response_body}

def maybe_init_preset_default_model(preset, path):
    if path == '/v1/engines/text-embedding-ada-002/embeddings':
        preset['model'] = 'text-embedding-ada-002'
    elif 'model' not in preset:
        if path in ['/v1/images/generations', '/v1/images/edits', '/v1/images/variations']:
            preset['model'] = 'dall-e-2'

def forward_request(app_id, request, auth_info, force_no_stream, request_type, forward_file_info):
    openai_key = auth_info.get('api_key','')
    proxy_domain = os.getenv('LANYING_CONNECTOR_OPENAI_PROXY_DOMAIN', '')
    if len(proxy_domain) > 0:
        proxy_api_key = os.getenv("LANYING_CONNECTOR_OPENAI_PROXY_API_KEY", '')
        url = proxy_domain + request.path
        headers = {"Authorization-Next":"Bearer " + openai_key,  "Authorization":"Basic " + proxy_api_key}
    else:
        url = "https://api.openai.com" + request.path
        headers = {"Authorization":"Bearer " + openai_key}
    if request_type == 'json':
        headers['Content-Type'] = "application/json"
        data = request.get_data()
        request_json = json.loads(data)
        stream = request_json.get('stream', False)
        if force_no_stream:
            stream = False
        if stream:
            logging.info(f"forward request stream start: app_id:{app_id}, url:{url}")
            response = requests.post(url, data=data, headers=headers, stream=True)
            logging.info(f"forward request stream finish: app_id:{app_id}, status_code: {response.status_code}")
            return (stream, response)
        else:
            logging.info(f"forward request start: app_id:{app_id}, url:{url}")
            response = requests.post(url, data=data, headers=headers)
            logging.info(f"forward request finish: app_id:{app_id}, status_code: {response.status_code}")
            return (stream, response)
    else:
        form_data = request.form
        files = {}
        if forward_file_info:
            logging.info(f"file_key from forward_file_info :{forward_file_info['filename']}, {forward_file_info['mimetype']}, {forward_file_info['path']}")
            with open(forward_file_info['path'], 'rb') as file_stream:
                files['file'] = (forward_file_info['filename'], file_stream, forward_file_info['mimetype'])
                logging.info(f"forward form request start: app_id:{app_id}, url:{url}")
                response = requests.post(url, data=form_data, files=files, headers=headers)
                logging.info(f"forward form request finish: app_id:{app_id}, status_code: {response.status_code}")
        else:
            for file_key in request.files:
                file = request.files[file_key]
                logging.info(f"file_key:{file_key}, {file.filename}, {file.mimetype}, {file.stream}")
                files[file_key] = (file.filename, file.stream, file.mimetype)
            logging.info(f"forward form request start: app_id:{app_id}, url:{url}")
            response = requests.post(url, data=form_data, files=files, headers=headers)
            logging.info(f"forward form request finish: app_id:{app_id}, status_code: {response.status_code}")
        stream = False
        return (stream, response)

def check_authorization(request):
    try:
        authorization = request.headers.get('Authorization')
        if authorization:
            bearer_token = str(authorization)
            prefix = "Bearer "
            if bearer_token.startswith(prefix):
                token = bearer_token[len(prefix):]
                tokens = token.split("-")
                if len(tokens) == 3:
                    app_id = tokens[0]
                    config = lanying_config.get_lanying_connector(app_id)
                    if config:
                        if token == config.get('access_token', ''):
                            return {'result':'ok', 'app_id':app_id, 'config':config}
    except Exception as e:
        logging.exception(e)
    return {'result':'error', 'msg':'bad_authorization', 'code':'bad_authorization'}

def check_embedding_authorization(request):
    try:
        authorization = request.headers.get('Authorization')
        if authorization:
            bearer_token = str(authorization)
            prefix = "Bearer "
            if bearer_token.startswith(prefix):
                token = bearer_token[len(prefix):]
                tokens = token.split("-")
                if len(tokens) == 2:
                    auth_secret = lanying_config.get_embedding_auth_secret()
                    if auth_secret == tokens[1]:
                        app_id = tokens[0]
                        config = lanying_config.get_lanying_connector(app_id)
                        if config:
                            return {'result':'ok', 'app_id':app_id, 'config':config}
    except Exception as e:
        logging.exception(e)
    return {'result':'error', 'msg':'bad_authorization', 'code':'bad_authorization'}

def init_chatbot_config(config, msg):
    app_id = msg['appId']
    check_res = check_message_chatbot_id(config, msg)
    if check_res['result'] == 'ok':
        chatbot_user_id = check_res['chatbot_user_id']
        chatbot_id = lanying_chatbot.get_user_chatbot_id(app_id, chatbot_user_id)
        chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot:
            config['chatbot'] = chatbot
            for key in ["history_msg_count_max", "history_msg_count_min","history_msg_size_max",
                        "message_per_month_per_user", "linked_capsule_id", "linked_publish_capsule_id",
                        "quota_exceed_reply_type","quota_exceed_reply_msg", "chatbot_id", "group_history_use_mode",
                        "audio_to_text", "image_vision", "audio_to_text_model"]:
                if key in chatbot:
                    config[key] = chatbot[key]
        else:
            logging.warning(f"cannot get chatbot info: app_id={app_id}, chatbot_user_id:{chatbot_user_id}, chatbot_id:{chatbot_id}")

def check_message_chatbot_id(config, msg):
    app_id = msg['appId']
    from_user_id = str(msg['from']['uid'])
    to_user_id = str(msg['to']['uid'])
    msg_type = msg['type']
    if msg_type == 'CHAT':
        is_chatbot = is_chatbot_user_id(app_id, to_user_id, config)
        if is_chatbot:
            return {'result': 'ok', 'chatbot_user_id': to_user_id}
    elif msg_type == 'GROUPCHAT':
        group_id = to_user_id
        msg_config = lanying_utils.safe_json_loads(msg.get('config'))
        chatbot_user_id = find_chatbot_user_id_in_group_mention(config, app_id, group_id, from_user_id, msg_config)
        if chatbot_user_id:
            return {'result': 'ok', 'chatbot_user_id': to_user_id}
    return {'result': 'error', 'message': 'no chatbot'}

def check_message_need_reply(config, msg):
    fromUserId = str(msg['from']['uid'])
    toUserId = str(msg['to']['uid'])
    app_id = msg['appId']
    msg_type = msg['type']
    if msg_type == 'CHAT':
        is_chatbot = is_chatbot_user_id(app_id, toUserId, config)
        if is_chatbot and fromUserId != toUserId:
            config['reply_from'] = toUserId
            config['reply_to'] = fromUserId
            config['send_from'] = fromUserId
            config['send_to'] = toUserId
            config['reply_msg_type'] = 'CHAT'
            config['request_msg_id'] = msg['msgId']
            try:
                ext = json.loads(msg['ext'])
                if ext.get('ai',{}).get('role', 'none') == 'ai':
                    logging.info(f"hard stop message reply for ai role | msg:{msg}")
                    return {'result':'error', 'msg':''} # message is from ai
            except Exception as e:
                pass
            try:
                content = msg['content']
                round = get_ai_message_round(fromUserId, toUserId, content)
                logging.info(f"check_message_need_reply round | app_id:{app_id}, fromUserId:{fromUserId}, toUserId:{toUserId}, round:{round}")
                limit = 3
                if round == limit:
                    logging.info(f"soft stop message reply for round limit reached | round:{round}/{limit}, msg:{msg}")
                    return {'result':'error', 'msg':'您发消息速度过快，请稍后再试。'}
                elif round > limit:
                    logging.info(f"hard stop message reply for round limit reached | round:{round}/{limit}, msg:{msg}")
                    return {'result':'error', 'msg':''}
            except Exception as e:
                pass
            return {'result':'ok', 'chatbot_user_id': toUserId}
    elif msg_type == 'GROUPCHAT':
        group_id = toUserId
        msg_config = lanying_utils.safe_json_loads(msg.get('config'))
        chatbot_user_id = find_chatbot_user_id_in_group_mention(config, app_id, group_id, fromUserId, msg_config)
        if chatbot_user_id:
            config['reply_from'] = chatbot_user_id
            config['reply_to'] = group_id
            config['send_from'] = fromUserId
            config['send_to'] = group_id
            config['reply_msg_type'] = 'GROUPCHAT'
            config['request_msg_id'] = msg['msgId']
            try:
                ext = json.loads(msg['ext'])
                if ext.get('ai',{}).get('role', 'none') == 'ai':
                    logging.info(f"hard stop group message reply for ai role | msg:{msg}")
                    return {'result':'error', 'msg':''} # message is from ai
            except Exception as e:
                pass
            try:
                content = msg['content']
                round = get_ai_message_round(0, group_id, content)
                logging.info(f"check_message_need_reply round | app_id:{app_id}, groupId:{group_id}, round:{round}")
                limit = 3
                if round == limit:
                    logging.info(f"soft stop group message reply for round limit reached | round:{round}/{limit}, msg:{msg}")
                    return {'result':'error', 'msg':'您发消息速度过快，请稍后再试。'}
                elif round > limit:
                    logging.info(f"hard stop group message reply for round limit reached | round:{round}/{limit}, msg:{msg}")
                    return {'result':'error', 'msg':''}
            except Exception as e:
                pass
            return {'result':'ok', 'chatbot_user_id': chatbot_user_id}
    return {'result':'error', 'msg':''}

def find_chatbot_user_id_in_group_mention(config, app_id, group_id, fromUserId, msg_config):
    mention_list = msg_config.get('mentionList', [])
    for user_id in mention_list:
        user_id = str(user_id)
        if fromUserId != user_id and is_chatbot_user_id(app_id, user_id, config):
            return user_id
    return None

def handle_sync_messages(config, msg):
    set_sync_mode(config)
    try:
        handle_chat_message(config, msg)
    except Exception as e:
        logging.exception(e)
    return get_sync_mode_messages(config)

def handle_chat_message(config, msg):
    app_id = msg['appId']
    msg_type = msg['type']
    if msg_type not in ["CHAT", "GROUPCHAT"]:
        return ''
    try:
        init_chatbot_config(config, msg)
        maybe_transcription_audio_msg(config, msg)
        maybe_add_history(config, msg)
        reply = handle_chat_message_try(config, msg, 3)
    except Exception as e:
        logging.error("fail to handle_chat_message:")
        logging.exception(e)
        reply = lanying_config.get_message_404(app_id)
    if isinstance(reply, list):
        reply_list = reply
    else:
        reply_list = [reply]
    cnt = 0
    for now_reply in reply_list:
        if len(now_reply) > 0:
            cnt += 1
            lcExt = {}
            try:
                ext = json.loads(config['ext'])
                if 'ai' in ext:
                    lcExt = ext['ai']
                elif 'lanying_connector' in ext:
                    lcExt = ext['lanying_connector']
            except Exception as e:
                pass
            reply_ext = {
                'ai': {
                    'stream': False,
                    'role': 'ai',
                    'result': 'error'
                }
            }
            if 'feedback' in lcExt:
                reply_ext['ai']['feedback'] = lcExt['feedback']
            replyMessageAsync(config, now_reply, reply_ext)
            if cnt == 1 and msg_type == 'GROUPCHAT' and 'reply_msg_type' in config:
                logging.info(f"ADD HISTORY CONFIG:{config}")
                now = int(time.time())
                redis = lanying_redis.get_redis_connection()
                history = {'time':now}
                history['type'] = 'group'
                history['content'] = now_reply
                history['group_id'] = config['reply_to']
                history['from'] =  config['reply_from']
                if 'send_from' in config:
                    history['mention_list'] = [int(config['send_from'])]
                historyListKey = historyListGroupKey(app_id, config['reply_to'])
                addHistory(redis, historyListKey, history)
            if len(reply_list) > 0:
                time.sleep(0.5)

def handle_chat_message_try(config, msg, retry_times):
    app_id = msg['appId']
    msg_type = msg['type']
    checkres = check_message_need_reply(config, msg)
    if checkres['result'] == 'error':
        return checkres['msg']
    chatbot_user_id = checkres['chatbot_user_id']
    config['chatbot_user_id'] = chatbot_user_id
    reply_message_read_ack(config, msg)
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    preset = copy.deepcopy(config.get('preset',{}))
    is_chatbot_mode = lanying_chatbot.is_chatbot_mode(app_id)
    checkres = check_message_deduct_failed(msg['appId'], config)
    if checkres['result'] == 'error':
        return checkres['msg']
    checkres = check_product_id(msg['appId'], config)
    if checkres['result'] == 'error':
        return checkres['msg']
    ctype = msg['ctype']
    content = msg.get('content','')
    command_ext = {}
    if msg_type == 'CHAT':
        if ctype == 'TEXT':
            if content.startswith("/"):
                result = handle_embedding_command(msg, config)
                if isinstance(result, str):
                    if len(result) > 0:
                        return result
                elif result["result"] == "continue":
                    command_ext = result["command_ext"]
            elif content.startswith('http://') or content.startswith('https://'):
                result = handle_chat_links(msg, config)
                if len(result) > 0:
                    return result
        elif ctype == 'FILE':
            return handle_chat_file(msg, config)
        elif ctype == 'AUDIO':
            if content == '':
                return '对不起，我无法处理语音消息。'
        else:
            return ''
    else:
        if ctype == 'AUDIO':
            if content == '':
                return '对不起，我无法处理语音消息。'
        elif ctype != 'TEXT':
            return ''
    if is_chatbot_mode:
        chatbot = config.get('chatbot')
        if chatbot:
            preset = chatbot['preset']
    checkres = check_message_per_month_per_user(msg, config)
    if checkres['result'] == 'error':
        return checkres['msg']
    lcExt = {}
    presetExt = {}
    redis = lanying_redis.get_redis_connection()
    try:
        ext = json.loads(config['ext'])
        if 'ai' in ext:
            lcExt = ext['ai']
        elif 'lanying_connector' in ext:
            lcExt = ext['lanying_connector']
    except Exception as e:
        pass
    preset_name = ""
    if preset_name == "":
        try:
            if "preset_name" in command_ext:
                if command_ext['preset_name'] != "default":
                    if is_chatbot_mode:
                        sub_chatbot = lanying_chatbot.get_chatbot_by_name(app_id, chatbot['chatbot_ids'], command_ext['preset_name'])
                        if sub_chatbot:
                            chatbot = sub_chatbot
                            preset = sub_chatbot['preset']
                            preset_name = command_ext['preset_name']
                            logging.info(f"using preset_name from command:{preset_name}")
                    else:
                        preset = preset['presets'][command_ext['preset_name']]
                        preset_name = command_ext['preset_name']
                        logging.info(f"using preset_name from command:{preset_name}")
        except Exception as e:
            logging.exception(e)
    if preset_name == "":
        try:
            if 'preset_name' in lcExt:
                if lcExt['preset_name'] != "default":
                    if is_chatbot_mode:
                        sub_chatbot = lanying_chatbot.get_chatbot_by_name(app_id, chatbot['chatbot_ids'], lcExt['preset_name'])
                        if sub_chatbot:
                            chatbot = sub_chatbot
                            preset = sub_chatbot['preset']
                            preset_name = lcExt['preset_name']
                            logging.info(f"using preset_name from lc_ext:{preset_name}")
                    else:
                        preset = preset['presets'][lcExt['preset_name']]
                        preset_name = lcExt['preset_name']
                        logging.info(f"using preset_name from lc_ext:{preset_name}")
        except Exception as e:
            logging.exception(e)
    if preset_name == "":
        lastChoosePresetName = get_preset_name(redis, fromUserId, toUserId)
        logging.info(f"lastChoosePresetName:{lastChoosePresetName}")
        if lastChoosePresetName:
            try:
                if lastChoosePresetName != "default":
                    if is_chatbot_mode:
                        sub_chatbot = lanying_chatbot.get_chatbot_by_name(app_id, chatbot['chatbot_ids'], lastChoosePresetName)
                        if sub_chatbot:
                            chatbot = sub_chatbot
                            preset = json.loads(sub_chatbot['preset'])
                            preset_name = lastChoosePresetName
                            logging.info(f"using preset_name from last_choose_preset:{preset_name}")
                    else:
                        preset = preset['presets'][lastChoosePresetName]
                        preset_name = lastChoosePresetName
                        logging.info(f"using preset_name from last_choose_preset:{preset_name}")
            except Exception as e:
                logging.exception(e)
    if preset_name == "":
        if is_chatbot_mode:
            preset_name = chatbot['name']
        else:
            preset_name = "default"
    if 'presets' in preset:
        del preset['presets']
    if 'ext' in preset:
        presetExt = copy.deepcopy(preset['ext'])
        del preset['ext']
    is_debug = 'debug' in presetExt and presetExt['debug'] == True
    if is_debug:
        replyMessageAsync(config, f"[蓝莺AI] 当前预设为: {preset_name}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
    logging.info(f"lanying-connector:ext={json.dumps(lcExt, ensure_ascii=False)},presetExt:{presetExt}")
    vendor = config.get('vendor', 'openai')
    if 'vendor' in preset:
        vendor = preset['vendor']
    model_config = lanying_vendor.get_chat_model_config(vendor, preset['model'])
    if model_config:
        return handle_chat_message_with_config(config, model_config, vendor, msg, preset, lcExt, presetExt, preset_name, command_ext, retry_times)
    else:
        return f'不支持模型：{preset["model"]}'

def handle_chat_message_with_config(config, model_config, vendor, msg, preset, lcExt, presetExt, preset_name, command_ext, retry_times):
    app_id = msg['appId']
    ctype = msg.get('ctype', '')
    model = preset['model']
    check_res = check_message_limit(app_id, config, vendor, True)
    if check_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={check_res['msg']}")
        return check_res['msg']
    openai_key_type = check_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    doc_id = ""
    is_fulldoc = False
    content = msg['content']
    if 'new_content' in command_ext:
        content = command_ext['new_content']
        logging.info(f"using content in command:{content}")
    if doc_id == "" and 'doc_id' in command_ext:
        doc_id = command_ext['doc_id']
        is_fulldoc = command_ext.get('is_fulldoc', False)
        logging.info(f"using doc_id in command:{doc_id}, is_fulldoc:{is_fulldoc}")
    auth_info = get_preset_auth_info(config, openai_key_type, vendor)
    prepare_info = lanying_vendor.prepare_chat(vendor, auth_info, preset)
    add_reference = presetExt.get('add_reference', 'none')
    reference = presetExt.get('reference')
    reference_list = []
    messages = preset.get('messages',[])
    user_functions = []
    function_names = {}
    system_functions = get_system_functions(config, presetExt)
    for function_info in system_functions:
        if 'name' in function_info:
            function_names[function_info['name']] = function_info['name']
    now = int(time.time())
    history = {'time':now}
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    msg_type = msg['type']
    if msg_type == 'CHAT':
        historyListKey = historyListChatGPTKey(app_id, fromUserId, toUserId)
    elif msg_type == 'GROUPCHAT':
        historyListKey = historyListGroupKey(app_id, toUserId)
    redis = lanying_redis.get_redis_connection()
    is_debug = 'debug' in presetExt and presetExt['debug'] == True
    if 'reset_prompt' in lcExt and lcExt['reset_prompt'] == True:
        removeAllHistory(redis, historyListKey)
        del_preset_name(redis, fromUserId, toUserId)
        del_embedding_info(redis, fromUserId, toUserId)
    if msg_type == 'CHAT' and 'prompt_ext' in lcExt and lcExt['prompt_ext']:
        customHistoryList = []
        for customHistory in lcExt['prompt_ext']:
            if customHistory['role'] and customHistory['content']:
                customHistoryList.append({'role':customHistory['role'], 'content': customHistory['content']})
        addHistory(redis, historyListKey, {'list':customHistoryList, 'time':now})
    if 'ai_generate' in lcExt and lcExt['ai_generate'] == False:
        if msg_type == 'CHAT':
            history['user'] = content
            history['assistant'] = ''
            history['uid'] = fromUserId
            history['type'] = 'ask'
            addHistory(redis, historyListKey, history)
        return ''
    if content == '/reset_prompt' or content == "/reset":
        if msg_type == 'CHAT':
            removeAllHistory(redis, historyListKey)
        del_preset_name(redis, fromUserId, toUserId)
        del_embedding_info(redis, fromUserId, toUserId)
        return 'prompt is reset'
    preset_embedding_infos = lanying_embedding.get_preset_embedding_infos(config.get('embeddings'), app_id, preset_name)
    for now_embedding_info in lanying_ai_plugin.get_preset_function_embeddings(app_id, preset_name):
        preset_embedding_infos.append(now_embedding_info)
    if 'linked_publish_capsule_id' in config:
        linked_publish_capsule_id = config['linked_publish_capsule_id']
        for now_embedding_info in lanying_embedding.get_preset_embedding_infos_by_publish_capsule_id(linked_publish_capsule_id):
            preset_embedding_infos.append(now_embedding_info)
        for now_embedding_info in lanying_ai_plugin.get_preset_function_embeddings_by_publish_capsule_id(linked_publish_capsule_id):
            preset_embedding_infos.append(now_embedding_info)
    if 'linked_capsule_id' in config:
        linked_capsule_id = config['linked_capsule_id']
        for now_embedding_info in lanying_embedding.get_preset_embedding_infos_by_capsule_id(linked_capsule_id):
            preset_embedding_infos.append(now_embedding_info)
        for now_embedding_info in lanying_ai_plugin.get_preset_function_embeddings_by_capsule_id(linked_capsule_id):
            preset_embedding_infos.append(now_embedding_info)
    if len(preset_embedding_infos) > 0:
        context = ""
        context_with_distance = ""
        functions_with_distance = ""
        is_use_old_embeddings = False
        embedding_names = []
        for preset_embedding_info in preset_embedding_infos:
            embedding_names.append(preset_embedding_info["embedding_name"])
        embedding_names_str = ",".join(embedding_names)
        embedding_info = get_embedding_info(redis, fromUserId, toUserId)
        using_embedding = embedding_info.get('using_embedding', 'auto')
        last_embedding_name = embedding_info.get('last_embedding_name', '')
        last_embedding_text = embedding_info.get('last_embedding_text', '')
        logging.info(f"using_embedding state: using_embedding={using_embedding}, last_embedding_name={last_embedding_name}, text_byte_size(last_embedding_text)={text_byte_size(last_embedding_text)}, embedding_names_str={embedding_names_str}")
        if using_embedding == 'once' and last_embedding_text != '' and last_embedding_name == embedding_names_str:
            context = last_embedding_text
            is_use_old_embeddings = True
        if context == '': 
            embedding_min_distance = 1.0
            embedding_role = presetExt.get('embedding_role', 'system')
            if is_fulldoc:
                embedding_role = "user"
            first_preset_embedding_info = preset_embedding_infos[0]
            embedding_max_distance = presetExt.get('embedding_max_distance', 1.0)
            embedding_content = first_preset_embedding_info.get('embedding_content', "请严格按照下面的知识回答我之后的所有问题:")
            embedding_content_type = presetExt.get('embedding_content_type', 'text')
            embedding_history_num = presetExt.get('embedding_history_num', 0)
            embedding_query_text = calc_embedding_query_text(config, content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId, model_config)
            ask_message = {"role": "user", "content": content}
            embedding_message =  {"role": embedding_role, "content": embedding_content}
            history_msg_size_max = config.get('history_msg_size_max', 1024)
            history_reserved = min(512, history_msg_size_max)
            embedding_token_limit = model_token_limit(model_config) - calcMessagesTokens(messages, model, vendor) - preset.get('max_tokens', 1024) - calcMessageTokens(ask_message, model, vendor) - calcMessageTokens(embedding_message, model, vendor) - history_reserved
            logging.info(f"embedding_token_limit | model:{model}, embedding_token_limit:{embedding_token_limit}")
            search_result = multi_embedding_search(app_id, config, openai_key_type, embedding_query_text, preset_embedding_infos, doc_id, is_fulldoc, embedding_token_limit)
            for doc in search_result:
                if hasattr(doc, 'doc_id'):
                    if hasattr(doc, 'reference'):
                        doc_reference = doc.reference
                    else:
                        doc_reference = ''
                    if (doc.doc_id, doc_reference) not in reference_list:
                        reference_list.append((doc.doc_id, doc_reference))
                now_distance = float(doc.vector_score if hasattr(doc, 'vector_score') else "0.0")
                if embedding_min_distance > now_distance:
                    embedding_min_distance = now_distance
                segment_id = lanying_embedding.parse_segment_id_int_value(doc)
                segment_begin = f'\n<KNOWLEDGE id="{doc.block_id}">\n'
                segment_end = '\n</KNOWLEDGE>\n'
                if hasattr(doc, 'question') and doc.question != "":
                    line_sep = "\n------\n"
                    if line_sep in doc.question or line_sep in doc.text:
                        line_sep = "\n######\n"
                    qa_text = f"问:{line_sep}{doc.question}{line_sep}答:{line_sep}{doc.text}{line_sep}"
                    ## qa_text = f"<QUESTION>\n{doc.question}\n</QUESTION>\n<ANSWER>\n{doc.text}\n</ANSWER>"
                    ## qa_text = "\n问: " + doc.question + "\n答: " + doc.text + "\n\n"
                    context = context + segment_begin + qa_text + segment_end
                    if is_debug:
                        context_with_distance = context_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + qa_text + "\n\n"
                elif hasattr(doc, 'function') and doc.function != "":
                    function_info = json.loads(doc.function)
                    if 'priority' not in function_info:
                        function_info['priority'] = 10
                    function_name = function_info.get('name', '')
                    if function_name in function_names:
                        function_name_finish = False
                        if hasattr(doc, 'doc_id'):
                            new_function_name = f"{function_name}_{doc.doc_id.replace('-','_')}"
                            if new_function_name not in function_names:
                                function_info["name"]  = new_function_name
                                function_names[new_function_name] = function_name
                                function_name_finish = True
                        if not function_name_finish:
                            function_seq = 2
                            while True:
                                new_function_name = f"class_{function_seq}_{function_name}"
                                if new_function_name not in function_names:
                                    function_info["name"]  = new_function_name
                                    function_names[new_function_name] = function_name
                                    break
                                function_seq += 1
                    else:
                        function_info["name"] = function_name
                        function_names[function_name] = function_name
                    function_info["doc_id"] = doc.doc_id
                    function_info["distance"] = now_distance
                    function_info["short_name"]  = function_name
                    function_info["owner_app_id"] = doc.owner_app_id
                    function_info = lanying_ai_plugin.remove_function_parameters_without_function_call_reference(doc.owner_app_id, function_info, doc.doc_id)
                    user_functions.append(function_info)
                elif embedding_content_type == 'summary':
                    context = context + segment_begin + doc.summary + segment_end + "\n\n"
                    if is_debug:
                        context_with_distance = context_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + doc.summary + "\n\n"
                else:
                    context = context + segment_begin + doc.text + segment_end+ "\n\n"
                    if is_debug:
                        context_with_distance = context_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + doc.text + "\n\n"
            if using_embedding == 'auto':
                if last_embedding_name != embedding_names_str or last_embedding_text == '' or embedding_min_distance <= embedding_max_distance:
                    embedding_info['last_embedding_name'] = embedding_names_str
                    embedding_info['last_embedding_text'] = context
                    set_embedding_info(redis, fromUserId, toUserId, embedding_info)
                else:
                    context = last_embedding_text
                    is_use_old_embeddings = True
            elif using_embedding == 'once':
                embedding_info['last_embedding_name'] = embedding_names_str
                embedding_info['last_embedding_text'] = context
                set_embedding_info(redis, fromUserId, toUserId, embedding_info)
            if context != "":
                context_with_prompt = f"{embedding_content}\n\n{context}"
                context_with_distance = f"{embedding_content}\n\n{context_with_distance}"
                messages.append({'role':embedding_role, 'content':context_with_prompt})
            if len(user_functions) > 0:
                user_functions = sort_functions(user_functions)
                if is_debug:
                    for function_info in user_functions:
                        functions_with_distance += f"[distance:{function_info.get('distance')}, function_name:{function_info.get('short_name','')}, priority:{function_info.get('priority')}]\n\n"
            if is_debug:
                if is_use_old_embeddings:
                    replyMessageAsync(config, f"[蓝莺AI] 使用之前存储的embeddings:\n[embedding_min_distance={embedding_min_distance}]\n{context}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
                else:
                    replyMessageAsync(config, f"[蓝莺AI] prompt信息如下:\n[embedding_min_distance={embedding_min_distance}]\n{context_with_distance}\n{functions_with_distance}\n",{'ai':{'role': 'ai', 'is_debug_msg': True}})
    if msg_type == 'CHAT':
        history_result = loadHistory(config, app_id, redis, historyListKey, content, messages, now, preset, presetExt, model_config, vendor)
        if history_result['result'] == 'error':
            return history_result['message']
        userHistoryList = history_result['data']
        for userHistory in userHistoryList:
            logging.info(f'userHistory:{userHistory}')
            messages.append(userHistory)
        messages.append({"role": "user", "content": content})
    else:
        history_result = loadGroupHistory(config, app_id, redis, historyListKey, content, messages, now, preset, presetExt, model_config, vendor)
        if history_result['result'] == 'error':
            return history_result['message']
        userHistoryList = history_result['data']
        for userHistory in userHistoryList:
            logging.info(f'GroupHistory:{userHistory}')
            messages.append(userHistory)
    preset['messages'] = messages
    if msg_type == 'GROUPCHAT':
        preset['user'] = config['send_from']
    functions = system_functions
    functions.extend(user_functions)
    if len(functions) > 0:
        preset['functions'] = functions
    else:
        if 'function' in preset:
            del preset['function']
    preset_message_lines = "\n".join([f"{message.get('role','')}:{message.get('content','')}" for message in messages])
    logging.info(f"==========final preset messages/functions============\n{preset_message_lines}\n{functions}")
    is_force_stream = ('force_stream' in lcExt and lcExt['force_stream'] == True)
    if is_force_stream:
        logging.info("force use stream")
        preset['stream'] = True
    if ctype == 'AUDIO':
        if 'stream' in preset:
            del preset['stream']
    oper_msg_config = {
        'force_callback': True
    }
    response = lanying_vendor.chat(vendor, prepare_info, preset)
    logging.info(f"vendor response | vendor:{vendor}, response:{response}")
    stream_msg_id = 0
    reply_ext = {
            'ai': {
                'stream': False,
                'role': 'ai'
            }
        }
    if 'feedback' in lcExt:
        reply_ext['ai']['feedback'] = lcExt['feedback']
    function_call_times = 5
    is_stream = False
    stream_msg_last_send_time = 0
    function_messages = []
    while True:
        is_stream = ('reply_generator' in response)
        if is_stream:
            reply_generator = response.get('reply_generator')
            reply = response['reply']
            stream_interval_default = 1 if is_force_stream else 3
            stream_interval = max(1, presetExt.get('stream_interval', stream_interval_default))
            stream_collect_count = presetExt.get('stream_collect_count', 10)
            content_collect = []
            content_count = 0
            collect_start_time = time.time()
            reply_ext['ai']['stream'] = True
            reply_ext['ai']['stream_interval'] = stream_interval
            reply_ext['ai']['seq'] = 0
            reply_ext['ai']['finish'] = False
            stream_usage = {}
            stream_function_name = ""
            stream_function_args = ""
            try:
                for delta in reply_generator:
                    # logging.info(f"KKK:delta:{delta}")
                    delta_content = delta.get('content', '')
                    if not delta_content:
                        delta_content = ''
                    if 'usage' in delta:
                        stream_usage = delta['usage']
                    if "function_call" in delta:
                        if "name" in delta["function_call"]:
                            stream_function_name = delta["function_call"]["name"]
                        if "arguments" in delta["function_call"]:
                            if delta.get('arguments_merge_type', 'append') == 'replace':
                                stream_function_args = delta["function_call"]["arguments"]
                            else:
                                stream_function_args += delta["function_call"]["arguments"]
                    content_count += len(delta_content)
                    content_collect.append(delta_content)
                    collect_now = time.time()
                    delta_time = collect_now - collect_start_time
                    if delta_time >= stream_interval and content_count >= stream_collect_count:
                        message_to_send = ''.join(content_collect)
                        if stream_msg_id > 0:
                            reply_ext['ai']['seq'] += 1
                            replyMessageOperAsync(config, stream_msg_id, 11, message_to_send, reply_ext, oper_msg_config, True)
                        else:
                            try:
                                reply_ext['ai']['seq'] += 1
                                stream_msg_id = replyMessageSync(config, message_to_send, reply_ext)
                            except Exception as e:
                                pass
                        reply += message_to_send
                        content_count = 0
                        content_collect = []
                        collect_start_time = collect_now
                        stream_msg_last_send_time = collect_now
            except Exception as e:
                logging.info("stream got error")
                logging.exception(e)
            reply += ''.join(content_collect)
            stream_reponse = stream_lines_to_response(preset, reply, vendor, stream_usage, stream_function_name, stream_function_args)
            response['reply'] = reply
            response['usage'] = stream_reponse['usage']
            if 'function_call' in stream_reponse:
                response['function_call'] = stream_reponse['function_call']
        add_message_statistic(app_id, config, preset, response, openai_key_type, model_config)
        function_call = response.get('function_call')
        if function_call and function_call_times > 0:
            function_call_debug = copy.deepcopy(function_call)
            if 'name' in function_call_debug:
                function_name_debug = function_call_debug['name']
                if function_name_debug in function_names:
                    function_call_debug['name'] = function_names[function_name_debug]
            if is_debug:
                replyMessageAsync(config, f"[蓝莺AI] 触发函数：{function_call_debug}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
            response = handle_function_call(app_id, config, function_call, preset, openai_key_type, model_config, vendor, prepare_info, is_debug, function_messages, reply_ext)
            function_call_times -= 1
        else:
            break
    reply = response['reply']
    command = None
    try:
        command = json.loads(reply)['ai']
        pass
    except Exception as e:
        pass
    if command is None:
        try:
            command = json.loads(reply)['lanying-connector']
            pass
        except Exception as e:
            pass
    if command:
        if is_debug:
            replyMessageAsync(config, f"[蓝莺AI]收到如下JSON:\n{reply}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
        if 'preset_welcome' in command:
            reply = command['preset_welcome']
    if command and 'ai_generate' in command and command['ai_generate'] == True:
        pass
    else:
        if msg_type == 'CHAT':
            history['user'] = content
            history['assistant'] = reply
            history['uid'] = fromUserId
            history['function_messages'] = function_messages
            addHistory(redis, historyListKey, history)
        elif msg_type == 'GROUPCHAT':
            history['type'] = 'group'
            history['content'] = reply
            history['group_id'] = config['reply_to']
            history['from'] = config['reply_from']
            history['function_messages'] = function_messages
            history['function_messages_owner'] = config['send_from']
            if 'send_from' in config:
                history['mention_list'] = [int(config['send_from'])]
            addHistory(redis, historyListKey, history)
    if msg_type == 'CHAT' and command:
        if 'reset_prompt' in command:
            removeAllHistory(redis, historyListKey)
            del_preset_name(redis, fromUserId, toUserId)
            del_embedding_info(redis, fromUserId, toUserId)
        if 'preset_name' in command:
            set_preset_name(redis, fromUserId, toUserId, command['preset_name'])
        if 'using_embedding' in command:
            if command['using_embedding'] == 'once':
                set_embedding_info(redis, fromUserId, toUserId, {'using_embedding':command['using_embedding']})
            elif command['using_embedding'] == 'auto':
                embedding_info = get_embedding_info(redis, fromUserId, toUserId)
                embedding_info['using_embedding'] = command['using_embedding']
                set_embedding_info(redis, fromUserId, toUserId, embedding_info)
        if 'prompt_ext' in command and command['prompt_ext']:
            customHistoryList = []
            for customHistory in command['prompt_ext']:
                if customHistory['role'] and customHistory['content']:
                    customHistoryList.append({'role':customHistory['role'], 'content': customHistory['content']})
            addHistory(redis, historyListKey, {'list':customHistoryList, 'time':now})
    if command and 'ai_generate' in command and command['ai_generate'] == True:
        if retry_times > 0:
            if 'preset_welcome' in command:
                replyMessageAsync(config, command['preset_welcome'],{'ai':{'role': 'ai'}})
            return handle_chat_message_try(config, msg, retry_times - 1)
        else:
            return ''
    if reference:
        location = reference.get('location', 'none')
        if location == 'ext' or location == "both":
            doc_desc_list = []
            links = []
            seq = 0
            for doc_id,doc_reference in reference_list:
                embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
                doc_info = lanying_embedding.get_doc(embedding_uuid_from_doc_id, doc_id)
                if doc_info:
                    doc_metadata = {}
                    try:
                        doc_metadata = json.loads(doc_info.get('metadata','{}'))
                    except Exception as e:
                        pass
                    if len(doc_reference) > 0:
                        link = doc_reference
                    else:
                        link = ''
                        if 'link' in doc_metadata:
                            metadata_link = doc_metadata['link']
                            if isinstance(metadata_link, str) and len(metadata_link) > 0:
                                link = metadata_link
                        if link == '':
                            link = doc_info.get('lanying_link', '')
                        if link == '':
                            link = doc_info.get('filename', '')
                    if link not in links and not is_link_need_ignore(link):
                        seq += 1
                        links.append(link)
                        doc_desc_list.append({'seq':seq, 'doc_id':doc_id, 'link':link, 'metadata': doc_metadata})
            reply_ext['reference'] = doc_desc_list
        if location == 'body' or location == "both":
            doc_format = reference.get('style', '{seq}.{doc_id}.{link}')
            seperator = reference.get('seperator', ',')
            prefix = reference.get('prefix', 'reference: ')
            doc_desc_list = []
            links = []
            seq = 0
            for doc_id,doc_reference in reference_list:
                embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
                doc_info = lanying_embedding.get_doc(embedding_uuid_from_doc_id, doc_id)
                if doc_info:
                    doc_metadata = {}
                    try:
                        doc_metadata = json.loads(doc_info.get('metadata','{}'))
                    except Exception as e:
                        pass
                    if len(doc_reference) > 0:
                        link = doc_reference
                    else:
                        link = ''
                        if 'link' in doc_metadata:
                            metadata_link = doc_metadata['link']
                            if isinstance(metadata_link, str) and len(metadata_link) > 0:
                                link = metadata_link
                        if link == '':
                            link = doc_info.get('lanying_link', '')
                        if link == '':
                            link = doc_info.get('filename', '')
                    if link not in links and not is_link_need_ignore(link):
                        seq += 1
                        links.append(link)
                        doc_desc = doc_format.replace('{seq}', f"{seq}").replace('{doc_id}', doc_id).replace('{link}', link)
                        for k,v in doc_metadata.items():
                            var_name = '{'+str(k)+'}'
                            if var_name in doc_desc:
                                try:
                                    doc_desc = doc_desc.replace(var_name, str(v))
                                except Exception as e:
                                    pass
                        doc_desc = re.sub(r'\{[a-zA-Z0-9\-_]+\}', '', doc_desc)
                        doc_desc_list.append(doc_desc)
            if len(doc_desc_list) > 0:
                reply = reply + "\n" + prefix + seperator.join(doc_desc_list)
    else:
        if add_reference == 'body' or add_reference == "both":
            reference_doc_id_list = []
            for doc_id, reference in reference_list:
                reference_doc_id_list.append(doc_id)
            reply = reply + f"\nreference: {reference_doc_id_list}"
        if add_reference == 'ext' or add_reference == "both":
            reference_doc_id_list = []
            for doc_id, reference in reference_list:
                reference_doc_id_list.append(doc_id)
            reply_ext['reference'] = reference_doc_id_list
    if len(reply) > 0:
        if stream_msg_id > 0:
            reply_ext['ai']['seq'] += 1
            reply_ext['ai']['finish'] = True
            now = time.time()
            send_delay = stream_msg_last_send_time + 1 - now
            if send_delay > 0:
                logging.info(f"Delay Send replace msg |  stream_msg_delay_send_time: {stream_msg_last_send_time}, now:{now}, send_delay: {send_delay}")
                time.sleep(send_delay)
            add_ai_message_cnt(reply)
            replyMessageOperAsync(config, stream_msg_id, 12, reply, reply_ext, oper_msg_config, False)
        else:
            if is_stream:
                reply_ext['ai']['seq'] += 1
                reply_ext['ai']['finish'] = True
                replyMessageAsync(config, reply, reply_ext)
            else:
                reply_ext['ai']['stream'] = False
                replyMessageAsync(config, reply, reply_ext)
    return ''

def is_link_need_ignore(link):
    if link.startswith("ai_plugin_") or link.startswith("dummy_filename_"):
        return True
    return False

def handle_function_call(app_id, config, function_call, preset, openai_key_type, model_config, vendor, prepare_info, is_debug, function_messages, reply_ext):
    function_name = function_call.get('name')
    function_args = json.loads(function_call.get('arguments', '{}'))
    functions = preset.get('functions', [])
    function_config = {}
    for function in functions:
        if function['name'] == function_name:
            function_config = function
    doc_id = function_config.get('doc_id', '')
    owner_app_id = function_config.get('owner_app_id', app_id)
    system_envs = {
        'admin_token': {
            'value': config.get('lanying_admin_token','')
        },
        'api_key': {
            'value': f"Bearer {config.get('access_token','')}"
        },
        'ai_api_key': {
            'value': f"Bearer {config.get('access_token','')}"
        },
        'app_id': {
            'value': app_id
        },
        'current_user_id': {
            'value': config.get('from_user_id','0')
        }
    }
    function_config = lanying_ai_plugin.fill_function_info(owner_app_id, function_config, doc_id, system_envs)
    if 'function_call' in function_config:
        lanying_function_call = function_config['function_call']
        function_call_type = lanying_function_call.get('type', 'http')
        if function_call_type == 'http':
            method = lanying_function_call.get('method', 'get')
            url = fill_function_args(function_args, lanying_function_call.get('url', ''))
            params = ensure_value_is_string(fill_function_args(function_args, lanying_function_call.get('params', {})))
            headers = ensure_value_is_string(fill_function_args(function_args, lanying_function_call.get('headers', {})))
            body = fill_function_args(function_args, lanying_function_call.get('body', {}))
            auth = lanying_function_call.get('auth', {})
            response_rules = lanying_function_call.get('response_rules', [])
            if lanying_utils.is_valid_public_url(url):
                auth_type = auth.get('type', 'none')
                logging.info(f"start request function callback | app_id:{app_id},owner_app_id:{owner_app_id}, function_name:{function_name}, auth_type:{auth_type}, url:{url}, params:{params}, headers: {headers}, body: {body}")
                auth_username = auth.get('username', '')
                auth_password = auth.get('password', '')
                if auth_type == 'basic' and len(auth_username) > 0 and len(auth_password) > 0:
                    auth_opts = HTTPBasicAuth(auth_username, auth_password)
                elif auth_type == 'digest' and len(auth_username) > 0 and len(auth_password) > 0:
                    auth_opts = HTTPDigestAuth(auth_username, auth_password)
                else:
                    auth_opts = None
                if is_lanying_send_msg_url(url):
                    if 'content' in body:
                        try:
                            send_message_content = str(body['content'])
                            content_type = str(body.get('content_type', 0))
                            if content_type == '0':
                                logging.info(f"Found send message plugin, so add content to ai_message:{send_message_content}")
                                add_ai_message_cnt(send_message_content)
                        except Exception as e:
                            pass
                if method == 'get':
                    function_response = requests.get(url, params=params, headers=headers, auth = auth_opts, timeout = (20.0, 40.0))
                else:
                    function_response = requests.post(url, params=params, headers=headers, json = body, auth = auth_opts, timeout = (20.0, 40.0))
                function_content = function_response.text
                if 'send_image_to_client' in response_rules:
                    result = send_image_to_client(config, reply_ext, function_response)
                    function_content = json.dumps(result, ensure_ascii=False)
                elif 'send_audio_to_client' in response_rules:
                    result = send_audio_to_client(config, reply_ext, function_response, function_args)
                    function_content = json.dumps(result, ensure_ascii=False)
                logging.info(f"finish request function callback | app_id:{app_id}, function_name:{function_name}, function_content: {function_content}")
                if is_debug:
                    replyMessageAsync(config, f"[蓝莺AI] 函数调用结果：{function_content}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
                function_message = {
                    "role": "function",
                    "name": function_name,
                    "content": function_content
                }
                response_message = {
                    "role": "assistant",
                    "content": "",
                    "function_call": function_call
                }
                if 'send_audio_to_client' in response_rules:
                    function_messages.append(response_message)
                    response = {
                        'result': 'ok',
                        'reply': ''
                    }
                    logging.info(f"send_audio_to_client is set, so skip ai reply message")
                    return response
                else:
                    append_message(preset, model_config, response_message)
                    append_message(preset, model_config, function_message)
                    function_messages.append(response_message)
                    function_messages.append(function_message)
                    response = lanying_vendor.chat(vendor, prepare_info, preset)
                    logging.info(f"vendor function response | vendor:{vendor}, response:{response}")
                    return response
        elif function_call_type == 'system':
            logging.info(f"handle system function call:{function_call}")
            function_response = handle_system_function(config, function_name, function_args)
            function_message = {
                "role": "function",
                "name": function_name,
                "content": json.dumps(function_response, ensure_ascii=False)
            }
            response_message = {
                "role": "assistant",
                "content": "",
                "function_call": function_call
            }
            append_message(preset, model_config, response_message)
            append_message(preset, model_config, function_message)
            function_messages.append(response_message)
            function_messages.append(function_message)
            response = lanying_vendor.chat(vendor, prepare_info, preset)
            logging.info(f"vendor function response | vendor:{vendor}, response:{response}")
            return response
    else:
        function_response = {'result': 'fail', 'message': 'function not exist'}
        function_message = {
            "role": "function",
            "name": function_name,
            "content": json.dumps(function_response, ensure_ascii=False)
        }
        response_message = {
            "role": "assistant",
            "content": "",
            "function_call": function_call
        }
        append_message(preset, model_config, response_message)
        append_message(preset, model_config, function_message)
        function_messages.append(response_message)
        function_messages.append(function_message)
        response = lanying_vendor.chat(vendor, prepare_info, preset)
        logging.info(f"vendor function response | vendor:{vendor}, response:{response}")
        return response
    raise Exception('bad_preset_function')

def send_image_to_client(config, reply_ext, response):
    try:
        response_json = response.json()
        image_url = response_json['data'][0]['url']
        replyMessageImageAsync(config, image_url, reply_ext)
        return {
            'result': 'success'
        }
    except Exception as e:
        return {
            'result': 'error',
            'message': 'exception'
        }

def send_audio_to_client(config, reply_ext, response, function_args):
    try:
        content = function_args.get("input", '')
        audio_filename = f"/tmp/audio_{int(time.time())}_{uuid.uuid4()}.mp3"
        if response.status_code == 200:
            with open(audio_filename, 'wb') as f:
                f.write(response.content)
            replyAudioMessageAsync(config, content, audio_filename, reply_ext)
            return {'result':'success'}
        else:
            return {
                'result': 'error',
                'message': 'fail to transform text to speech'
            }
    except Exception as e:
        return {
            'result': 'error',
            'message': 'exception'
        }

def handle_system_function(config, function_name, function_args):
    # if function_name == 'system_create_image':
    #     prompt = function_args.get('prompt', '')
    #     url = global_lanying_connector_server + '/v1/images/generations'
    #     headers = {
    #         "Content-Type": "application/json",
    #         "Authorization": f"Bearer {config['access_token']}"
    #     }
    #     body = {
    #         "model": config['image_generator_model'],
    #         "prompt": prompt,
    #         "n": 1,
    #         "size": "1024x1024"
    #     }
    #     response = requests.post(url, headers=headers, json = body, timeout = (20.0, 40.0))
    #     try:
    #         response_json = response.json()
    #         image_url = response_json['data'][0]['url']
    #         ext = {'ai':{'role': 'ai'}}
    #         replyMessageImageAsync(config, image_url, ext)
    #         return {
    #             'result': 'success'
    #         }
    #     except Exception as e:
    #         pass
    return {'result': 'failed'}

def is_lanying_send_msg_url(url):
    parsed = urlparse(url)
    if (parsed.path == '/message/send' or parsed.path == '//message/send') and 'api.maximtop' in parsed.netloc:
        return True
    return False

def ensure_value_is_string(obj):
    ret = {}
    for k,v in obj.items():
        if isinstance(v,bool):
            ret[k] = str(v).lower()
        else:
            ret[k] = str(v)
    return ret

def fill_function_args(function_args, obj):
    if isinstance(obj, str):
        for k,v in function_args.items():
            obj = obj.replace("{" + k + "}", str(v))
        return obj
    elif isinstance(obj, list):
        ret = []
        for item in obj:
            new_item = fill_function_args(function_args, item)
            ret.append(new_item)
        return ret
    elif isinstance(obj, dict):
        if ('type' in obj and obj['type'] == 'variable' and 'value' in obj):
            variable_name = obj['value']
            if variable_name in function_args:
                return function_args[variable_name]
            else:
                logging.info(f"fill_function_args | variable not found: {variable_name}")
                return ''
        else:
            ret = {}
            for k,v in obj.items():
                ret[k] = fill_function_args(function_args, v)
            return ret
    else:
        return obj

def append_message(preset, model_config, message):
    model = model_config['model']
    vendor = model_config['vendor']
    messages = preset.get('messages', [])
    completionTokens = preset.get('max_tokens', 1024)
    token_limit = model_token_limit(model_config)
    message_size = calcMessageTokens(message, model, vendor)
    if message_size > (token_limit - completionTokens) / 2:
        trunc_size = max(100, round(len(message['content']) * (token_limit - completionTokens) / 2 / message_size))
        logging.info(f"trunc function message length | old: {len(message['content'])}, new: {trunc_size}, message:{message}")
        message['content'] = message['content'][:trunc_size]
    messages.append(message)
    token_cnt = 0
    token_cnt += lanying_embedding.calc_functions_tokens(preset.get('functions',[]), model, vendor)
    for msg in messages:
        token_cnt += calcMessageTokens(msg, model, vendor)
    while token_cnt + completionTokens > token_limit:
        delete_list = []
        for i in range(len(messages)):
            if i > 0 and i < len(messages) - 4:
                if messages[i]['role'] == 'system':
                    delete_list.append(i)
                    token_cnt -= calcMessageTokens(messages[i], model, vendor)
                    break
                elif messages[i]['role'] == 'user' and  messages[i+1]['role'] == 'assistant':
                    token_cnt -= calcMessageTokens(messages[i], model, vendor)
                    token_cnt -= calcMessageTokens(messages[i+1], model, vendor)
                    delete_list.append(i)
                    delete_list.append(i+1)
                    break
        if len(delete_list) == 1:
            del messages[delete_list[0]]
        elif len(delete_list) == 2:
            del messages[delete_list[1]]
            del messages[delete_list[0]]
        elif len(delete_list) == 0:
            logging.info(f"can not found message to delete in first stage | messages: {messages}, model_config:{model_config}")
            if messages[0]['role'] == 'system':
                token_cnt -= calcMessageTokens(messages[0], model, vendor)
                del messages[0]
            else:
                logging.info(f"fail to limit message size: rest messages:{messages}")
                raise Exception('fail to limit message size')
    preset['messages'] = messages
    return preset

def multi_embedding_search(app_id, config, api_key_type, embedding_query_text, preset_embedding_infos, doc_id, is_fulldoc, embedding_token_limit):
    list = []
    max_tokens = 0
    max_blocks = 0
    preset_idx = 0
    text_hashes = {'-'}
    embedding_cache = {}
    for preset_embedding_info in preset_embedding_infos:
        embedding_name = preset_embedding_info['embedding_name']
        if doc_id != "":
            embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
            if not ('embedding_uuid' in preset_embedding_info and preset_embedding_info['embedding_uuid'] == embedding_uuid_from_doc_id):
                logging.info(f"skip embedding_name for doc_id: embedding_name:{embedding_name}, doc_id:{doc_id}")
                continue
            else:
                logging.info(f"choose embedding_name for doc_id: embedding_name:{embedding_name}, doc_id:{doc_id}")
        embedding_uuid = preset_embedding_info['embedding_uuid']
        embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
        vendor = embedding_uuid_info.get('vendor', 'openai')
        advised_model = embedding_uuid_info.get('model', '')
        model_config = lanying_vendor.get_embedding_model_config(vendor, advised_model)
        cache_key = vendor + ":" + model_config['model']
        if cache_key in embedding_cache:
            q_embedding = embedding_cache[cache_key]
        else:
            q_embedding = fetch_embeddings(app_id, config, api_key_type, embedding_query_text, vendor, model_config)
            embedding_cache[cache_key] = q_embedding
        preset_idx = preset_idx + 1
        embedding_max_tokens = lanying_embedding.word_num_to_token_num(preset_embedding_info.get('embedding_max_tokens', 1024))
        embedding_max_blocks = preset_embedding_info.get('embedding_max_blocks', 2)
        if is_fulldoc:
            embedding_max_tokens = embedding_token_limit
            embedding_max_blocks = max(100, embedding_max_blocks)
        if embedding_max_tokens > embedding_token_limit:
            embedding_max_tokens = embedding_token_limit
        max_tokens += embedding_max_tokens
        max_blocks += embedding_max_blocks
        doc_ids = preset_embedding_info.get('doc_ids', [])
        embedding_owner_app_id = app_id
        if 'app_id' in preset_embedding_info and len(preset_embedding_info['app_id']) > 0:
            embedding_owner_app_id = preset_embedding_info['app_id']
        check_storage_limit = (embedding_owner_app_id == app_id) # TODO: refine
        docs = lanying_embedding.search_embeddings(embedding_owner_app_id, embedding_name, doc_id, q_embedding, embedding_max_tokens, embedding_max_blocks, is_fulldoc, doc_ids, check_storage_limit)
        idx = 0
        for doc in docs:
            if hasattr(doc, 'text_hash'):
                text_hash = doc.text_hash
            else:
                text_hash = lanying_embedding.sha256(doc.text)
                logging.info(f"calc text sha256:{text_hash}")
            if text_hash in text_hashes:
                continue
            text_hashes.add(text_hash)
            idx = idx+1
            vector_store = float(doc.vector_score if hasattr(doc, 'vector_score') else "0.0")
            doc.__dict__['owner_app_id'] = embedding_owner_app_id
            if is_fulldoc:
                seq_id = lanying_embedding.parse_segment_id_int_value(doc)
                list.append(((seq_id,idx, preset_idx),doc))
            else:
                list.append(((idx,vector_store,preset_idx),doc))
    sorted_list = sorted(list)
    ret = []
    now_tokens = 0
    blocks_num = 0
    if max_tokens > embedding_token_limit:
        max_tokens = embedding_token_limit
    max_continue_cnt = 2 * len(preset_embedding_infos)
    for sort_key,doc in sorted_list:
        doc_tokens = int(doc.num_of_tokens) + 8
        if hasattr(doc, 'function') and doc.function != "":
            doc_tokens += 20
        elif hasattr(doc, 'question') and doc.question != "":
            doc_tokens += 20
        else:
            doc_tokens += 10
        now_tokens += doc_tokens
        blocks_num += 1
        logging.info(f"multi_embedding_search count token: sort_key:{sort_key}, max_tokens:{max_tokens}, now_tokens:{now_tokens}, num_of_tokens:{int(doc.num_of_tokens)},max_blocks:{max_blocks},blocks_num:{blocks_num}")
        if now_tokens > max_tokens:
            if max_continue_cnt > 0:
                max_continue_cnt -= 1
                now_tokens -= doc_tokens
                logging.info(f"multi_embedding_search num_of_token too large so skip: num_of_tokens:{doc_tokens}, max_continue_cnt:{max_continue_cnt}")
                continue
            else:
                break
        if blocks_num > max_blocks:
            break
        ret.append(doc)
    return ret

def loadHistory(config, app_id, redis, historyListKey, content, messages, now, preset, presetExt, model_config, vendor):
    history_msg_count_min = ensure_even(config.get('history_msg_count_min', 1))
    history_msg_count_max = ensure_even(config.get('history_msg_count_max', 10))
    history_msg_size_max = config.get('history_msg_size_max', 4096)
    history_msg_count_min = ensure_even(presetExt.get('history_msg_count_min', history_msg_count_min))
    history_msg_count_max = ensure_even(presetExt.get('history_msg_count_max', history_msg_count_max))
    history_msg_size_max = presetExt.get('history_msg_size_max', history_msg_size_max)
    completionTokens = preset.get('max_tokens', 1024)
    model = preset['model']
    token_limit = model_token_limit(model_config)
    messagesSize = calcMessagesTokens(messages, model, vendor)
    askMessage = {"role": "user", "content": content}
    nowSize = calcMessageTokens(askMessage, model, vendor) + messagesSize
    if nowSize + completionTokens >= token_limit:
        logging.info(f'stop history without history for max tokens: app_id={app_id}, now prompt size:{nowSize}, completionTokens:{completionTokens},token_limit:{token_limit}')
        return {'result':'error', 'message': lanying_config.get_message_too_long(app_id)}
    res = []
    history_bytes = 0
    history_count = 0
    for history in reversed_history_generator(historyListKey):
        if isinstance(history, list):
            nowHistoryList = history
        else:
            nowHistoryList = []
            userMessage = {'role':'user', 'content': history['user']}
            assistantMessage = {'role':'assistant', 'content': history['assistant']}
            nowHistoryList.append(userMessage)
            if 'function_messages' in history:
                for function_message in history['function_messages']:
                    nowHistoryList.append(function_message)
            if len(assistantMessage['content']) > 0:
                nowHistoryList.append(assistantMessage)
        history_count += len(nowHistoryList)
        historySize = 0
        for nowHistory in nowHistoryList:
            historySize += calcMessageTokens(nowHistory, model, vendor)
            now_history_content = nowHistory.get('content','')
            now_history_bytes = text_byte_size(now_history_content)
            history_bytes += now_history_bytes
            logging.info(f"history_bytes: app_id={app_id}, content={now_history_content}, bytes={now_history_bytes}")
        if history_count > history_msg_count_min:
            if history_count > history_msg_count_max:
                logging.info(f"stop history for history_msg_count_max: app_id={app_id}, history_msg_count_max={history_msg_count_max}, history_count={history_count}")
                break
            if history_bytes > history_msg_size_max:
                logging.info(f"stop history for history_msg_size_max: app_id={app_id}, history_msg_size_max={history_msg_size_max}, history_count={history_count}")
                break
        if nowSize + historySize + completionTokens < token_limit:
            for nowHistory in reversed(nowHistoryList):
                res.append(nowHistory)
            nowSize += historySize
            logging.info(f'history state: app_id={app_id}, now_prompt_size={nowSize}, history_count={history_count}, history_bytes={history_bytes}')
        else:
            logging.info(f'stop history for max tokens: app_id={app_id}, now_prompt_size:{nowSize}, completionTokens:{completionTokens}, token_limit:{token_limit}')
            break
    logging.info(f"history finish: app_id={app_id}, vendor:{vendor}, now_prompt_size:{nowSize}, completionTokens:{completionTokens}, token_limit:{token_limit}")
    return {'result':'ok', 'data': reversed(res)}

def loadGroupHistory(config, app_id, redis, historyListKey, content, messages, now, preset, presetExt, model_config, vendor):
    group_history_use_mode = config.get('group_history_use_mode', 'all')
    history_msg_count_min = ensure_even(config.get('history_msg_count_min', 1))
    history_msg_count_max = ensure_even(config.get('history_msg_count_max', 10))
    history_msg_size_max = config.get('history_msg_size_max', 4096)
    history_msg_count_min = ensure_even(presetExt.get('history_msg_count_min', history_msg_count_min))
    history_msg_count_max = ensure_even(presetExt.get('history_msg_count_max', history_msg_count_max))
    history_msg_size_max = presetExt.get('history_msg_size_max', history_msg_size_max)
    completionTokens = preset.get('max_tokens', 1024)
    model = preset['model']
    token_limit = model_token_limit(model_config)
    messagesSize = calcMessagesTokens(messages, model, vendor)
    ask_user_id = config['send_from']
    ai_user_id = config['reply_from']
    askMessage = {"role": "user", "content": content, "name": ask_user_id}
    nowSize = calcMessageTokens(askMessage, model, vendor) + messagesSize
    if nowSize + completionTokens >= token_limit:
        logging.info(f'stop history without history for max tokens: app_id={app_id}, now prompt size:{nowSize}, completionTokens:{completionTokens},token_limit:{token_limit}')
        return {'result':'error', 'message': lanying_config.get_message_too_long(app_id)}
    res = []
    history_bytes = 0
    history_count = 0
    for history in reversed_group_history_generator(historyListKey):
        message_from = str(history.get('from', ''))
        message_content = history.get('content', '')
        mention_list = history.get('mention_list', [])
        if group_history_use_mode == 'mention':
            if message_from == ai_user_id:
                if int(ask_user_id) in mention_list:
                    pass
                else:
                    logging.info(f"group_history_use_mode skip assistant not mention message:{message_from}, {mention_list}")
                    continue
            elif message_from == ask_user_id:
                if int(ai_user_id) in mention_list:
                    pass
                else:
                    logging.info(f"group_history_use_mode skip user not mention message:{message_from}, {mention_list}")
                    continue
            else:
                logging.info(f"group_history_use_mode skip other user message:{message_from}")
                continue
        if message_from == ai_user_id:
            now_message = {'role': 'assistant', 'content': message_content}
        else:
            now_message = {'role': 'user', 'content': message_content, 'name': message_from}
        nowHistoryList = []
        if 'function_messages' in history and 'function_messages_owner' in history:
            if history['function_messages_owner'] == config['send_from']:
                for function_message in history['function_messages']:
                    nowHistoryList.append(function_message)
        if len(now_message['content']) > 0:
            nowHistoryList.append(now_message)
        history_count += len(nowHistoryList)
        historySize = 0
        for nowHistory in nowHistoryList:
            historySize += calcMessageTokens(nowHistory, model, vendor)
            now_history_content = nowHistory.get('content','')
            now_history_bytes = text_byte_size(now_history_content)
            history_bytes += now_history_bytes
            logging.info(f"history_bytes: app_id={app_id}, content={now_history_content}, bytes={now_history_bytes}")
        if history_count > history_msg_count_min:
            if history_count > history_msg_count_max:
                logging.info(f"stop history for history_msg_count_max: app_id={app_id}, history_msg_count_max={history_msg_count_max}, history_count={history_count}")
                break
            if history_bytes > history_msg_size_max:
                logging.info(f"stop history for history_msg_size_max: app_id={app_id}, history_msg_size_max={history_msg_size_max}, history_count={history_count}")
                break
        if nowSize + historySize + completionTokens < token_limit:
            for nowHistory in reversed(nowHistoryList):
                res.append(nowHistory)
            nowSize += historySize
            logging.info(f'history state: app_id={app_id}, now_prompt_size={nowSize}, history_count={history_count}, history_bytes={history_bytes}')
        else:
            logging.info(f'stop history for max tokens: app_id={app_id}, now_prompt_size:{nowSize}, completionTokens:{completionTokens}, token_limit:{token_limit}')
            break
    logging.info(f"history finish: app_id={app_id}, vendor:{vendor}, now_prompt_size:{nowSize}, completionTokens:{completionTokens}, token_limit:{token_limit}")
    return {'result':'ok', 'data': reversed(res)}

def reversed_history_generator(historyListKey):
    redis = lanying_redis.get_redis_connection()
    now = int(time.time())
    history_list = []
    for historyStr in getHistoryList(redis, historyListKey):
        history = json.loads(historyStr)
        if history['time'] < now - expireSeconds:
            removeHistory(redis, historyListKey, historyStr)
        history_list.append(history)
    last_history = None
    for history in reversed(history_list):
        if 'list' in history:
            yield history['list']
        elif last_history is None:
            type = history.get('type', 'both')
            if type == 'ask':
                logging.info(f"found a ask only history:{historyListKey}")
                history['assistant'] = 'OK'
            last_history = history
        else:
            type = history.get('type', 'both')
            last_type = last_history.get('type', 'both')
            if last_type == 'reply':
                history['user'] =  merge_history_content(history['user'], last_history['user'])
                history['assistant'] =  merge_history_content(history['assistant'], last_history['assistant'])
                last_history = history
            elif last_type == 'ask' or last_type == 'both':
                if type == 'ask':
                    history['user'] =  merge_history_content(history['user'], last_history['user'])
                    history['assistant'] =  merge_history_content(history['assistant'], last_history['assistant'])
                    history['type'] = last_type
                    last_history = history
                else:
                    yield last_history
                    last_history = history
    if last_history:
        last_type = last_history.get('type', 'both')
        if last_type == 'ask' or last_type == 'both':
            yield last_history

def reversed_group_history_generator(historyListKey):
    redis = lanying_redis.get_redis_connection()
    now = int(time.time())
    history_list = []
    for historyStr in getHistoryList(redis, historyListKey):
        history = json.loads(historyStr)
        if history['time'] < now - expireSeconds:
            removeHistory(redis, historyListKey, historyStr)
        history_list.append(history)
    for history in reversed(history_list):
        type = history.get('type', 'none')
        if type == 'group':
            yield history

def merge_history_content(a, b):
    if a == '':
        return b
    elif b == '':
        return a
    else:
        return a + '\n' + b

def model_token_limit(model_config):
    return model_config['token_limit']

def historyListChatGPTKey(app_id, fromUserId, toUserId):
    return "lanying:connector:history:list:chatGPT:" + app_id + ":" + fromUserId + ":" + toUserId

def historyListGroupKey(app_id, groupId):
    return "lanying:connector:history:list:group:" + app_id + ":" + groupId

def addHistory(redis, historyListKey, history):
    if redis:
        Count = redis.rpush(historyListKey, json.dumps(history))
        redis.expire(historyListKey, expireSeconds)
        if Count > maxUserHistoryLen:
            redis.lpop(historyListKey)

def getHistoryList(redis, historyListKey):
    if redis:
        return redis.lrange(historyListKey, 0, -1)
    return []

def removeHistory(redis, historyListKey, historyStr):
    if redis:
        redis.lrem(historyListKey, 1, historyStr)

def removeAllHistory(redis, historyListKey):
    if redis:
        redis.delete(historyListKey)

def preset_name_key(fromUserId, toUserId):
    return "lanying:connector:preset_name:gpt3" + fromUserId + ":" + toUserId

def set_preset_name(redis, fromUserId, toUserId, preset_name):
    if redis:
        key = preset_name_key(fromUserId,toUserId)
        redis.set(key, preset_name)
        redis.expire(key, presetNameExpireSeconds)

def get_preset_name(redis, fromUserId, toUserId):
    if redis:
        key = preset_name_key(fromUserId,toUserId)
        value = redis.get(key)
        if value:
            return str(value, 'utf-8')

def del_preset_name(redis, fromUserId, toUserId):
    if redis:
        key = preset_name_key(fromUserId,toUserId)
        redis.delete(key)

def calcMessagesTokens(messages, model, vendor):
    try:
        encoding = lanying_vendor.encoding_for_model(vendor, model)
        num_tokens = 0
        for message in messages:
            num_tokens += 4
            for key, value in message.items():
                if isinstance(value, dict):
                    for k,v in value.items():
                        num_tokens += len(encoding.encode(v, disallowed_special=()))
                else:
                    num_tokens += len(encoding.encode(value, disallowed_special=()))
                if key == "name":
                    num_tokens += -1
        num_tokens += 2
        return num_tokens
    except Exception as e:
        logging.exception(e)
        return MaxTotalTokens

def calcMessageTokens(message, model, vendor):
    try:
        encoding = lanying_vendor.encoding_for_model(vendor, model)
        num_tokens = 0
        num_tokens += 4
        for key, value in message.items():
            if isinstance(value, dict):
                for k,v in value.items():
                    num_tokens += len(encoding.encode(v, disallowed_special=()))
            else:
                num_tokens += len(encoding.encode(value, disallowed_special=()))
            if key == "name":
                num_tokens += -1
        return num_tokens
    except Exception as e:
        logging.exception(e)
        return MaxTotalTokens

def get_preset_auth_info(config, openai_key_type, vendor):
    if openai_key_type == 'share':
        DefaultApiKey = lanying_config.get_lanying_connector_default_api_key(vendor)
        DefaultApiGroupId = lanying_config.get_lanying_connector_default_api_group_id(vendor)
        DefaultSecretKey = lanying_config.get_lanying_connector_default_secret_key(vendor)
        if DefaultApiKey:
            return {
                'api_key':DefaultApiKey,
                'secret_key':DefaultSecretKey,
                'api_group_id':DefaultApiGroupId
            }
    else:
        return get_preset_self_auth_info(config, vendor)

def get_preset_self_auth_info(config, vendor):
    auth_info = config.get('vendors', {}).get(vendor)
    if auth_info is None and vendor == "openai": # for compatibility
        api_key = config.get('openai_api_key', '')
        if api_key != '':
            auth_info = {
                'api_key': api_key
            }
    return auth_info

def reply_message_read_ack(config, msg):
    msg_type = msg['type']
    is_sync_mode = get_is_sync_mode(config)
    if msg_type == 'CHAT' and not is_sync_mode:
        fromUserId = config['from_user_id']
        toUserId = config['to_user_id']
        msgId = config['msg_id']
        appId = config['app_id']
        lanying_connector.sendReadAckAsync(appId, toUserId, fromUserId, msgId)

def check_image_quota(model_config, preset):
    n = abs(int(preset.get('n', 1)))
    quality = str(preset.get('quality', 'standard')).lower()
    size = str(preset.get('size', '1024x1024')).lower()
    image_summary = f"{quality}_{size}"
    image_quota = model_config.get("image_quota", {})
    if image_summary in image_quota and n > 0:
        return {'result':'ok', 'quota': n * image_quota[image_summary], 'image_summary': image_summary}
    else:
        return {'result':'error', 'message': 'The size is not supported by this model.'}

def check_text_to_speech_quota(model_config, preset):
    input = preset.get('input', '')
    byte_size = text_byte_size(input)
    quota_count_value = model_config['quota_count_value']
    count = math.ceil(byte_size / quota_count_value)
    if  count < 1:
        count = 1
    quota = model_config['quota'] * count
    logging.info(f"check_text_to_speech_quota | input:{input}, byte_size:{byte_size}, count: {count}, quota:{quota}")
    return {'result':'ok', 'quota': quota}

def check_speech_to_text_quota(model_config, preset, request):
    if 'file' not in request.files:
        return {'result': 'error', 'message': 'file not exist', 'code': 'file_not_exist'}
    file = request.files['file']
    if file.filename == '':
        return {'result': 'error', 'message': 'file not exist', 'code': 'file_not_exist'}
    audio_file_path = f"/tmp/audio-{int(time.time())}-{uuid.uuid4()}-{file.filename}"
    file.save(audio_file_path)
    duration_ms = 0
    try:
        audio = AudioSegment.from_file(audio_file_path)
        duration_ms = len(audio)
    except Exception as e:
        logging.exception(e)
    duration = math.ceil(duration_ms / 1000)
    if duration <= 0:
        return {'result': 'error', 'message': 'file format not support', 'code': 'file_format_not_support'}
    file_info = {
        'filename': file.filename,
        'path': audio_file_path,
        'mimetype': file.mimetype
    }
    quota_count_value = model_config['quota_count_value']
    count = math.ceil(duration / quota_count_value)
    if  count < 1:
        count = 1
    quota = model_config['quota'] * count
    logging.info(f"check_speech_to_text_quota | audio_file_path:{audio_file_path}, duration_ms:{duration_ms}, duration: {duration}, quota:{quota}")
    return {'result':'ok', 'quota': quota, 'duration_ms': duration_ms, 'duration': duration, 'file_info': file_info}

def add_message_statistic(app_id, config, preset, response, openai_key_type, model_config):
    model_type = model_config.get('type', '')
    if model_type == 'image':
        logging.info(f"add_message_statistic {model_type} response: {response}, model_config: {model_config}")
        redis = lanying_redis.get_redis_connection()
        if redis:
            model = preset['model']
            message_count_quota = model_config['quota']
            logging.info(f"add message statistic: app_id={app_id}, model={model}, message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")
            key_count = 0
            for key in get_message_statistic_keys(config, app_id):
                key_count += 1
                redis.hincrby(key, 'image_message_count', 1)
                if openai_key_type == 'share':
                    add_quota(redis, key, 'message_count_quota_share', message_count_quota)
                else:
                    add_quota(redis, key, 'message_count_quota_self', message_count_quota)
                new_message_count_quota = add_quota(redis, key, 'message_count_quota', message_count_quota)
                if key_count == 1 and new_message_count_quota > 100 and (new_message_count_quota+99) // 100 != (new_message_count_quota - message_count_quota+99) // 100:
                    notify_butler(app_id, 'message_count_quota_reached', get_message_limit_state(app_id))
            # try:
            #     maybe_statistic_ai_capsule(config, app_id, product_id, message_count_quota, openai_key_type)
            # except Exception as e:
            #     logging.exception(e)
        else:
            logging.error(f"skip image statistic | app_id:{app_id}, preset:{preset}, response:{response}, openai_key_type:{openai_key_type}, model_config:{model_config}")
    elif model_type in ['text_to_speech', 'speech_to_text'] :
        logging.info(f"add_message_statistic {model_type} response: {response}, model_config: {model_config}")
        redis = lanying_redis.get_redis_connection()
        if redis:
            model = preset['model']
            message_count_quota = model_config['quota']
            if model_type == 'speech_to_text':
                speech_to_text_duration = config.get('speech_to_text_duration', 0)
                logging.info(f"speech_to_text response | duration={speech_to_text_duration}, response:{response}")
            logging.info(f"add message statistic: app_id={app_id}, model={model}, message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")
            key_count = 0
            for key in get_message_statistic_keys(config, app_id):
                key_count += 1
                redis.hincrby(key, f'{model_type}_message_count', 1)
                if openai_key_type == 'share':
                    add_quota(redis, key, 'message_count_quota_share', message_count_quota)
                else:
                    add_quota(redis, key, 'message_count_quota_self', message_count_quota)
                new_message_count_quota = add_quota(redis, key, 'message_count_quota', message_count_quota)
                if key_count == 1 and new_message_count_quota > 100 and (new_message_count_quota+99) // 100 != (new_message_count_quota - message_count_quota+99) // 100:
                    notify_butler(app_id, 'message_count_quota_reached', get_message_limit_state(app_id))
            # try:
            #     maybe_statistic_ai_capsule(config, app_id, product_id, message_count_quota, openai_key_type)
            # except Exception as e:
            #     logging.exception(e)
        else:
            logging.error(f"skip image statistic | app_id:{app_id}, preset:{preset}, response:{response}, openai_key_type:{openai_key_type}, model_config:{model_config}")
    
    elif 'usage' in response:
        usage = response['usage']
        completion_tokens = usage.get('completion_tokens',0)
        prompt_tokens = usage.get('prompt_tokens', 0)
        total_tokens = usage.get('total_tokens', 0)
        text_size = calc_used_text_size(preset, response, model_config)
        model = preset['model']
        message_count_quota = calc_message_quota(model_config, total_tokens)
        redis = lanying_redis.get_redis_connection()
        product_id = config.get('product_id', 0)
        if product_id == 0:
            logging.info(f"skip message statistic for no product_id: app_id={app_id}, model={model}, completion_tokens={completion_tokens}, prompt_tokens={prompt_tokens}, total_tokens={total_tokens},text_size={text_size}, message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")
            return
        if redis:
            logging.info(f"add message statistic: app_id={app_id}, model={model}, completion_tokens={completion_tokens}, prompt_tokens={prompt_tokens}, total_tokens={total_tokens},text_size={text_size}, message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")
            key_count = 0
            for key in get_message_statistic_keys(config, app_id):
                key_count += 1
                redis.hincrby(key, 'total_tokens', total_tokens)
                redis.hincrby(key, 'text_size', text_size)
                redis.hincrby(key, 'message_count', 1)
                if openai_key_type == 'share':
                    add_quota(redis, key, 'message_count_quota_share', message_count_quota)
                else:
                    add_quota(redis, key, 'message_count_quota_self', message_count_quota)
                new_message_count_quota = add_quota(redis, key, 'message_count_quota', message_count_quota)
                if key_count == 1 and new_message_count_quota > 100 and (new_message_count_quota+99) // 100 != (new_message_count_quota - message_count_quota+99) // 100:
                    notify_butler(app_id, 'message_count_quota_reached', get_message_limit_state(app_id))
            try:
                maybe_statistic_ai_capsule(config, app_id, product_id, message_count_quota, openai_key_type)
            except Exception as e:
                logging.exception(e)
        else:
            logging.error(f"fail to statistic message: app_id={app_id}, model={model}, completion_tokens={completion_tokens}, prompt_tokens={prompt_tokens}, total_tokens={total_tokens},text_size={text_size},message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")

def maybe_statistic_ai_capsule(config, app_id, product_id, message_count_quota, openai_key_type):
    if 'linked_publish_capsule_id' in config:
        linked_publish_capsule_id = config['linked_publish_capsule_id']
        capsule = lanying_ai_capsule.get_publish_capsule(linked_publish_capsule_id)
        if capsule:
            statistic_capsule(capsule, app_id, product_id, message_count_quota, openai_key_type)
    if 'linked_capsule_id' in config:
        linked_capsule_id = config['linked_capsule_id']
        capsule = lanying_ai_capsule.get_capsule(linked_capsule_id)
        if capsule:
            statistic_capsule(capsule, app_id, product_id, message_count_quota, openai_key_type)

def statistic_capsule(capsule, app_id, product_id, message_count_quota, openai_key_type):
    now = datetime.now()
    capsule_app_id = capsule['app_id']
    capsule_id = capsule['capsule_id']
    everymonth_key = lanying_ai_capsule.statistic_capsule_key(capsule_app_id, now)
    app_ids_key = lanying_ai_capsule.statistic_capsule_app_ids_key(now)
    redis = lanying_redis.get_redis_connection()
    field = json.dumps({
        'capsule_id': capsule_id,
        'product_id': product_id,
        'openai_key_type': openai_key_type,
        'day': now.strftime('%Y-%m-%d'),
        'app_id': app_id
    })
    redis.hincrbyfloat(everymonth_key, field, message_count_quota)
    redis.hincrby(app_ids_key, capsule_app_id, 1)

def add_quota(redis, key, field, quota):
    if isinstance(quota, int):
        return redis.hincrby(key, field, quota)
    else:
        field_float = field + "_float"
        new_quota = redis.hincrby(key, field_float, round(quota * 10000))
        if new_quota >= 10000:
            increment = new_quota // 10000
            redis.hincrby(key, field_float, - increment * 10000)
            return redis.hincrby(key, field, increment)
        else:
            return redis.hincrby(key, field, 0)

def check_message_limit(app_id, config, vendor, is_chat):
    message_per_month = config.get('message_per_month', 0)
    product_id = config.get('product_id', 0)
    if product_id == 0:
        return {'result':'ok', 'openai_key_type':'self'}
    enable_extra_price = False
    if config.get('enable_extra_price', 0) == 1 and product_id >= 7005:
        enable_extra_price = True
    redis = lanying_redis.get_redis_connection()
    if redis:
        key = get_message_statistic_keys(config, app_id)[0]
        message_count_quota = redis.hincrby(key, 'message_count_quota', 0)
        quota_pre_check = config.get('quota_pre_check', 0)
        if message_count_quota + quota_pre_check < message_per_month:
            return {'result':'ok', 'openai_key_type':'share'}
        else:
            if enable_extra_price:
                self_auth_info = get_preset_self_auth_info(config, vendor)
                if self_auth_info:
                    return {'result':'ok', 'openai_key_type':'self'}
                else:
                    return {'result':'ok', 'openai_key_type':'share'}
            elif is_chat:
                msgs = []
                error_msg = lanying_config.get_message_no_quota(app_id)
                msgs.append(error_msg)
                quota_exceed_reply_type = config.get('quota_exceed_reply_type', 'capsule')
                if quota_exceed_reply_type == 'msg':
                    quota_exceed_reply_msg = config.get('quota_exceed_reply_msg', '')
                    if quota_exceed_reply_msg != '':
                        msgs.append(quota_exceed_reply_msg)
                elif quota_exceed_reply_type == 'capsule':
                    share_text = lanying_ai_capsule.get_share_text(app_id, config.get('chatbot_id'))
                    if share_text != '':
                        msgs.append(share_text)
                if len(msgs) == 1:
                    return {'result':'error', 'code':'no_quota', 'msg': msgs[0]}
                else:
                    return {'result':'error', 'code':'no_quota', 'msg': msgs}
            else:
                return {'result':'error', 'code':'no_quota', 'msg': lanying_config.get_message_no_quota(app_id)}
    else:
        return {'result':'error', 'code':'internal_error','msg':lanying_config.get_message_404(app_id)}

def check_message_per_month_per_user(msg, config):
    app_id = msg['appId']
    from_user_id = msg['from']['uid']
    limit = config.get('message_per_month_per_user', -1)
    if limit > 0:
        redis = lanying_redis.get_redis_connection()
        if redis:
            now = datetime.now()
            key = f"lanying:connector:message_per_month_per_user:{app_id}:{from_user_id}:{now.year}:{now.month}"
            value = redis.incrby(key, 1)
            if value > limit:
                msgs = []
                error_msg = lanying_config.get_message_reach_user_message_limit(app_id)
                msgs.append(error_msg)
                quota_exceed_reply_type = config.get('quota_exceed_reply_type', 'capsule')
                quota_exceed_reply_msg = config.get('quota_exceed_reply_msg', '')
                logging.info(f"exceed message_per_month_per_user app_id:{app_id}, reply_type:{quota_exceed_reply_type}, reply_msg:{quota_exceed_reply_msg}")
                if quota_exceed_reply_type == 'msg':
                    if quota_exceed_reply_msg != '':
                        msgs.append(quota_exceed_reply_msg)
                elif quota_exceed_reply_type == 'capsule':
                    share_text = lanying_ai_capsule.get_share_text(app_id, config.get('chatbot_id'))
                    if share_text != '':
                        msgs.append(share_text)
                if len(msgs) == 1:
                    return {'result':'error', 'msg': msgs[0]}
                else:
                    return {'result':'error', 'msg': msgs}
    return {'result':'ok'}

def check_message_deduct_failed(app_id, config):
    if lanying_config.get_lanying_connector_deduct_failed(app_id):
        return {'result':'error', 'code':'deduct_failed', 'msg':lanying_config.get_message_deduct_failed(app_id)}
    return {'result':'ok'}

def check_product_id(app_id, config):
    productId = 0
    if config and 'product_id' in config:
        productId = config['product_id']
    if productId == 0:
        logging.info(f"service is expired: app_id={app_id}")
        return {'result':'error', 'msg':'service is expired'}
    return {'result':'ok'}

def get_message_limit_state(app_id):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        redis = lanying_redis.get_redis_connection()
        if redis:
            key = get_message_statistic_keys(config, app_id)[0]
            kvs = redis.hgetall(key)
            if kvs is None:
                return {}
            ret = {}
            for k,v in kvs.items():
                ret[k.decode('utf-8')] = int(v.decode('utf-8'))
            return ret
    return {}

def buy_message_quota(app_id, type, value):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        redis = lanying_redis.get_redis_connection()
        if redis:
            key = get_message_statistic_keys(config, app_id)[0]
            if type == "share":
                return redis.hincrby(key, 'message_count_quota_buy_share', value)
            else:
                return redis.hincrby(key, 'message_count_quota_buy_self', value)
    return -1

def check_model_allow(model_config, model):
    if model_config:
        return {'result':'ok'}
    return {'result':'error', 'msg':f'model {model} is not supported'}

def calc_message_quota(model_config, total_tokens):
    multi = model_config['quota']
    count = round(total_tokens / 2048)
    if  count < 1:
        count = 1
    return count * multi

def calc_used_text_size(preset, response, model_config):
    text_size = 0
    if model_config['type'] == "chat":
        for message in preset['messages']:
            text_size += text_byte_size(message.get('content', ''))
        if 'reply' in response:
            reply = response['reply']
        else:
            try:
                reply = response['choices'][0]['message']['content'].strip()
            except Exception as e:
                reply = ''
        text_size += text_byte_size(reply)
    elif model_config['type'] == "embedding":
        text_size += text_byte_size(preset['input'])
    else:
        raise Exception(f'bad model config:{model_config}')
    return text_size

def get_message_statistic_keys(config, app_id):
    now = datetime.now()
    month_start_date = datetime(now.year, now.month, 1)
    if 'start_date' in config:
        pay_start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')
    else:
        pay_start_date = month_start_date
    while now >= pay_start_date:
        end_date = pay_start_date + relativedelta(months=1)
        if now >= pay_start_date and now < end_date:
            break
        else:
            pay_start_date = end_date
    pay_start_key = f"lanying:connector:statistics:message:pay_start_date:{app_id}:{pay_start_date.strftime('%Y-%m-%d')}"
    month_start_key = f"lanying:connector:statistics:message:month_start_date:{app_id}:{month_start_date.strftime('%Y-%m-%d')}"
    everyday_key = f"lanying:connector:statistics:message:everyday:{app_id}:{now.strftime('%Y-%m-%d')}"
    product_id = config.get('product_id', 0)
    tenant_id = config.get('tenant_id', 0)
    if product_id == 7001 and tenant_id > 0:
        share_key = f"lanying:connector:statistics:message:pay_start_date:{tenant_id}:{pay_start_date.strftime('%Y-%m-%d')}"
        return [share_key, pay_start_key, month_start_key, everyday_key]
    else:
        return [pay_start_key, month_start_key, everyday_key]

def notify_butler(app_id, event, data):
    logging.info(f"notify butler: app_id={app_id}, event={event}, data={json.dumps(data)}")
    endpoint = os.getenv('LANYING_BUTLER_ENDPOINT', 'https://butler.lanyingim.com')
    try:
        sendResponse = requests.post(f"{endpoint}/app/lanying_connector_event",
                                        headers={'app_id': app_id},
                                        json={'app_id':app_id, 'event':event, 'data':data})
        logging.info(sendResponse)
    except Exception as e:
        logging.info(e)

def ensure_even(num):
    if num % 2 == 1:
        num += 1
    return num

def check_message_rate(app_id, path):
    redis = lanying_redis.get_redis_connection()
    now = int(time.time())
    key = f"lanying_connector:rate_limit:{app_id}:{path}:{now}"
    limit = lanying_config.get_lanying_connector_rate_limit(app_id)
    count = redis.incrby(key, 1)
    redis.expire(key, 10)
    if count > limit:
        logging.info(f"app:{app_id} is exceed rate limit, limit is {limit}, count is {count}")
        return {'result':'error', 'code':429, 'msg': 'request too fast'}
    return {'result':'ok'}

def embedding_info_key(fromUserId, toUserId):
    return "lanying:connector:embedding_info:" + fromUserId + ":" + toUserId

def set_embedding_info(redis, fromUserId, toUserId, embedding_info):
    if redis:
        key = embedding_info_key(fromUserId,toUserId)
        redis.set(key, json.dumps(embedding_info))
        redis.expire(key, using_embedding_expire_seconds)

def get_embedding_info(redis, fromUserId, toUserId):
    if redis:
        key = embedding_info_key(fromUserId,toUserId)
        value = redis.get(key)
        if value:
            try:
                return json.loads(value)
            except Exception as e:
                return {}
    return {}

def del_embedding_info(redis, fromUserId, toUserId):
    if redis:
        key = embedding_info_key(fromUserId,toUserId)
        redis.delete(key)

def create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor, model):
    return lanying_embedding.create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor, model)

def configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor, model):
    return lanying_embedding.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor, model)

def list_embeddings(app_id):
    return lanying_embedding.list_embeddings(app_id)

def get_embedding_doc_info_list(app_id, embedding_name, start, end):
    return lanying_embedding.get_embedding_doc_info_list(app_id, embedding_name, start, end)

def list_embedding_tasks(app_id, embedding_name):
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info:
        task_list = lanying_embedding.get_task_list(embedding_name_info['embedding_uuid'])
        return task_list
    return []

def fetch_embeddings(app_id, config, openai_key_type, text, vendor, model_config):
    model = model_config['model']
    embedding_api_key_type = openai_key_type
    logging.info(f"fetch_embeddings: app_id={app_id}, vendor:{vendor}, model={model},text={text}")
    auth_info = get_preset_auth_info(config, embedding_api_key_type, vendor)
    if auth_info is None:
        embedding_api_key_type = "share"
        auth_info = get_preset_auth_info(config, embedding_api_key_type, vendor)
    prepare_info = lanying_vendor.prepare_embedding(vendor, auth_info, 'query')
    response = lanying_vendor.embedding(vendor, prepare_info, model, text)
    embedding = response['embedding']
    preset = {'model':model, 'input':text}
    add_message_statistic(app_id, config, preset, response, embedding_api_key_type, model_config)
    return embedding

def handle_embedding_command(msg, config):
    app_id = msg['appId']
    content = msg['content']
    result = lanying_command.find_command(content, app_id)
    if result["result"] == "found":
        name = result["name"]
        args = [msg, config]
        args.extend(result["args"])
        try:
            return eval(name)(*args)
        except Exception as e:
            logging.info(f"eval command exception: app_id:{app_id}, content:{content}")
            logging.exception(e)
            return '命令执行失败'
    else:
        return ''
def bluevector_mode(msg, config, embedding_name):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    embedding_names = lanying_embedding.list_embedding_names(app_id)
    can_manage_embedding_names = []
    for now_embedding_name in embedding_names:
        result = check_can_manage_embedding(app_id, now_embedding_name, from_user_id)
        if result['result'] == 'ok':
            can_manage_embedding_names.append(now_embedding_name)
    if embedding_name in can_manage_embedding_names:
        redis = lanying_redis.get_redis_connection()
        key = user_default_embedding_name_key(app_id, from_user_id)
        redis.set(key, embedding_name)
        return f"设置成功"
    else:
        return f"设置失败：「{embedding_name}」不是合法知识库名称， 合法值为：{can_manage_embedding_names}"

def bluevector_add(msg, config, embedding_name, file_uuid):
    from_user_id = int(msg['from']['uid'])
    to_user_id = int(msg['to']['uid'])
    app_id = msg['appId']
    result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
    if result['result'] == 'error':
        return result['message']
    logging.info(f"receive embedding add command: app_id:{app_id}, from_user_id:{from_user_id}, file_uuid:{file_uuid}")
    attachment_str = get_attachment(from_user_id, file_uuid)
    if attachment_str:
        attachment = json.loads(attachment_str)
        url = attachment['url']
        dname = attachment['dName']
        headers = {'app_id': app_id,
                'access-token': config['lanying_admin_token'],
                'user_id': str(to_user_id)}
        trace_id = lanying_embedding.create_trace_id()
        lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
        lanying_embedding.update_trace_field(trace_id, "notify_from", to_user_id)
        add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], 'file', -1])
        return f'添加成功，请等待系统处理。'
    else:
        return f'文件ID({file_uuid})不存在'

def bluevector_add_with_preset(msg, config, preset_name, embedding_name, file_uuid):
    return bluevector_add(msg, config, embedding_name, file_uuid)

def bluevector_delete(msg, config, embedding_name, doc_id):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
    if result['result'] == 'error':
        return result['message']
    logging.info(f"receive embedding delete doc command: app_id:{app_id}, from_user_id:{from_user_id}, doc_id:{doc_id}")
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info is None:
        return f'知识库({embedding_name})不存在'
    else:
        doc_info = lanying_embedding.get_doc(embedding_name_info["embedding_uuid"], doc_id)
        if doc_info is None:
            return f"文档ID(doc_id)不存在"
        delete_doc_from_embedding(app_id, embedding_name, doc_id)
        return f'删除成功，请等待系统处理。'

def bluevector_delete_with_preset(msg, config, preset_name, embedding_name, doc_id):
    return bluevector_delete(msg, config, embedding_name, doc_id)

def bluevector_get_metadata(msg, config, doc_id):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    embedding_name = lanying_embedding.get_embedding_name_by_doc_id(app_id, doc_id)
    if embedding_name is None:
        return '文档不存在'
    result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
    if result['result'] == 'error':
        return result['message']
    logging.info(f"receive doc metadata get command: app_id:{app_id}, from_user_id:{from_user_id}, doc_id:{doc_id}")
    result = lanying_embedding.get_doc_metadata(app_id, embedding_name, doc_id)
    if result['result'] == 'error':
        return f'文档metadata获取失败：{result["message"]}'
    metadata = result['data']
    return f'文档metadata为：\n{json.dumps(metadata, ensure_ascii=False)}'

def bluevector_set_metadata(msg, config, doc_id, field, value):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    embedding_name = lanying_embedding.get_embedding_name_by_doc_id(app_id, doc_id)
    if embedding_name is None:
        return '文档不存在'
    result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
    if result['result'] == 'error':
        return result['message']
    logging.info(f"receive doc metadata set command: app_id:{app_id}, from_user_id:{from_user_id}, doc_id:{doc_id}, field:{field}, value:{value}")
    result = lanying_embedding.get_doc_metadata(app_id, embedding_name, doc_id)
    if result['result'] == 'error':
        return f'文档metadata设置失败：{result["message"]}'
    metadata = result['data']
    metadata[field] = value
    lanying_embedding.set_doc_metadata(app_id, embedding_name, doc_id, metadata)
    return f'文档metadata设置成功'

def bluevector_status(msg, config, embedding_name):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
    if result['result'] == 'error':
        return result['message']
    result = []
    total, doc_list = lanying_embedding.get_embedding_doc_info_list(app_id, embedding_name, -20, -1)
    for doc in doc_list:
        result.append(f"文档ID:{doc['doc_id']}, 文件名:{doc['filename']}, 状态:{doc['status']}, 进度：{doc.get('progress_finish', '-')}/{doc.get('progress_total', '-')}")
    return f"文档总数:{total}\n最近文档列表为:\n" + "\n".join(result)

def bluevector_status_with_preset(msg, config, preset_name, embedding_name):
    return bluevector_status(msg, config, embedding_name)

def bluevector_info(msg, config):
    return bluevector_info_by_preset(msg, config, "default")

def bluevector_info_by_preset(msg, config, preset_name):
    app_id = msg['appId']
    embedding_infos = lanying_embedding.get_preset_embedding_infos(config.get('embeddings'), app_id, preset_name)
    if len(embedding_infos) == 0:
        return "当前预设未绑定知识库"
    result = ["当前预设绑定的知识库为："]
    for embedding_info in embedding_infos:
        embedding_name = embedding_info['embedding_name']
        embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
        embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_name_info['embedding_uuid'])
        max_block_size = int(embedding_uuid_info.get("max_block_size", "350"))
        storage_file_size = int(embedding_uuid_info.get("storage_file_size", "0"))
        result.append(f"知识库名称: {embedding_name}, 分片大小:{max_block_size}字, 存储空间:{round(storage_file_size / 1024 / 1024, 3)}MB")
    return "\n".join(result)

def bluevector_help(msg, config):
    from_user_id = int(msg['from']['uid'])
    to_user_id = int(msg['to']['uid'])
    app_id = msg['appId']
    if lanying_embedding.is_app_embedding_admin_user(app_id, from_user_id):
        return lanying_command.pretty_help(app_id, to_user_id)
    else:
        return f"无法执行此命令，用户（ID：{from_user_id}）不是企业知识库管理员。"

def bluevector_error(msg, config):
    return '错误：命令格式不正确。\n可以使用 /help 或者 /+空格 查看命令说明。'

def help(msg, config):
    app_id = msg['appId']
    to_user_id = int(msg['to']['uid'])
    return lanying_command.pretty_help(app_id, to_user_id)

def search_on_doc_by_default_preset(msg, config, doc_id, new_content):
    return search_on_doc_by_preset(msg, config, "default", doc_id, new_content)

def search_on_doc_by_preset(msg, config, preset_name, doc_id, new_content):
    app_id = msg['appId']
    embedding_infos = lanying_embedding.get_preset_embedding_infos(config.get('embeddings'), app_id, preset_name)
    result = "not_bind"
    for embedding_info in embedding_infos:
        embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
        if 'embedding_uuid' in embedding_info and embedding_info['embedding_uuid'] == embedding_uuid_from_doc_id:
            doc_info = lanying_embedding.get_doc(embedding_uuid_from_doc_id, doc_id)
            if doc_info:
                result = "found"
            else:
                result = "not_exist"
    if result == "not_bind":
        return "文档ID所在知识库未绑定到此预设"
    elif result == "not_exist":
        return "文档ID不存在"
    else:
        return {'result':'continue', 'command_ext':{'preset_name':preset_name, "doc_id":doc_id, "new_content":new_content}}
    
def search_on_fulldoc_by_default_preset(msg, config, doc_id, new_content):
    return search_on_fulldoc_by_preset(msg, config, "default", doc_id, new_content)

def search_on_fulldoc_by_preset(msg, config, preset_name, doc_id, new_content):
    app_id = msg['appId']
    embedding_infos = lanying_embedding.get_preset_embedding_infos(config.get('embeddings'), app_id, preset_name)
    result = "not_bind"
    for embedding_info in embedding_infos:
        embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
        if 'embedding_uuid' in embedding_info and embedding_info['embedding_uuid'] == embedding_uuid_from_doc_id:
            doc_info = lanying_embedding.get_doc(embedding_uuid_from_doc_id, doc_id)
            if doc_info:
                result = "found"
            else:
                result = "not_exist"
    if result == "not_bind":
        return "文档ID所在知识库未绑定到此预设"
    elif result == "not_exist":
        return "文档ID不存在"
    else:
        return {'result':'continue', 'command_ext':{'preset_name':preset_name, "doc_id":doc_id, "new_content":new_content, "is_fulldoc": True}}

def search_by_preset(msg, config, preset_name, new_content):
    return {'result':'continue', 'command_ext':{'preset_name':preset_name, "new_content":new_content}}

def add_doc_to_embedding(app_id, embedding_name, dname, url, type, limit, max_depth, filters, urls, generate_lanying_links):
    config = lanying_config.get_lanying_connector(app_id)
    if lanying_chatbot.is_chatbot_mode(app_id):
        user_id = lanying_chatbot.get_default_user_id(app_id)
    else:
        user_id = config['lanying_user_id']
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': str(user_id)}
    embedding_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_info:
        trace_id = lanying_embedding.create_trace_id()
        opts = {
            'generate_lanying_links': generate_lanying_links == True
        }
        if type == 'site':
            embedding_uuid = embedding_info['embedding_uuid']
            task_id = lanying_embedding.create_task(embedding_uuid, type, urls)
            lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", config['access_token'])
            site_task_id = lanying_url_loader.create_task(urls)
            lanying_embedding.update_task_field(embedding_uuid, task_id, "site_task_id", site_task_id)
            lanying_embedding.update_task_field(embedding_uuid, task_id, "generate_lanying_links", str(generate_lanying_links == True))
            prepare_site.apply_async(args = [trace_id, app_id, embedding_uuid, '.html', type, site_task_id, urls, 0, limit, task_id, max_depth, filters, opts])
        else:
            result = add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], type, limit, opts])
            logging.info(f"add_doc_to_embedding | result={result}")

def continue_embedding_task(app_id, embedding_name, task_id):
    config = lanying_config.get_lanying_connector(app_id)
    embedding_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_info:
        embedding_uuid = embedding_info['embedding_uuid']
        task_info = lanying_embedding.get_task(embedding_uuid, task_id)
        if task_info:
            if task_info["status"] != "finish":
                return {'result':'error', 'message': 'task_status_must_be_finish'}
            else:
                lanying_embedding.update_task_field(embedding_uuid,task_id, "status", "adding")
                trace_id = lanying_embedding.create_trace_id()
                lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", config['access_token'])
                total_num = lanying_embedding.get_task_details_count(embedding_uuid, task_id)
                lanying_embedding.update_task_field(embedding_uuid, task_id, "processing_total_num", total_num)
                continue_site_task.apply_async(args = [trace_id, app_id, embedding_uuid, task_id])
    return {'result':'ok'}

def delete_embedding_task(app_id, embedding_name, task_id):
    embedding_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_info:
        embedding_uuid = embedding_info['embedding_uuid']
        lanying_embedding.delete_task(embedding_uuid, task_id)

def re_run_doc_to_embedding(app_id, embedding_name, doc_id):
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info is None:
        return {'result':'error','message':'embedding_name not exist'}
    embedding_uuid = embedding_name_info["embedding_uuid"]
    doc_info = lanying_embedding.get_doc(embedding_uuid, doc_id)
    if doc_info is None:
        return {'result':'error', 'message':'doc_id not exist'}
    lanying_embedding.update_doc_field(embedding_uuid, doc_id, "status", "wait")
    config = lanying_config.get_lanying_connector(app_id)
    lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", config['access_token'])
    trace_id = lanying_embedding.create_trace_id()
    re_run_doc_to_embedding_by_doc_ids.apply_async(args = [trace_id, app_id, embedding_uuid, [doc_id]])
    return {'result':'ok', 'data':True}

def re_run_all_doc_to_embedding(app_id, embedding_name):
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info is None:
        return {'result':'error','message':'embedding_name not exist'}
    embedding_uuid = embedding_name_info["embedding_uuid"]
    trace_id = lanying_embedding.create_trace_id()
    doc_ids = lanying_embedding.get_embedding_doc_id_list(embedding_uuid, 0, -1)
    config = lanying_config.get_lanying_connector(app_id)
    lanying_embedding.update_embedding_uuid_info(embedding_uuid, "openai_secret_key", config['access_token'])
    re_run_doc_to_embedding_by_doc_ids.apply_async(args = [trace_id, app_id, embedding_uuid, doc_ids])
    return {'result':'ok', 'data':True}

def delete_doc_from_embedding(app_id, embedding_name, doc_id):
    return lanying_embedding.delete_doc_from_embedding(app_id, embedding_name, doc_id, delete_doc_data)

def get_embedding_usage(app_id):
    return lanying_embedding.get_embedding_usage(app_id)

def set_embedding_usage(app_id, storage_file_size_max):
    return lanying_embedding.set_embedding_usage(app_id, storage_file_size_max)

def save_attachment(from_user_id, attachment):
    redis = lanying_redis.get_redis_connection()
    file_id = redis.incrby(f"lanying-connector:attachment_id:{from_user_id}", 1)
    redis.setex(f"lanying-connnector:last_attachment:{from_user_id}:{file_id}", 86400, attachment)
    return file_id

def get_attachment(from_user_id, file_id):
    redis = lanying_redis.get_redis_connection()
    return redis.get(f"lanying-connnector:last_attachment:{from_user_id}:{file_id}")

def check_upload_embedding(msg, config, ext, app_id):
    from_user_id = int(msg['from']['uid'])
    if lanying_embedding.is_app_embedding_admin_user(app_id, from_user_id):
        allow_exts  = lanying_embedding.allow_exts()
        allow_exts.append(".zip")
        if ext in allow_exts:
            return {'result':'ok'}
        else:
            return {'result':'error', 'message': f'对不起，暂时只支持{allow_exts}格式的知识库'}
    else:
        return {'result':'error', 'message':'对不起，我无法处理文件消息'}

def check_can_manage_embedding(app_id, embedding_name, from_user_id):
    embedding_name_info = lanying_embedding.get_embedding_name_info(app_id, embedding_name)
    if embedding_name_info:
        admin_user_ids_str = embedding_name_info.get("admin_user_ids", "")
        admin_user_ids = admin_user_ids_str.split(',')
        for admin_user_id in admin_user_ids:
            if admin_user_id != '' and int(admin_user_id) == from_user_id:
                return {'result':'ok'}
    return {'result':'error', 'message':f'知识库不存在，或者你（ID：{from_user_id}）没有这个知识库的权限'}

def text_byte_size(text):
    return len(text.encode('utf-8'))

def calc_embedding_query_text(config, content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId, model_config):
    if embedding_history_num <= 0:
        return content
    result = [content]
    now = int(time.time())
    history_count = 0
    history_size = lanying_embedding.num_of_tokens(content)
    token_limit = model_token_limit(model_config)
    redis = lanying_redis.get_redis_connection()
    msg_type = config['reply_msg_type']
    for historyStr in reversed(getHistoryList(redis, historyListKey)):
        history = json.loads(historyStr)
        if history['time'] < now - expireSeconds:
            removeHistory(redis, historyListKey, historyStr)
        else:
            if history_count < embedding_history_num:
                if 'list' in history:
                    pass
                else:
                    if msg_type == 'CHAT':
                        history_content = history.get('user', '')
                    else:
                        history_content = history.get('content', '')
                    if len(history_content) > 0:
                        token_num = lanying_embedding.num_of_tokens(history_content) + 1
                        if history_size + token_num < token_limit:
                            history_count += 1
                            result.append(history_content)
                            history_size += token_num
                        else:
                            break
            else:
                break
    embedding_query_text = '\n'.join(result)
    if is_debug:
        replyMessageAsync(config, f"[蓝莺AI] 使用问题历史算向量:\n{embedding_query_text}",{'ai':{'role': 'ai', 'is_debug_msg': True}})
    return embedding_query_text

def handle_chat_file(msg, config):
    from_user_id = int(msg['from']['uid'])
    to_user_id = int(msg['to']['uid'])
    app_id = msg['appId']
    attachment_str = msg['attachment']
    attachment = json.loads(attachment_str)
    dname = attachment['dName']
    ext = lanying_embedding.parse_file_ext(dname)
    check_result = check_upload_embedding(msg, config, ext, app_id)
    if check_result['result'] == 'error':
        return check_result['message']
    logging.info(f"recevie embedding file from chat: app_id:{app_id}, attachment_str:{attachment_str}")
    embedding_names = lanying_embedding.list_embedding_names(app_id)
    can_manage_embedding_names = []
    for now_embedding_name in embedding_names:
        result = check_can_manage_embedding(app_id, now_embedding_name, from_user_id)
        if result['result'] == 'ok':
            can_manage_embedding_names.append(now_embedding_name)
    if len(can_manage_embedding_names) == 1:
        embedding_name = can_manage_embedding_names[0]
        logging.info(f"choose embedding_name for unique: {can_manage_embedding_names}")
        url = attachment['url']
        dname = attachment['dName']
        headers = {'app_id': app_id,
                'access-token': config['lanying_admin_token'],
                'user_id': str(to_user_id)}
        trace_id = lanying_embedding.create_trace_id()
        lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
        lanying_embedding.update_trace_field(trace_id, "notify_from", to_user_id)
        metadata = parse_metadata(msg)
        add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], 'file', -1, {'metadata': metadata}])
        return f'添加到知识库({embedding_name})成功，请等待系统处理。'
    else:
        default_embedding_name = get_user_default_embedding_name(app_id, from_user_id)
        if default_embedding_name and default_embedding_name in can_manage_embedding_names:
            logging.info(f"choose embedding_name for default: default:{default_embedding_name}, all:{can_manage_embedding_names}")
            embedding_name = default_embedding_name
            url = attachment['url']
            dname = attachment['dName']
            headers = {'app_id': app_id,
                    'access-token': config['lanying_admin_token'],
                    'user_id': str(to_user_id)}
            trace_id = lanying_embedding.create_trace_id()
            lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
            lanying_embedding.update_trace_field(trace_id, "notify_from", to_user_id)
            metadata = parse_metadata(msg)
            add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], 'file', -1, {'metadata': metadata}])
            return f'添加到知识库({embedding_name})成功，请等待系统处理。'
        else:
            file_id = save_attachment(from_user_id, attachment_str)
            return f'上传文件成功， 文件ID:{file_id} 。\n您绑定了多个知识库{can_manage_embedding_names}, 可以设置默认知识库来自动添加文档到知识库,\n命令格式为：/bluevector mode auto <KNOWLEDGE_BASE_NAME>'

def parse_metadata(msg):
    metadata = {}
    try:
        ext = json.loads(msg["ext"])
        for k,v in ext["ai"]["metadata"].items():
            try:
                metadata[str(k)] = str(v)
            except Exception as ee:
                pass
    except Exception as e:
        pass
    return metadata

def handle_chat_links(msg, config):
    from_user_id = int(msg['from']['uid'])
    to_user_id = int(msg['to']['uid'])
    app_id = msg['appId']
    content = msg['content']
    fields = re.split("[ \r\n]{1,}", content)
    urls = []
    for field in fields:
        if field.startswith("https://") or field.startswith("http://"):
            urls.append(field)
    if len(urls) == 0:
        return ""
    if not lanying_embedding.is_app_embedding_admin_user(app_id, from_user_id):
        return ""
    embedding_names = lanying_embedding.list_embedding_names(app_id)
    can_manage_embedding_names = []
    for embedding_name in embedding_names:
        result = check_can_manage_embedding(app_id, embedding_name, from_user_id)
        if result['result'] == 'ok':
            can_manage_embedding_names.append(embedding_name)
    if len(can_manage_embedding_names) == 0:
        return ""
    elif len(can_manage_embedding_names) == 1:
        embedding_name = can_manage_embedding_names[0]
        for url in urls:
            headers = {}
            trace_id = lanying_embedding.create_trace_id()
            lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
            lanying_embedding.update_trace_field(trace_id, "notify_from", to_user_id)
            metadata = parse_metadata(msg)
            add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, 'url.html', config['access_token'], 'url', -1, {'metadata': metadata}])
        return f'添加到知识库({embedding_name})成功，请等待系统处理。'
    else:
        default_embedding_name = get_user_default_embedding_name(app_id, from_user_id)
        if default_embedding_name and default_embedding_name in can_manage_embedding_names:
            for url in urls:
                headers = {}
                trace_id = lanying_embedding.create_trace_id()
                lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
                lanying_embedding.update_trace_field(trace_id, "notify_from", to_user_id)
                metadata = parse_metadata(msg)
                add_embedding_file.apply_async(args = [trace_id, app_id, default_embedding_name, url, headers, 'url.html', config['access_token'], 'url', -1,{'metadata': metadata}])
            return f'添加到知识库({default_embedding_name})成功，请等待系统处理。'
        else:
            return f"您绑定了多个知识库{can_manage_embedding_names}，请设置默认知识库名称， 命令格式为：/bluevector mode auto <KNOWLEDGE_BASE_NAME>"

def get_user_default_embedding_name(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    key = user_default_embedding_name_key(app_id, user_id)
    result = redis.get(key)
    if result:
        return result.decode('utf-8')
    else:
        return None

def user_default_embedding_name_key(app_id, user_id):
    return f'lanying_connector:user_default_embedding_name:{app_id}:{user_id}'
def list_models():
    return lanying_vendor.list_models()

def stream_lines_to_response(preset, reply, vendor, usage, stream_function_name, stream_function_args):
    if 'total_tokens' in usage:
        total_tokens = usage['total_tokens']
        prompt_tokens = usage.get('prompt_tokens', 0)
        completion_tokens = usage.get('completion_tokens', total_tokens - prompt_tokens)
    else:
        prompt_tokens = calcMessagesTokens(preset.get('messages',[]), preset['model'], vendor) + lanying_embedding.calc_functions_tokens(preset.get('functions',[]), preset['model'], vendor)
        completion_tokens = calcMessageTokens({'role':'assistant', 'content':reply}, preset['model'], vendor)
        total_tokens = prompt_tokens + completion_tokens
    response = {
        'reply': reply,
        'usage':{
            'completion_tokens' : completion_tokens,
            'prompt_tokens' : prompt_tokens,
            'total_tokens' : total_tokens
        }
    }
    if stream_function_name != "":
        response["function_call"] = {
            "name": stream_function_name,
            "arguments": stream_function_args
        }
    logging.info(f"stream_lines_to_response | response:{response}")
    return response

def maybe_add_history(config, msg):
    if need_add_history(config, msg):
        app_id = msg['appId']
        redis = lanying_redis.get_redis_connection()
        now = int(time.time())
        history = {'time':now}
        content = msg.get('content', '')
        msg_type = msg['type']
        if msg_type == 'CHAT':
            ai_user_id = msg['from']['uid']
            human_user_id = msg['to']['uid']
            historyListKey = historyListChatGPTKey(app_id, human_user_id, ai_user_id)
            history['user'] = ''
            history['assistant'] = content
            history['uid'] = human_user_id
            history['type'] = 'reply'
            addHistory(redis, historyListKey, history)
        elif msg_type == 'GROUPCHAT':
            from_user_id = msg['from']['uid']
            group_id = msg['to']['uid']
            historyListKey = historyListGroupKey(app_id, group_id)
            history['type'] = 'group'
            history['content'] = content
            history['group_id'] = group_id
            history['from'] = from_user_id
            msg_config = lanying_utils.safe_json_loads(msg.get('config'))
            mention_list = msg_config.get('mentionList', [])
            history['mention_list'] = mention_list
            addHistory(redis, historyListKey, history)

def is_chatbot_audio_to_text_on(config):
    if 'chatbot' in config:
        chatbot = config['chatbot']
        if chatbot['audio_to_text'] == 'on':
            return True
    return False

def maybe_transcription_audio_msg(config, msg):
    app_id = msg['appId']
    ctype = msg.get('ctype')
    from_user_id = str(msg['from']['uid'])
    content = msg.get('content', '')
    if ctype == 'AUDIO' and content == '' and is_chatbot_audio_to_text_on(config):
        attachment = lanying_utils.safe_json_loads(msg.get('attachment',''))
        url = attachment.get('url', '')
        if len(url) > 0:
            if lanying_utils.is_lanying_url(url):
                url += '&format=mp3'
            audio_filename = f"/tmp/audio_{app_id}_{from_user_id}_{int(time.time())}_{uuid.uuid4()}.mp3"
            res = lanying_im_api.download_url(config, app_id, from_user_id, url, audio_filename)
            logging.info(f"transcription_audio_msg result: {res}")
            if res['result'] == 'ok':
                res = speech_to_text(config, audio_filename)
                logging.info(f"speech_to_text | result: {res}")
                if res['result'] == 'ok':
                    audio_text = res['data']['text']
                    logging.info(f"mark audio msg text: msg:{msg}, audio_text:{audio_text}")
                    msg['content'] = audio_text
                    executor.submit(add_audio_msg_text, config, msg)

def speech_to_text(config, audio_filename):
    try:
        headers = {
            "Authorization": f"Bearer {config['access_token']}"
        }
        data = {
            "model": config['audio_to_text_model'],
            "response_format": "verbose_json"
        }
        files = {'file': ('audio.mp3', open(audio_filename, 'rb'))}
        url = global_lanying_connector_server + '/v1/audio/transcriptions'
        response = requests.post(url, headers=headers, data=data, files=files)
        if response.status_code == 200:
            return {'result': 'ok', 'data': response.json()}
        else:
            return {'result': 'error', 'message': 'bad_status_code', 'text': response.text}
    except Exception as e:
        logging.exception(e)
        return {'result': 'error', 'message': 'exception'}

def add_audio_msg_text(config, msg):
    app_id = msg['appId']
    from_user_id = str(msg['from']['uid'])
    to_user_id = str(msg['to']['uid'])
    msg_type = msg['type']
    msg_id = msg['msgId']
    content_type = 12
    content = msg.get('content', '')
    extra = {
        'ext': lanying_utils.safe_json_loads(msg.get("ext", '')),
        'attachment': lanying_utils.safe_json_loads(msg.get("attachment", '')),
        'config': lanying_utils.safe_json_loads(msg.get("config", '')),
        'related_mid': msg_id
    }
    if msg_type == 'CHAT':
        return lanying_im_api.send_message_async(config, app_id, from_user_id, to_user_id, 1, content_type, content, extra)
    else:
        return lanying_im_api.send_message_async(config, app_id, from_user_id, to_user_id, 2, content_type, content, extra)

def text_to_speech(config, content, audio_filename):
    url = global_lanying_connector_server + '/v1/audio/speech'
    headers = {
        "Authorization": f"Bearer {config['access_token']}"
    }
    data = {
        'model': config['text_to_audio_model'],
        'input': content,
        'voice': config['text_to_audio_voice']
    }
    logging.info(f"text_to_speech | data={data}")
    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        with open(audio_filename, 'wb') as f:
            f.write(response.content)
            return {'result':'ok'}
    else:
        logging.info("出现错误，状态码：", response.status_code)
    return {'result': 'error', 'message': 'fail to transform text to speech'}

def need_add_history(config, msg):
    try:
        fromUserId = str(msg['from']['uid'])
        toUserId = str(msg['to']['uid'])
        type = msg['type']
        app_id = msg['appId']
        ctype = msg.get('ctype')
        allow_ctypes = ['TEXT']
        if is_chatbot_audio_to_text_on(config):
            allow_ctypes.append('AUDIO')
        if ctype not in allow_ctypes:
            logging.info(f"need_add_history skip for ctype not in{allow_ctypes}")
            return False
        content = msg.get('content', '')
        if content == '':
            return False
        if type == 'CHAT':
            is_chatbot = is_chatbot_user_id(app_id, fromUserId, config)
            if is_chatbot and toUserId != fromUserId:
                ext = msg.get('ext','')
                try:
                    ext_json = json.loads(ext)
                except Exception as e:
                    ext_json = {}
                try:
                    role = ext_json.get('ai', {}).get('role', 'none')
                    if role == 'ai':
                        return False
                except Exception as e:
                    pass
                return True
        elif type == 'GROUPCHAT':
            ext = msg.get('ext','')
            try:
                ext_json = json.loads(ext)
            except Exception as e:
                ext_json = {}
            try:
                role = ext_json.get('ai', {}).get('role', 'none')
                if role == 'ai':
                    return False
            except Exception as e:
                pass
            try:
                is_debug_msg = ext_json.get('ai', {}).get('is_debug_msg', False)
                if is_debug_msg == True:
                    return False
            except Exception as e:
                pass
            return True
    except Exception as e:
        pass
    return False

@bp.route("/service/openai/create_ai_plugin", methods=["POST"])
def create_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_name = str(data['plugin_name'])
    result = lanying_ai_plugin.create_ai_plugin(app_id, plugin_name)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/list_ai_plugins", methods=["POST"])
def list_ai_plugins():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_ai_plugin.list_ai_plugins(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/list_ai_functions", methods=["POST"])
def list_ai_functions():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    start = int(data.get('start', 0))
    end = int(data.get('end', 20))
    result = lanying_ai_plugin.list_ai_functions(app_id, plugin_id, start, end)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/add_ai_function_to_ai_plugin", methods=["POST"])
def add_ai_function_to_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    name = str(data['name'])
    description = str(data['description'])
    parameters = dict(data['parameters'])
    function_call = dict(data['function_call'])
    priority = int(data.get('priority', 10))
    result = lanying_ai_plugin.add_ai_function_to_ai_plugin(app_id, plugin_id, name, description, parameters, function_call, priority)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/delete_ai_function_from_ai_plugin", methods=["POST"])
def delete_ai_function_from_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    function_id = str(data['function_id'])
    result = lanying_ai_plugin.delete_ai_function_from_ai_plugin(app_id, plugin_id, function_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/configure_ai_plugin", methods=["POST"])
def configure_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    name = str(data['name'])
    headers = dict(data.get('headers',{}))
    params = dict(data.get('params',{}))
    envs = dict(data.get('envs',{}))
    endpoint = str(data.get('endpoint', ''))
    auth = dict(data.get('auth',{}))
    result = lanying_ai_plugin.configure_ai_plugin(app_id, plugin_id, name, endpoint, headers, envs, params, auth)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/delete_ai_plugin", methods=["POST"])
def delete_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    result = lanying_ai_plugin.delete_ai_plugin(app_id, plugin_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/configure_ai_function", methods=["POST"])
def configure_ai_function():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    function_id = str(data['function_id'])
    priority = int(data.get('priority', 10))
    name = str(data['name'])
    description = str(data['description'])
    parameters = dict(data['parameters'])
    function_call = dict(data['function_call'])
    result = lanying_ai_plugin.configure_ai_function(app_id, plugin_id, function_id, name, description, parameters,function_call, priority)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/bind_ai_plugin", methods=["POST"])
def bind_ai_plugin():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    type = str(data['type'])
    name = str(data['name'])
    value_list = list(data['list'])
    result = lanying_ai_plugin.bind_ai_plugin(app_id, type, name, value_list)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/get_ai_plugin_bind_relation", methods=["POST"])
def get_ai_plugin_bind_relation():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_ai_plugin.get_ai_plugin_bind_relation(app_id)
    resp = make_response({'code':200, 'data':result})
    return resp

@bp.route("/service/openai/get_ai_plugin_embedding", methods=["POST"])
def get_ai_plugin_embedding():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_ai_plugin.get_ai_plugin_embedding(app_id)
    resp = make_response({'code':200, 'data':result})
    return resp

@bp.route("/service/openai/configure_ai_plugin_embedding", methods=["POST"])
def configure_ai_plugin_embedding():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    embedding_max_tokens = int(data['embedding_max_tokens'])
    embedding_max_blocks = int(data['embedding_max_blocks'])
    vendor = str(data['vendor'])
    model = str(data.get('model', ''))
    result = lanying_ai_plugin.configure_ai_plugin_embedding(app_id, embedding_max_tokens, embedding_max_blocks, vendor, model)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/plugin_export", methods=["POST"])
def plugin_export():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    plugin_id = str(data['plugin_id'])
    result = lanying_ai_plugin.plugin_export(app_id, plugin_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/plugin_import", methods=["POST"])
def plugin_import():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    type = str(data['type'])
    public_id = str(data.get('public_id', ''))
    url = str(data.get('url', ''))
    if type == 'public_id' and len(public_id) > 0:
        result = plugin_import_by_public_id(app_id, public_id)
    elif type == 'file' and len(url) > 0:
        result = plugin_import_by_url(type, app_id, url)
    elif type == 'swagger' and len(url) > 0:
        result = plugin_import_by_url(type, app_id, url)
    else:
        result = {'result': 'error', 'message':'need args: type, url or public_id'}
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/list_public_plugins", methods=["POST"])
def list_public_plugins():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    type = str(data.get('type', 'normal'))
    result = lanying_ai_plugin.list_public_plugins(type)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/create_chatbot", methods=["POST"])
def create_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    name = str(data['name'])
    nickname = str(data.get('nickname', ''))
    desc = str(data['desc'])
    avatar = str(data.get('avatar', ''))
    user_id = int(data['user_id'])
    lanying_link = str(data['lanying_link'])
    preset = dict(data['preset'])
    history_msg_count_max = int(data['history_msg_count_max'])
    history_msg_count_min = int(data['history_msg_count_min'])
    history_msg_size_max = int(data['history_msg_size_max'])
    message_per_month_per_user = int(data['message_per_month_per_user'])
    chatbot_ids = data.get('chatbot_ids', [])
    welcome_message = str(data.get('welcome_message','嘿，你好'))
    quota_exceed_reply_type = str(data.get('quota_exceed_reply_type', 'capsule'))
    quota_exceed_reply_msg = str(data.get('quota_exceed_reply_msg', ''))
    group_history_use_mode = str(data.get('group_history_use_mode', 'all'))
    image_vision = str(data.get('image_vision', 'off'))
    audio_to_text = str(data.get('audio_to_text', 'off'))
    audio_to_text_model = str(data.get('audio_to_text_model', 'whisper-1'))
    result = lanying_chatbot.create_chatbot(app_id, name, nickname, desc, avatar, user_id, lanying_link,
                                            preset, history_msg_count_max, history_msg_count_min, history_msg_size_max,
                                            message_per_month_per_user, chatbot_ids, welcome_message, quota_exceed_reply_type,
                                            quota_exceed_reply_msg, group_history_use_mode,
                                            audio_to_text, image_vision, audio_to_text_model)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/configure_chatbot", methods=["POST"])
def configure_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    chatbot_id = str(data['chatbot_id'])
    name = str(data['name'])
    nickname = str(data.get('nickname', ''))
    desc = str(data['desc'])
    avatar = str(data.get('avatar', ''))
    user_id = int(data['user_id'])
    lanying_link = str(data['lanying_link'])
    preset = dict(data['preset'])
    history_msg_count_max = int(data['history_msg_count_max'])
    history_msg_count_min = int(data['history_msg_count_min'])
    history_msg_size_max = int(data['history_msg_size_max'])
    message_per_month_per_user = int(data['message_per_month_per_user'])
    chatbot_ids = data.get('chatbot_ids', [])
    welcome_message = str(data.get('welcome_message',''))
    quota_exceed_reply_type = str(data.get('quota_exceed_reply_type', 'capsule'))
    quota_exceed_reply_msg = str(data.get('quota_exceed_reply_msg', ''))
    group_history_use_mode = str(data.get('group_history_use_mode', 'all'))
    image_vision = str(data.get('image_vision', 'off'))
    audio_to_text = str(data.get('audio_to_text', 'off'))
    audio_to_text_model = str(data.get('audio_to_text_model', 'whisper-1'))
    result = lanying_chatbot.configure_chatbot(app_id, chatbot_id, name, nickname, desc, avatar, user_id, lanying_link,
                                               preset, history_msg_count_max, history_msg_count_min, history_msg_size_max,
                                               message_per_month_per_user, chatbot_ids,welcome_message, quota_exceed_reply_type,
                                               quota_exceed_reply_msg, group_history_use_mode,
                                               audio_to_text, image_vision, audio_to_text_model)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/get_chatbot_avatar_upload_url", methods=["POST"])
def get_chatbot_avatar_upload_url():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    user_id = int(data['user_id'])
    result = lanying_im_api.get_user_avatar_upload_url(app_id, user_id)
    if result.get('code') != 200:
        resp = make_response({'code':result['code'], 'message':result['message']})
    else:
        data = result["data"]
        real_download_url = lanying_im_api.get_avatar_real_download_url(app_id, user_id, data['download_url'])
        data['real_download_url'] = real_download_url
        resp = make_response({'code':200, 'data':data})
    return resp

@bp.route("/service/openai/get_chatbot", methods=["POST"])
def get_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    chatbot_id = str(data['chatbot_id'])
    result = lanying_chatbot.get_chatbot_dto(app_id, chatbot_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/is_chatbot_mode", methods=["POST"])
def is_chatbot_mode():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_chatbot.is_chatbot_mode(app_id)
    resp = make_response({'code':200, 'data': result})
    return resp

@bp.route("/service/openai/set_chatbot_mode", methods=["POST"])
def set_chatbot_mode():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    mode = bool(data['mode'])
    result = lanying_chatbot.set_chatbot_mode(app_id, mode)
    resp = make_response({'code':200, 'data': result})
    return resp

# @bp.route("/service/openai/set_chatbot_status", methods=["POST"])
# def set_chatbot_status():
#     if not check_access_token_valid():
#         resp = make_response({'code':401, 'message':'bad authorization'})
#         return resp
#     text = request.get_data(as_text=True)
#     data = json.loads(text)
#     app_id = str(data['app_id'])
#     chatbot_id = str(data['chatbot_id'])
#     status = str(data['status'])
#     lanying_chatbot.set_chatbot_field(app_id, chatbot_id, "status", status)
#     resp = make_response({'code':200, 'data': {'success':True}})
#     return resp

@bp.route("/service/openai/get_default_user_id", methods=["POST"])
def get_default_user_id():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    default_user_id = lanying_chatbot.get_default_user_id(app_id)
    if not default_user_id:
        default_user_id = lanying_config.get_lanying_user_id(app_id)
    if not default_user_id:
        default_user_id = 0
    resp = make_response({'code':200, 'data': default_user_id})
    return resp

@bp.route("/service/openai/list_chatbots", methods=["POST"])
def list_chatbots():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    chatbots = lanying_chatbot.list_chatbots(app_id)
    dtos = []
    for chatbot in chatbots:
        linked_embedding_names = []
        linked_plugin_names = []
        if 'linked_capsule_id' in chatbot:
            capsule = lanying_ai_capsule.get_capsule(chatbot['linked_capsule_id'])
            if capsule:
                capsule_info = make_linked_capsule_info(capsule)
                linked_embedding_names.extend(capsule_info['embedding_names'])
                linked_plugin_names.extend(capsule_info['plugin_names'])
        if 'linked_publish_capsule_id' in chatbot:
            capsule = lanying_ai_capsule.get_publish_capsule(chatbot['linked_publish_capsule_id'])
            if capsule:
                capsule_info = make_linked_capsule_info(capsule)
                linked_embedding_names.extend(capsule_info['embedding_names'])
                linked_plugin_names.extend(capsule_info['plugin_names'])
        chatbot['linked_embedding_names'] = linked_embedding_names
        chatbot['linked_plugin_names'] = linked_plugin_names
        dtos.append(chatbot)
    resp = make_response({'code':200, 'data':{'list': dtos}})
    return resp

def make_linked_capsule_info(capsule):
    app_id = capsule['app_id']
    chatbot = lanying_chatbot.get_chatbot(app_id, capsule['chatbot_id'])
    embedding_names = []
    plugin_names = []
    if chatbot:
        config = lanying_config.get_lanying_connector(app_id)
        embedding_uuids = config.get('embeddings',{}).get(chatbot['name'], [])
        for embedding_uuid in embedding_uuids:
            embedding_uuid_info = lanying_embedding.get_embedding_uuid_info(embedding_uuid)
            if embedding_uuid_info:
                embedding_names.append(embedding_uuid_info['embedding_name'])
        plugin_relation = lanying_ai_plugin.get_ai_plugin_bind_relation(app_id)
        plugin_ids = plugin_relation.get(chatbot['name'],[])
        for plugin_id in plugin_ids:
            plugin = lanying_ai_plugin.get_ai_plugin(app_id, plugin_id)
            if plugin:
                plugin_names.append(plugin['name'])
    return {'embedding_names': embedding_names, "plugin_names": plugin_names}

@bp.route("/service/openai/bind_embedding", methods=["POST"])
def bind_embedding():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    type = str(data['type'])
    name = str(data['name'])
    value_list = list(data['list'])
    result = lanying_chatbot.bind_embedding(app_id, type, name, value_list)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/get_embedding_bind_relation", methods=["POST"])
def get_embedding_bind_relation():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_chatbot.get_embedding_bind_relation(app_id)
    resp = make_response({'code':200, 'data':result})
    return resp

@bp.route("/service/openai/share_capsule", methods=["POST"])
def share_capsule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    chatbot_id = str(data['chatbot_id'])
    name = str(data['name'])
    desc = str(data['desc'])
    link = str(data['link'])
    password = str(data['password'])
    month_price = int(data.get('month_price', '0'))
    year_price = int(data.get('year_price', '0'))
    result = lanying_ai_capsule.share_capsule(app_id, chatbot_id, name, desc, link, password, month_price, year_price)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/get_capsule_info", methods=["POST"])
def get_capsule_info():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    capsule_id = str(data['capsule_id'])
    capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if capsule is None:
        resp = make_response({'code':400, 'message':'capsule not exist'})
    else:
        if len(capsule['password']) > 0:
            capsule['password'] = '******'
        dto = {}
        for k,v in capsule.items():
            if k not in ["app_id", "chatbot_id"]:
                dto[k] = v
        resp = make_response({'code':200, 'data':dto})
    return resp

@bp.route("/service/openai/get_capsule_all_info", methods=["POST"])
def get_capsule_all_info():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    capsule_id = str(data['capsule_id'])
    capsule = lanying_ai_capsule.get_capsule(capsule_id)
    if capsule is None:
        resp = make_response({'code':400, 'message':'capsule not exist'})
    else:
        resp = make_response({'code':200, 'data':capsule})
    return resp

@bp.route("/service/openai/list_app_capsules", methods=["POST"])
def list_app_capsules():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_ai_capsule.list_app_capsules(app_id)
    resp = make_response({'code':200, 'data':{'list': result}})
    return resp

@bp.route("/service/openai/check_create_chatbot_from_capsule", methods=["POST"])
def check_create_chatbot_from_capsule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    capsule_id = str(data['capsule_id'])
    password = str(data['password'])
    cycle_type = str(data.get('cycle_type', 'month'))
    price = int(data.get('price', '0'))
    result = lanying_chatbot.check_create_chatbot_from_capsule(app_id, capsule_id, password, cycle_type, price)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/create_chatbot_from_capsule", methods=["POST"])
def create_chatbot_from_capsule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    capsule_id = str(data['capsule_id'])
    password = str(data['password'])
    cycle_type = str(data.get('cycle_type', 'month'))
    price = int(data.get('price', '0'))
    user_id = int(data['user_id'])
    lanying_link = str(data['lanying_link'])
    result = lanying_chatbot.create_chatbot_from_capsule(app_id, capsule_id, password, cycle_type, price, user_id, lanying_link)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/check_create_chatbot_from_publish_capsule", methods=["POST"])
def check_create_chatbot_from_publish_capsule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    capsule_id = str(data['capsule_id'])
    cycle_type = str(data.get('cycle_type', 'month'))
    price = int(data.get('price', '0'))
    result = lanying_chatbot.check_create_chatbot_from_publish_capsule(app_id, capsule_id, cycle_type, price)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/create_chatbot_from_publish_capsule", methods=["POST"])
def create_chatbot_from_publish_capsule():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    capsule_id = str(data['capsule_id'])
    cycle_type = str(data.get('cycle_type', 'month'))
    price = int(data.get('price', '0'))
    user_id = int(data['user_id'])
    lanying_link = str(data['lanying_link'])
    result = lanying_chatbot.create_chatbot_from_publish_capsule(app_id, capsule_id, cycle_type, price, user_id, lanying_link)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/delete_chatbot", methods=["POST"])
def delete_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    chatbot_id = str(data['chatbot_id'])
    result = lanying_chatbot.delete_chatbot(app_id, chatbot_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/list_publish_capsules", methods=["POST"])
def list_publish_capsules():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    page_num = int(data['page_num'])
    page_size = int(data['page_size'])
    result = lanying_ai_capsule.list_publish_capsules(page_num, page_size)
    capsules = result['list']
    total = result['total']
    resp = make_response({'code':200, 'data':{'list':capsules, 'total': total}})
    return resp

@bp.route("/service/openai/get_doc_metadata", methods=["POST"])
def get_doc_metadata():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    embedding_name = str(data['embedding_name'])
    doc_id = str(data['doc_id'])
    result = lanying_embedding.get_doc_metadata(app_id, embedding_name, doc_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openai/set_doc_metadata", methods=["POST"])
def set_doc_metadata():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    embedding_name = str(data['embedding_name'])
    doc_id = str(data['doc_id'])
    metadata = dict(data['metadata'])
    lanying_embedding.set_doc_metadata(app_id, embedding_name, doc_id, metadata)
    resp = make_response({'code':200, 'data':{'success':True}})
    return resp

@bp.route("/service/openai/get_app_capsule_quota_incomes", methods=["POST"])
def get_app_capsule_quota_incomes():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    year = int(data['year'])
    month = int(data['month'])
    incomes = lanying_ai_capsule.get_app_capsule_quota_incomes(app_id, year, month)
    dtoList = []
    for k,v in incomes.items():
        dtoList.append({
            'capsule_id': k,
            'quota': v
        })
    resp = make_response({'code':200, 'data':{'list':dtoList}})
    return resp

@bp.route("/service/openai/get_quota_income_app_ids", methods=["POST"])
def get_quota_income_app_ids():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    year = int(data['year'])
    month = int(data['month'])
    app_ids = lanying_ai_capsule.get_quota_income_app_ids(year, month)
    resp = make_response({'code':200, 'data':{'list':app_ids}})
    return resp

@bp.route("/service/openai/sync_messages", methods=["POST"])
def sync_messages():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    msg = json.loads(text)
    logging.info(f"receive sync messages start | msg:{msg}")
    app_id = str(msg['appId'])
    try:
        config = lanying_config.get_lanying_connector(app_id)
        newConfig = copy.deepcopy(config)
        newConfig['from_user_id'] = msg['from']['uid']
        newConfig['to_user_id'] = msg['to']['uid']
        newConfig['ext'] = msg.get('ext', '')
        newConfig['app_id'] = msg['appId']
        newConfig['msg_id'] = msg['msgId']
        messages = handle_sync_messages(newConfig, msg)
        logging.info(f"receive sync messages finish | msg:{msg}, messages:{messages}")
        resp = make_response({'code':200, 'data':{'messages': messages}})
        return resp
    except Exception as e:
        logging.exception(e)
        resp = make_response({'code':500, 'message':'server internal error'})
        return resp

def plugin_import_by_public_id(app_id, public_id):
    plugin_info = lanying_ai_plugin.get_public_plugin(public_id)
    if not plugin_info:
        return {'result':'error', 'message': 'plugin not exist'}
    plugin_config = json.loads(plugin_info['config'])
    return lanying_ai_plugin.plugin_import('file', app_id, plugin_config, 'public_id', public_id)

def plugin_import_by_url(type, app_id, url):
    config = lanying_config.get_lanying_connector(app_id)
    if lanying_chatbot.is_chatbot_mode(app_id):
        user_id = lanying_chatbot.get_default_user_id(app_id)
    else:
        user_id = config['lanying_user_id']
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': str(user_id)}
    filename = f"/tmp/plugin-import-{int(time.time())}-{random.randint(1,100000000)}"
    lanying_file_storage.download_url(url, headers, filename)
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        plugin_config = json.loads(content)
        return lanying_ai_plugin.plugin_import(type, app_id, plugin_config, 'url', '')
    
def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

def is_chatbot_user_id(app_id, user_id, config):
    if lanying_chatbot.is_chatbot_mode(app_id):
        if lanying_chatbot.get_user_chatbot_id(app_id, user_id):
            return True
    else:
        myUserId = config.get('lanying_user_id')
        logging.info(f'lanying_user_id:{myUserId}')
        return myUserId != None and user_id == str(myUserId)
    return False

def sort_functions(functions):
    return sorted(functions, key=lambda x: x['priority'])

def get_ai_message_round(from_user_id, to_user_id, content):
    redis = lanying_redis.get_redis_connection()
    cnt = get_ai_message_cnt(content)
    round_key = get_ai_message_round_key(from_user_id, to_user_id)
    if cnt > 0:
        round = redis.incrby(round_key, 1)
        redis.expire(round_key, 300)
        return round
    else:
        redis.delete(round_key)
        return 0

def add_ai_message_cnt(content):
    redis = lanying_redis.get_redis_connection()
    key = get_ai_message_key(content)
    redis.setex(key, 300, 1)

def get_ai_message_cnt(content):
    redis = lanying_redis.get_redis_connection()
    key = get_ai_message_key(content)
    ret_str = lanying_redis.redis_get(redis, key)
    if ret_str:
        return int(ret_str)
    else:
        return 0

def get_ai_message_key(content):
    hash_value = lanying_utils.sha256(content)
    return f"lanying_connector:ai_message_key:{hash_value}"

def get_ai_message_round_key(from_user_id, to_user_id):
    return f"lanying_connector:ai_message_round_key:{from_user_id}:{to_user_id}"

def sendMessageAsync(app_id, notify_from, user_id, content, ext = {}):
    add_ai_message_cnt(content)
    return lanying_connector.sendMessageAsync(app_id, notify_from, user_id, content, ext)

def replyMessageAsync(config, content, ext = {}):
    add_ai_message_cnt(content)
    if 'reply_msg_type' in config:
        app_id = config['app_id']
        reply_msg_type = config['reply_msg_type']
        reply_from = config['reply_from']
        reply_to = config['reply_to']
        request_msg_id = config['request_msg_id']
        if 'ai' in ext:
            ext['ai']['request_msg_id'] = request_msg_id
        if get_is_sync_mode(config):
            add_sync_mode_message(config, reply_msg_type, reply_from, reply_to, content, ext)
            return
        if reply_msg_type == 'CHAT':
            return lanying_connector.sendMessageAsync(app_id, reply_from, reply_to, content, ext)
        elif reply_msg_type == 'GROUPCHAT':
            return lanying_message.send_group_message_async(config, app_id, reply_from, reply_to, content, ext)

def replyAudioMessageAsync(config, content, audio_filename, ext = {}):
    add_ai_message_cnt(content)
    if 'reply_msg_type' in config:
        app_id = config['app_id']
        reply_msg_type = config['reply_msg_type']
        reply_from = config['reply_from']
        reply_to = config['reply_to']
        request_msg_id = config['request_msg_id']
        if 'ai' in ext:
            ext['ai']['request_msg_id'] = request_msg_id
        file_type = 104
        if reply_msg_type == 'CHAT':
            to_type = 1
        else:
            to_type = 2
        duration_ms = 0
        try:
            audio = AudioSegment.from_file(audio_filename)
            duration_ms = len(audio)
        except Exception as e:
            logging.exception(e)
        duration = round(duration_ms / 1000)
        file_size = os.path.getsize(audio_filename)
        attachment = {
            'dName': 'voice',
            "fLen": file_size,
            'duration': duration
        }
        upload_res = lanying_im_api.upload_chat_file(app_id, reply_from, 'mp3', 'audio/mpeg', file_type, to_type, reply_to, audio_filename)
        if upload_res['result'] == 'ok':
            download_url = upload_res['url']
            attachment['url'] = download_url
        else:
            return replyMessageAsync(config, content, ext)
        if get_is_sync_mode(config):
            add_sync_mode_audio_message(config, reply_msg_type, reply_from, reply_to, content, ext, {}, attachment)
            return
        content_type = 2
        extra = {
            'ext': ext,
            'attachment': attachment
        }
        if reply_msg_type == 'CHAT':
            return lanying_im_api.send_message_async(config, app_id, reply_from, reply_to, 1, content_type, content, extra)
        elif reply_msg_type == 'GROUPCHAT':
            return lanying_im_api.send_message_async(config, app_id, reply_from, reply_to, 2, content_type, content, extra)

def replyMessageImageAsync(config, url, ext = {}):
    add_ai_message_cnt(url)
    if 'reply_msg_type' in config:
        app_id = config['app_id']
        reply_msg_type = config['reply_msg_type']
        reply_from = config['reply_from']
        reply_to = config['reply_to']
        request_msg_id = config['request_msg_id']
        file_type = 102
        if reply_msg_type == 'CHAT':
            to_type = 1
        else:
            to_type = 2
        attachment = {
            'dName':'image.png',
            'url': url,
            "fLen":0,
            "width":0,
            "height":0
        }
        content = ''
        if 'ai' in ext:
            ext['ai']['request_msg_id'] = request_msg_id
        if get_is_sync_mode(config):
            upload_res = lanying_im_api.download_url_and_upload_to_im(app_id, reply_from, url, 'png', file_type, to_type, reply_to)
            if upload_res['result'] == 'ok':
                download_url = upload_res['url']
                attachment['url'] = download_url
                attachment['fLen'] = upload_res['file_size']
            add_sync_mode_image_message(config, reply_msg_type, reply_from, reply_to, content, attachment, ext)
            return
        extra = {
            'ext': ext,
            'attachment': attachment,
            'download_args': [app_id, reply_from, url, 'png', file_type, to_type, reply_to]
        }
        if reply_msg_type == 'CHAT':
            return lanying_im_api.send_message_async(config, app_id, reply_from, reply_to, 1, 1, content, extra)
        elif reply_msg_type == 'GROUPCHAT':
            return lanying_im_api.send_message_async(config, app_id, reply_from, reply_to, 2, 1, content, extra)

def replyMessageSync(config, content, ext = {}):
    add_ai_message_cnt(content)
    if 'reply_msg_type' in config:
        app_id = config['app_id']
        reply_msg_type = config['reply_msg_type']
        reply_from = config['reply_from']
        reply_to = config['reply_to']
        request_msg_id = config['request_msg_id']
        if 'ai' in ext:
            ext['ai']['request_msg_id'] = request_msg_id
        if get_is_sync_mode(config):
            add_sync_mode_message(config, reply_msg_type, reply_from, reply_to, content, ext)
            return int(time.time() * 1000000)
        if reply_msg_type == 'CHAT':
            return lanying_connector.sendMessage(app_id, reply_from, reply_to, content, ext)
        elif reply_msg_type == 'GROUPCHAT':
            return lanying_message.send_group_message_sync(config, app_id, reply_from, reply_to, content, ext)

def replyMessageOperAsync(config, stream_msg_id, oper_type, content, ext, msg_config, online_only):
    if 'reply_msg_type' in config:
        app_id = config['app_id']
        reply_msg_type = config['reply_msg_type']
        reply_from = config['reply_from']
        reply_to = config['reply_to']
        if get_is_sync_mode(config):
            if oper_type == 12:
                add_sync_mode_message(config, reply_msg_type, reply_from, reply_to, content, ext, msg_config)
            return
        if reply_msg_type == 'CHAT':
            return lanying_connector.sendMessageOperAsync(app_id, reply_from, reply_to, stream_msg_id, oper_type, content, ext, msg_config, online_only)
        else:
            return lanying_message.send_group_message_oper_async(config, app_id, reply_from, reply_to, stream_msg_id, oper_type, content, ext, msg_config, online_only)
    else:
        logging.info(f"Skip replyMessageOperAsync for config:{config}")

def is_debug_message(ext):
    try:
        return ext['ai']['is_debug_msg'] == True
    except Exception as e:
        return False

def set_sync_mode(config):
    config['is_sync_mode'] = True
    config['sync_mode_messages'] = []

def get_is_sync_mode(config):
    return config.get('is_sync_mode', False)

def get_sync_mode_messages(config):
    return config.get('sync_mode_messages',[])

def is_stream_msg_not_finish(ext):
    try:
        ai = ext.get('ai', {})
        is_stream = bool(ai.get('stream', False))
        is_finish = bool(ai.get('finish', False))
        if is_stream and not is_finish:
            return True
    except Exception as e:
        pass
    return False

def add_sync_mode_message(config, reply_msg_type, reply_from, reply_to, content, ext, msg_config={}):
    if is_debug_message(ext):
        return
    if is_stream_msg_not_finish(ext):
        return
    app_id = config['app_id']
    message_antispam = lanying_config.get_message_antispam(app_id)
    msg_config['antispam_prompt'] = message_antispam
    now = time.time()
    message = {
        'msg_id': int(now * 1000000),
        'timestamp': int(now * 1000),
        'type': reply_msg_type,
        'ctype': 'TEXT',
        'from_xid': {'uid': int(reply_from)},
        'to_xid': {'uid': int(reply_to)},
        'content': content,
        'ext': json.dumps(ext, ensure_ascii=False),
        'config': json.dumps(msg_config, ensure_ascii=False)
    }
    config['sync_mode_messages'].append(message)

def add_sync_mode_audio_message(config, reply_msg_type, reply_from, reply_to, content, ext, msg_config, attachment):
    if is_debug_message(ext):
        return
    if is_stream_msg_not_finish(ext):
        return
    app_id = config['app_id']
    message_antispam = lanying_config.get_message_antispam(app_id)
    msg_config['antispam_prompt'] = message_antispam
    now = time.time()
    message = {
        'msg_id': int(now * 1000000),
        'timestamp': int(now * 1000),
        'type': reply_msg_type,
        'ctype': 'AUDIO',
        'from_xid': {'uid': int(reply_from)},
        'to_xid': {'uid': int(reply_to)},
        'content': content,
        'ext': json.dumps(ext, ensure_ascii=False),
        'config': json.dumps(msg_config, ensure_ascii=False),
        'attachment': json.dumps(attachment, ensure_ascii=False)
    }
    config['sync_mode_messages'].append(message)

def add_sync_mode_image_message(config, reply_msg_type, reply_from, reply_to, content, attachment, ext, msg_config={}):
    if is_debug_message(ext):
        return
    if is_stream_msg_not_finish(ext):
        return
    app_id = config['app_id']
    message_antispam = lanying_config.get_message_antispam(app_id)
    msg_config['antispam_prompt'] = message_antispam
    now = time.time()
    message = {
        'msg_id': int(now * 1000000),
        'timestamp': int(now * 1000),
        'type': reply_msg_type,
        'ctype': 'IMAGE',
        'from_xid': {'uid': int(reply_from)},
        'to_xid': {'uid': int(reply_to)},
        'content': content,
        'attachment': json.dumps(attachment, ensure_ascii=False),
        'ext': json.dumps(ext, ensure_ascii=False),
        'config': json.dumps(msg_config, ensure_ascii=False)
    }
    config['sync_mode_messages'].append(message)

def get_system_functions(config, presetExt):
    functions = []
    # if config['image_generator'] == 'on':
    #     image_generator_function = {
    #         "name": "system_create_image",
    #         "description": "根据提示创建一幅图像。当给出一个纯文本提示以生成图像时，创建一个可用于 dalle 的提示。请遵循以下政策：\n1. 如果用户请求多幅图像，请只返回用户 1 幅图像。",
    #         "parameters": {
    #             "type": "object",
    #             "properties": {
    #                 "prompt": {
    #                     "type": "string",
    #                     "description": "图像的提示词，用于大模型 dalle 生成图像，此参数需使用英文，最大长度为 4000 个字符。"
    #                 }
    #             },
    #             "required": [
    #                 "prompt"
    #             ]
    #         },
    #         "priority": 0,
    #         "function_call":{
    #             "type": "system"
    #         }
    #     }
    #     functions.append(image_generator_function)
    return functions
