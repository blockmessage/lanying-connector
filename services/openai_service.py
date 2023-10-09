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

service = 'openai_service'
bp = Blueprint(service, __name__)

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
    text = data.get('text')
    limit_res = check_message_limit(app_id, config, vendor)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    auth_info = get_preset_auth_info(config, openai_key_type, vendor)
    prepare_info = lanying_vendor.prepare_embedding(vendor,auth_info, 'db')
    response = lanying_vendor.embedding(vendor, prepare_info, text)
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
        lanying_embedding.delete_trace_field(trace_id, "notify_user")
        user_id = int(notify_user)
        lanying_user_id = config['lanying_user_id']
        if status == "success":
            lanying_connector.sendMessageAsync(app_id, lanying_user_id, user_id, f"文章（ID：{doc_id}）已加入知识库 {embedding_name}，有用的知识又增加了，谢谢您 ♪(･ω･)ﾉ",{'ai':{'role': 'ai'}})
        else:
            lanying_connector.sendMessageAsync(app_id, lanying_user_id, user_id, f"文章（ID：{doc_id}）加入知识库 {embedding_name}失败：{message}",{'ai':{'role': 'ai'}})

def handle_request(request):
    text = request.get_data(as_text=True)
    path = request.path
    logging.info(f"receive api request: {text}")
    auth_result = check_authorization(request)
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
    preset = json.loads(text)
    maybe_init_preset_model_for_embedding(preset, path)
    preset_name = "default"
    vendor = "openai"
    model = preset['model']
    model_config = lanying_vendor.get_chat_model_config(vendor, model)
    if model_config is None:
        model_config = lanying_vendor.get_embedding_model_config(vendor, model)
    model_res = check_model_allow(model_config, model)
    if model_res['result'] == 'error':
        logging.info(f"check_model_allow deny: app_id={app_id}, msg={model_res['msg']}")
        return model_res
    limit_res = check_message_limit(app_id, config, vendor)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    auth_info = get_preset_auth_info(config, openai_key_type, vendor)
    stream,response = forward_request(app_id, request, auth_info)
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
                                contents.append(content)
                            except Exception as e:
                                pass
                        yield line_str + '\n'
                finally:
                    reply = ''.join(contents)
                    response_json = stream_lines_to_response(preset, reply, vendor, {}, "", "")
                    add_message_statistic(app_id, config, preset, response_json, openai_key_type, model_config)
            return {'result':'ok', 'response':response, 'iter': generate_response}
        else:
            response_content = json.loads(response.content)
            add_message_statistic(app_id, config, preset, response_content, openai_key_type, model_config)
    else:
        logging.info(f"forward request: bad response | status_code: {response.status_code}, response_content:{response.content}")
    return {'result':'ok', 'response':response}

def maybe_init_preset_model_for_embedding(preset, path):
    if path == '/v1/engines/text-embedding-ada-002/embeddings':
        preset['model'] = 'text-embedding-ada-002'

def forward_request(app_id, request, auth_info):
    openai_key = auth_info.get('api_key','')
    proxy_domain = os.getenv('LANYING_CONNECTOR_OPENAI_PROXY_DOMAIN', '')
    if len(proxy_domain) > 0:
        proxy_api_key = os.getenv("LANYING_CONNECTOR_OPENAI_PROXY_API_KEY", '')
        url = proxy_domain + request.path
        headers = {"Content-Type":"application/json", "Authorization-Next":"Bearer " + openai_key,  "Authorization":"Basic " + proxy_api_key}
    else:
        url = "https://api.openai.com" + request.path
        headers = {"Content-Type":"application/json", "Authorization":"Bearer " + openai_key}
    data = request.get_data()
    request_json = json.loads(data)
    stream = request_json.get('stream', False)
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

def check_message_need_reply(config, msg):
    fromUserId = msg['from']['uid']
    toUserId = msg['to']['uid']
    type = msg['type']
    myUserId = config['lanying_user_id']
    logging.info(f'lanying_user_id:{myUserId}')
    if myUserId != None and toUserId == myUserId and fromUserId != myUserId and type == 'CHAT':
        try:
            ext = json.loads(msg['ext'])
            if ext.get('ai',{}).get('role', 'none') == 'ai':
                return {'result':'error', 'msg':''} # message is from ai
        except Exception as e:
            pass
        return {'result':'ok'}
    return {'result':'error', 'msg':''}

def handle_chat_message(config, msg):
    maybe_add_history(config, msg)
    try:
        reply = handle_chat_message_try(config, msg, 3)
    except Exception as e:
        logging.error("fail to handle_chat_message:")
        logging.exception(e)
        app_id = msg['appId']
        reply = lanying_config.get_message_404(app_id)
    if len(reply) > 0:
        lcExt = {}
        try:
            ext = json.loads(config['ext'])
            if 'ai' in ext:
                lcExt = ext['ai']
            elif 'lanying_connector' in ext:
                lcExt = ext['lanying_connector']
        except Exception as e:
            pass
        fromUserId = config['from_user_id']
        toUserId = config['to_user_id']
        reply_ext = {
            'ai': {
                'stream': False,
                'role': 'ai'
            }
        }
        if 'feedback' in lcExt:
            reply_ext['ai']['feedback'] = lcExt['feedback']
        lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, reply, reply_ext)

def handle_chat_message_try(config, msg, retry_times):
    checkres = check_message_need_reply(config, msg)
    if checkres['result'] == 'error':
        return checkres['msg']
    reply_message_read_ack(config)
    preset = copy.deepcopy(config['preset'])
    checkres = check_message_deduct_failed(msg['appId'], config)
    if checkres['result'] == 'error':
        return checkres['msg']
    checkres = check_product_id(msg['appId'], config)
    if checkres['result'] == 'error':
        return checkres['msg']
    ctype = msg['ctype']
    command_ext = {}
    if ctype == 'TEXT':
        content = msg['content']
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
    else:
        return ''
    checkres = check_message_per_month_per_user(msg, config)
    if checkres['result'] == 'error':
        return checkres['msg']
    lcExt = {}
    presetExt = {}
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
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
                    preset = preset['presets'][command_ext['preset_name']]
                preset_name = command_ext['preset_name']
                logging.info(f"using preset_name from command:{preset_name}")
        except Exception as e:
            logging.exception(e)
    if preset_name == "":
        try:
            if 'preset_name' in lcExt:
                if lcExt['preset_name'] != "default":
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
                    preset = preset['presets'][lastChoosePresetName]
                preset_name = lastChoosePresetName
                logging.info(f"using preset_name from last_choose_preset:{preset_name}")
            except Exception as e:
                logging.exception(e)
    if preset_name == "":
        preset_name = "default"
    if 'presets' in preset:
        del preset['presets']
    if 'ext' in preset:
        presetExt = copy.deepcopy(preset['ext'])
        del preset['ext']
    is_debug = 'debug' in presetExt and presetExt['debug'] == True
    if is_debug:
        lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] 当前预设为: {preset_name}",{'ai':{'role': 'ai'}})
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
    model = preset['model']
    check_res = check_message_limit(app_id, config, vendor)
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
    functions = []
    now = int(time.time())
    history = {'time':now}
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    historyListKey = historyListChatGPTKey(fromUserId, toUserId)
    redis = lanying_redis.get_redis_connection()
    is_debug = 'debug' in presetExt and presetExt['debug'] == True
    if 'reset_prompt' in lcExt and lcExt['reset_prompt'] == True:
        removeAllHistory(redis, historyListKey)
        del_preset_name(redis, fromUserId, toUserId)
        del_embedding_info(redis, fromUserId, toUserId)
    if 'prompt_ext' in lcExt and lcExt['prompt_ext']:
        customHistoryList = []
        for customHistory in lcExt['prompt_ext']:
            if customHistory['role'] and customHistory['content']:
                customHistoryList.append({'role':customHistory['role'], 'content': customHistory['content']})
        addHistory(redis, historyListKey, {'list':customHistoryList, 'time':now})
    if 'ai_generate' in lcExt and lcExt['ai_generate'] == False:
        history['user'] = content
        history['assistant'] = ''
        history['uid'] = fromUserId
        history['type'] = 'ask'
        addHistory(redis, historyListKey, history)
        return ''
    if content == '/reset_prompt' or content == "/reset":
        removeAllHistory(redis, historyListKey)
        del_preset_name(redis, fromUserId, toUserId)
        del_embedding_info(redis, fromUserId, toUserId)
        return 'prompt is reset'
    preset_embedding_infos = lanying_embedding.get_preset_embedding_infos(config.get('embeddings'), app_id, preset_name)
    for now_embedding_info in lanying_ai_plugin.get_preset_function_embeddings(app_id, preset_name):
        preset_embedding_infos.append(now_embedding_info)
    if len(preset_embedding_infos) > 0:
        context = ""
        context_with_distance = ""
        question_merge = presetExt.get('question_merge', True)
        question_answers = []
        question_answer_with_distance = ""
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
            embedding_query_text = calc_embedding_query_text(content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId, model_config)
            ask_message = {"role": "user", "content": content}
            embedding_message =  {"role": embedding_role, "content": embedding_content}
            embedding_token_limit = model_token_limit(model_config) - calcMessagesTokens(messages, model, vendor) - preset.get('max_tokens', 1024) - calcMessageTokens(ask_message, model, vendor) - calcMessageTokens(embedding_message, model, vendor)
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
                if hasattr(doc, 'question') and doc.question != "":
                    if question_merge:
                        qa_text = "\n问: " + doc.question + "\n答: " + doc.text + "\n\n"
                        context = context + qa_text
                        if is_debug:
                            context_with_distance = context_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + qa_text + "\n\n"
                    else:
                        question_info = {'role':'user', 'content':doc.question}
                        answer_info = {'role':'assistant', 'content':doc.text}
                        question_answers.append(question_info)
                        question_answers.append(answer_info)
                        if is_debug:
                            question_answer_with_distance = question_answer_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + "\n" + json.dumps(question_info, ensure_ascii=False) + "\n" + json.dumps(answer_info, ensure_ascii=False) + "\n\n"
                elif hasattr(doc, 'function') and doc.function != "":
                    function_info = json.loads(doc.function)
                    if is_debug:
                        functions_with_distance += f"[distance:{now_distance}, function_name:{function_info.get('name','')}]\n\n"
                    function_name = function_info.get('name', '')
                    function_info["name"]  = f"class{len(functions)}_{function_name}"
                    function_info["doc_id"] = doc.doc_id
                    functions.append(function_info)
                elif embedding_content_type == 'summary':
                    context = context + doc.summary + "\n\n"
                    if is_debug:
                        context_with_distance = context_with_distance + f"[distance:{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}, segment_id:{segment_id}]" + doc.summary + "\n\n"
                else:
                    context = context + doc.text + "\n\n"
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
            if len(question_answers) > 0:
                messages.extend(question_answers)
            if is_debug:
                if is_use_old_embeddings:
                    lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] 使用之前存储的embeddings:\n[embedding_min_distance={embedding_min_distance}]\n{context}",{'ai':{'role': 'ai'}})
                else:
                    lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] prompt信息如下:\n[embedding_min_distance={embedding_min_distance}]\n{context_with_distance}\n{question_answer_with_distance}\n{functions_with_distance}\n",{'ai':{'role': 'ai'}})
    history_result = loadHistory(config, app_id, redis, historyListKey, content, messages, now, preset, presetExt, model_config, vendor)
    if history_result['result'] == 'error':
        return history_result['message']
    userHistoryList = history_result['data']
    for userHistory in userHistoryList:
        logging.info(f'userHistory:{userHistory}')
        messages.append(userHistory)
    messages.append({"role": "user", "content": content})
    preset['messages'] = messages
    if len(functions) > 0:
        preset['functions'] = functions
    else:
        if 'function' in preset:
            del preset['function']
    preset_message_lines = "\n".join([f"{message.get('role','')}:{message.get('content','')}" for message in messages])
    logging.info(f"==========final preset messages/functions============\n{preset_message_lines}\n{functions}")
    if 'force_stream' in lcExt and lcExt['force_stream'] == True:
        logging.info("force use stream")
        preset['stream'] = True
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
    while True:
        is_stream = ('reply_generator' in response)
        if is_stream:
            reply_generator = response.get('reply_generator')
            reply = response['reply']
            stream_interval = max(1, presetExt.get('stream_interval', 3))
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
                        lanying_connector.sendMessageOperAsync(app_id, toUserId, fromUserId, stream_msg_id, 11, message_to_send, reply_ext, oper_msg_config, True)
                    else:
                        try:
                            reply_ext['ai']['seq'] += 1
                            stream_msg_id = lanying_connector.sendMessage(app_id, toUserId, fromUserId, message_to_send, reply_ext)
                        except Exception as e:
                            pass
                    reply += message_to_send
                    content_count = 0
                    content_collect = []
                    collect_start_time = collect_now
                    stream_msg_last_send_time = collect_now
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
                function_call_debug['name'] = function_name_debug[(function_name_debug.find('_')+1):]
            lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] 触发函数：{function_call_debug}",{'ai':{'role': 'ai'}})
            response = handle_function_call(app_id, config, function_call, preset, openai_key_type, model_config, vendor, prepare_info)
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
            lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG]收到如下JSON:\n{reply}",{'ai':{'role': 'ai'}})
        if 'preset_welcome' in command:
            reply = command['preset_welcome']
    if command and 'ai_generate' in command and command['ai_generate'] == True:
        pass
    else:
        history['user'] = content
        history['assistant'] = reply
        history['uid'] = fromUserId
        addHistory(redis, historyListKey, history)
    if command:
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
                lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, command['preset_welcome'],{'ai':{'role': 'ai'}})
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
                    seq += 1
                    if len(doc_reference) > 0:
                        link = doc_reference
                    else:
                        link = doc_info.get('lanying_link', '')
                        if link == '':
                            link = doc_info.get('filename', '')
                    if link not in links:
                        links.append(link)
                        doc_desc_list.append({'seq':seq, 'doc_id':doc_id, 'link':link})
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
                    seq += 1
                    if len(doc_reference) > 0:
                        link = doc_reference
                    else:
                        link = doc_info.get('lanying_link', '')
                        if link == '':
                            link = doc_info.get('filename', '')
                    if link not in links:
                        links.append(link)
                        doc_desc = doc_format.replace('{seq}', f"{seq}").replace('{doc_id}', doc_id).replace('{link}', link)
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
            lanying_connector.sendMessageOperAsync(app_id, toUserId, fromUserId, stream_msg_id, 12, reply, reply_ext, oper_msg_config, False)
        else:
            if is_stream:
                reply_ext['ai']['seq'] += 1
                reply_ext['ai']['finish'] = True
                lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, reply, reply_ext)
            else:
                reply_ext['ai']['stream'] = False
                lanying_connector.sendMessageAsync(config['app_id'], toUserId, fromUserId, reply, reply_ext)
    return ''

def handle_function_call(app_id, config, function_call, preset, openai_key_type, model_config, vendor, prepare_info):
    function_name = function_call.get('name')
    function_args = json.loads(function_call.get('arguments', '{}'))
    functions = preset.get('functions', [])
    function_config = {}
    for function in functions:
        if function['name'] == function_name:
            function_config = function
    doc_id = function_config.get('doc_id', '')
    function_config = lanying_ai_plugin.fill_function_info(app_id, function_config, doc_id)
    if 'function_call' in function_config:
        lanying_function_call = function_config['function_call']
        method = lanying_function_call.get('method', 'get')
        url = fill_function_args(function_args, lanying_function_call.get('url', ''))
        params = fill_function_args(function_args, lanying_function_call.get('params', {}))
        headers = fill_function_args(function_args, lanying_function_call.get('headers', {}))
        body = fill_function_args(function_args, lanying_function_call.get('body', {}))
        if lanying_utils.is_valid_public_url(url):
            logging.info(f"start request function callback | app_id:{app_id}, function_name:{function_name}, url:{url}, params:{params}, headers: {headers}, body: {body}")
            if method == 'get':
                function_response = requests.get(url, params=params, headers=headers, timeout = (20.0, 40.0))
            else:
                function_response = requests.post(url, params=params, headers=headers, json = body, timeout = (20.0, 40.0))
            function_content = function_response.text
            logging.info(f"finish request function callback | app_id:{app_id}, function_name:{function_name}, function_content: {function_content}")
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
            append_message(preset, model_config, response_message)
            append_message(preset, model_config, function_message)
            response = lanying_vendor.chat(vendor, prepare_info, preset)
            logging.info(f"vendor function response | vendor:{vendor}, response:{response}")
            return response
    raise Exception('bad_preset_function')

def fill_function_args(function_args, obj):
    if isinstance(obj, str):
        for k,v in function_args.items():
            obj = obj.replace("{" + k + "}", v)
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
        if vendor in embedding_cache:
            q_embedding = embedding_cache[vendor]
        else:
            q_embedding = fetch_embeddings(app_id, config, api_key_type, embedding_query_text, vendor)
            embedding_cache[vendor] = q_embedding
        preset_idx = preset_idx + 1
        embedding_max_tokens = lanying_embedding.word_num_to_token_num(preset_embedding_info.get('embedding_max_tokens', 1024))
        embedding_max_blocks = preset_embedding_info.get('embedding_max_blocks', 2)
        if is_fulldoc:
            embedding_max_tokens = embedding_token_limit
            embedding_max_blocks = max(100, embedding_max_blocks)
        if embedding_max_tokens > embedding_token_limit:
            embedding_max_tokens = embedding_token_limit
        if max_tokens < embedding_max_tokens:
            max_tokens = embedding_max_tokens
        if max_blocks < embedding_max_blocks:
            max_blocks = embedding_max_blocks
        doc_ids = preset_embedding_info.get('doc_ids', [])
        docs = lanying_embedding.search_embeddings(app_id, embedding_name, doc_id, q_embedding, embedding_max_tokens, embedding_max_blocks, is_fulldoc, doc_ids)
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
            if is_fulldoc:
                seq_id = lanying_embedding.parse_segment_id_int_value(doc)
                list.append(((seq_id,idx, preset_idx),doc))
            else:
                list.append(((idx,vector_store,preset_idx),doc))
    sorted_list = sorted(list)
    ret = []
    now_tokens = 0
    blocks_num = 0
    for _,doc in sorted_list:
        now_tokens += int(doc.num_of_tokens) + 8
        blocks_num += 1
        logging.info(f"search_embeddings count token: now_tokens:{now_tokens}, num_of_tokens:{int(doc.num_of_tokens)},blocks_num:{blocks_num}")
        if now_tokens > max_tokens:
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
            userMessage = {'role':'user', 'content': history['user']}
            assistantMessage = {'role':'assistant', 'content': history['assistant']}
            nowHistoryList = [userMessage, assistantMessage]
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

def merge_history_content(a, b):
    if a == '':
        return b
    elif b == '':
        return a
    else:
        return a + '\n' + b

def model_token_limit(model_config):
    return model_config['token_limit']

def historyListChatGPTKey(fromUserId, toUserId):
    return "lanying:connector:history:list:chatGPT:" + fromUserId + ":" + toUserId

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

def reply_message_read_ack(config):
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    msgId = config['msg_id']
    appId = config['app_id']
    lanying_connector.sendReadAckAsync(appId, toUserId, fromUserId, msgId)

def add_message_statistic(app_id, config, preset, response, openai_key_type, model_config):
    if 'usage' in response:
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
        else:
            logging.error(f"fail to statistic message: app_id={app_id}, model={model}, completion_tokens={completion_tokens}, prompt_tokens={prompt_tokens}, total_tokens={total_tokens},text_size={text_size},message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")

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

def check_message_limit(app_id, config, vendor):
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
        if message_count_quota < message_per_month:
            return {'result':'ok', 'openai_key_type':'share'}
        else:
            if enable_extra_price:
                self_auth_info = get_preset_self_auth_info(config, vendor)
                if self_auth_info:
                    return {'result':'ok', 'openai_key_type':'self'}
                else:
                    return {'result':'ok', 'openai_key_type':'share'}
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
                return {'result':'error', 'msg': lanying_config.get_message_reach_user_message_limit(app_id)}
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
            reply = response['choices'][0]['message']['content'].strip()
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

def create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor):
    return lanying_embedding.create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name, overlapping_size, vendor)

def configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor):
    return lanying_embedding.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name, max_block_size, overlapping_size, vendor)

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

def fetch_embeddings(app_id, config, openai_key_type, text, vendor):
    embedding_api_key_type = openai_key_type
    logging.info(f"fetch_embeddings: app_id={app_id}, vendor:{vendor}, text={text}")
    auth_info = get_preset_auth_info(config, embedding_api_key_type, vendor)
    if auth_info is None:
        embedding_api_key_type = "share"
        auth_info = get_preset_auth_info(config, embedding_api_key_type, vendor)
    prepare_info = lanying_vendor.prepare_embedding(vendor, auth_info, 'query')
    response = lanying_vendor.embedding(vendor, prepare_info, text)
    embedding = response['embedding']
    model = response['model']
    preset = {'model':model, 'input':text}
    model_config = lanying_vendor.get_embedding_model_config(vendor, model)
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
                'user_id': config['lanying_user_id']}
        trace_id = lanying_embedding.create_trace_id()
        lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
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
    app_id = msg['appId']
    if lanying_embedding.is_app_embedding_admin_user(app_id, from_user_id):
        return lanying_command.pretty_help(app_id)
    else:
        return f"无法执行此命令，用户（ID：{from_user_id}）不是企业知识库管理员。"

def bluevector_error(msg, config):
    return '错误：命令格式不正确。\n可以使用 /help 或者 /+空格 查看命令说明。'

def help(msg, config):
    app_id = msg['appId']
    return lanying_command.pretty_help(app_id)

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
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': config['lanying_user_id']}
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

def add_embedding_to_file():
    pass

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

def calc_embedding_query_text(content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId, model_config):
    if embedding_history_num <= 0:
        return content
    result = [content]
    now = int(time.time())
    history_count = 0
    history_size = lanying_embedding.num_of_tokens(content)
    model = 'text-embedding-ada-002'
    token_limit = model_token_limit(model_config)
    redis = lanying_redis.get_redis_connection()
    for historyStr in reversed(getHistoryList(redis, historyListKey)):
        history = json.loads(historyStr)
        if history['time'] < now - expireSeconds:
            removeHistory(redis, historyListKey, historyStr)
        else:
            if history_count < embedding_history_num:
                if 'list' in history:
                    pass
                else:
                    history_content = history['user']
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
    embedding_query_text = '\n'.join(reversed(result))
    if is_debug:
        lanying_connector.sendMessageAsync(app_id, toUserId, fromUserId, f"[LanyingConnector DEBUG] 使用问题历史算向量:\n{embedding_query_text}",{'ai':{'role': 'ai'}})
    return embedding_query_text

def handle_chat_file(msg, config):
    from_user_id = int(msg['from']['uid'])
    app_id = msg['appId']
    attachment_str = msg['attachment']
    attachment = json.loads(attachment_str)
    dname = attachment['dName']
    _,ext = os.path.splitext(dname)
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
                'user_id': config['lanying_user_id']}
        trace_id = lanying_embedding.create_trace_id()
        lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
        add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], 'file', -1])
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
                    'user_id': config['lanying_user_id']}
            trace_id = lanying_embedding.create_trace_id()
            lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
            add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], 'file', -1])
            return f'添加到知识库({embedding_name})成功，请等待系统处理。'
        else:
            file_id = save_attachment(from_user_id, attachment_str)
            return f'上传文件成功， 文件ID:{file_id} 。\n您绑定了多个知识库{can_manage_embedding_names}, 可以设置默认知识库来自动添加文档到知识库,\n命令格式为：/bluevector mode auto <KNOWLEDGE_BASE_NAME>'

def handle_chat_links(msg, config):
    from_user_id = int(msg['from']['uid'])
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
            add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, 'url.html', config['access_token'], 'url', -1])
        return f'添加到知识库({embedding_name})成功，请等待系统处理。'
    else:
        default_embedding_name = get_user_default_embedding_name(app_id, from_user_id)
        if default_embedding_name and default_embedding_name in can_manage_embedding_names:
            for url in urls:
                headers = {}
                trace_id = lanying_embedding.create_trace_id()
                lanying_embedding.update_trace_field(trace_id, "notify_user", from_user_id)
                add_embedding_file.apply_async(args = [trace_id, app_id, default_embedding_name, url, headers, 'url.html', config['access_token'], 'url', -1])
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
        prompt_tokens = calcMessagesTokens(preset.get('messages',[]), preset['model'], vendor)
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
        redis = lanying_redis.get_redis_connection()
        now = int(time.time())
        history = {'time':now}
        ai_user_id = msg['from']['uid']
        human_user_id = msg['to']['uid']
        content = msg.get('content', '')
        historyListKey = historyListChatGPTKey(human_user_id, ai_user_id)
        history['user'] = ''
        history['assistant'] = content
        history['uid'] = human_user_id
        history['type'] = 'reply'
        addHistory(redis, historyListKey, history)
        pass

def need_add_history(config, msg):
    try:
        fromUserId = msg['from']['uid']
        toUserId = msg['to']['uid']
        type = msg['type']
        myUserId = config['lanying_user_id']
        logging.info(f'lanying_user_id:{myUserId}')
        if myUserId != None and toUserId != myUserId and fromUserId == myUserId and type == 'CHAT':
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
    result = lanying_ai_plugin.add_ai_function_to_ai_plugin(app_id, plugin_id, name, description, parameters, function_call)
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
    endpoint = str(data.get('endpoint', ''))
    result = lanying_ai_plugin.configure_ai_plugin(app_id, plugin_id, name, headers, endpoint)
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
    name = str(data['name'])
    description = str(data['description'])
    parameters = dict(data['parameters'])
    function_call = dict(data['function_call'])
    result = lanying_ai_plugin.configure_ai_function(app_id, plugin_id, function_id, name, description, parameters,function_call)
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
    result = lanying_ai_plugin.configure_ai_plugin_embedding(app_id, embedding_max_tokens, embedding_max_blocks, vendor)
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
    public_id = str(data.get('public_id', ''))
    url = str(data.get('url', ''))
    if len(public_id) > 0:
        result = plugin_import_by_public_id(app_id, public_id)
    elif len(url) > 0:
        result = plugin_import_by_url(app_id, url)
    else:
        result = {'result': 'error', 'message':'need args: url or public_id'}
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
    result = lanying_ai_plugin.list_public_plugins()
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

def plugin_import_by_public_id(app_id, public_id):
    plugin_info = lanying_ai_plugin.get_public_plugin(public_id)
    if not plugin_info:
        return {'result':'error', 'message': 'plugin not exist'}
    plugin_config = json.loads(plugin_info['config'])
    return lanying_ai_plugin.plugin_import(app_id, plugin_config)

def plugin_import_by_url(app_id, url):
    config = lanying_config.get_lanying_connector(app_id)
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': config['lanying_user_id']}
    filename = f"/tmp/plugin-import-{int(time.time())}-{random.randint(1,100000000)}"
    lanying_file_storage.download_url(url, headers, filename)
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        plugin_config = json.loads(content)
        return lanying_ai_plugin.plugin_import(app_id, plugin_config)
    
def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False
