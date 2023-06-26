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
from lanying_tasks import add_embedding_file, delete_doc_data
import lanying_embedding
import re
import lanying_command
expireSeconds = 86400 * 3
presetNameExpireSeconds = 86400 * 3
using_embedding_expire_seconds = 86400 * 3
maxUserHistoryLen = 20
MaxTotalTokens = 4000

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
    model_res = check_model_allow(preset['model'])
    if model_res['result'] == 'error':
        logging.info(f"check_model_allow deny: app_id={app_id}, msg={model_res['msg']}")
        return model_res
    limit_res = check_message_limit(app_id, config)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    openai_key = get_openai_key(config, openai_key_type)
    response = forward_request(app_id, request, openai_key)
    if response.status_code == 200:
        response_content = json.loads(response.content)
        add_message_statistic(app_id, config, preset, response_content, openai_key_type)
    else:
        logging.info(f"forward request: bad response | status_code: {response.status_code}, response_content:{response.content}")
    return {'result':'ok', 'response':response}

def maybe_init_preset_model_for_embedding(preset, path):
    if path == '/v1/engines/text-embedding-ada-002/embeddings':
        preset['model'] = 'text-embedding-ada-002'

def forward_request(app_id, request, openai_key):
    url = "https://api.openai.com" + request.path
    data = request.get_data()
    headers = {"Content-Type":"application/json", "Authorization":"Bearer " + openai_key}
    logging.info(f"forward request start: app_id:{app_id}, url:{url}, data:{data}")
    response = requests.post(url, data=data, headers=headers)
    logging.info(f"forward request finish: app_id:{app_id}, status_code: {response.status_code}, response_content:{response.content}")
    return response

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
        pass
    return {'result':'error', 'msg':'bad_authorization'}

def handle_chat_message(msg, config, retry_times = 3):
    reply_message_read_ack(config)
    preset = copy.deepcopy(config['preset'])
    checkres = check_message_deduct_failed(msg['appId'], config)
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
        lcExt = ext['lanying_connector']
    except Exception as e:
        pass
    preset_name = ""
    if preset_name == "":
        try:
            if "preset_name" in command_ext:
                if command_ext['preset_name'] != "default":
                    preset = preset['presets'][command_ext['preset_name']]
                    init_preset_defaults(preset, config['preset'])
                preset_name = command_ext['preset_name']
                logging.info(f"using preset_name from command:{preset_name}")
        except Exception as e:
            logging.exception(e)
    if preset_name == "":
        try:
            if 'preset_name' in lcExt:
                if lcExt['preset_name'] != "default":
                    preset = preset['presets'][lcExt['preset_name']]
                    init_preset_defaults(preset, config['preset'])
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
                    init_preset_defaults(preset, config['preset'])
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
    logging.info(f"lanying-connector:ext={json.dumps(lcExt, ensure_ascii=False)}")
    isChatGPT = is_chatgpt_model(preset['model'])
    if isChatGPT:
        return handle_chat_message_chatgpt(msg, config, preset, lcExt, presetExt, preset_name, command_ext, retry_times)
    else:
        return ''

def handle_chat_message_chatgpt(msg, config, preset, lcExt, presetExt, preset_name, command_ext, retry_times):
    app_id = msg['appId']
    check_res = check_message_limit(app_id, config)
    if check_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={check_res['msg']}")
        return check_res['msg']
    openai_key_type = check_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    doc_id = ""
    content = msg['content']
    if 'new_content' in command_ext:
        content = command_ext['new_content']
        logging.info(f"using content in command:{content}")
    if doc_id == "" and 'doc_id' in command_ext:
        doc_id = command_ext['doc_id']
        logging.info(f"using doc_id in command:{doc_id}")
    openai_api_key = get_openai_key(config, openai_key_type)
    openai.api_key = openai_api_key
    add_reference = presetExt.get('add_reference', 'none')
    reference_list = []
    messages = preset.get('messages',[])
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
        return ''
    if content == '/reset_prompt':
        removeAllHistory(redis, historyListKey)
        del_preset_name(redis, fromUserId, toUserId)
        del_embedding_info(redis, fromUserId, toUserId)
        return 'prompt is reset'
    preset_embedding_infos = []
    if 'embedding_name' in presetExt:
        preset_embedding_infos.append(presetExt)
    other_preset_embedding_infos = lanying_embedding.get_preset_embedding_infos(app_id, preset_name)
    if len(other_preset_embedding_infos) > 0:
        if 'embedding_name' in presetExt:
            preset_embedding_infos.extend([item for item in other_preset_embedding_infos if item.get("embedding_name") != presetExt["embedding_name"]])
        else:
            preset_embedding_infos.extend(other_preset_embedding_infos)
    if len(preset_embedding_infos) > 0:
        context = ""
        context_with_distance = ""
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
            embedding_history_num = presetExt.get('embedding_history_num', 0)
            embedding_query_text = calc_embedding_query_text(content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId)
            q_embedding = fetch_embeddings(app_id, config, openai_key_type, embedding_query_text)
            search_result = multi_embedding_search(app_id, q_embedding, preset_embedding_infos, doc_id)
            embedding_min_distance = 1.0
            first_preset_embedding_info = preset_embedding_infos[0]
            embedding_max_distance = presetExt.get('embedding_max_distance', 1.0)
            embedding_content = first_preset_embedding_info.get('embedding_content', "请严格按照下面的知识回答我之后的所有问题:")
            embedding_content_type = presetExt.get('embedding_content_type', 'text')
            for doc in search_result:
                if hasattr(doc, 'doc_id') and doc.doc_id not in reference_list:
                    reference_list.append(doc.doc_id)
                now_distance = float(doc.vector_score)
                if embedding_min_distance > now_distance:
                    embedding_min_distance = now_distance
                if embedding_content_type == 'summary':
                    context = context + doc.summary + "\n\n"
                    context_with_distance = context_with_distance + f"[{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}]" + doc.summary + "\n\n"
                else:
                    context = context + doc.text + "\n\n"
                    context_with_distance = context_with_distance + f"[{now_distance}, doc_id:{doc.doc_id if hasattr(doc, 'doc_id') else '-'}]" + doc.text + "\n\n"
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
            context = f"{embedding_content}\n\n{context}"
            if is_debug:
                if is_use_old_embeddings:
                    lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] 使用之前存储的embeddings:\n[embedding_min_distance={embedding_min_distance}]\n{context}")
                else:
                    lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] prompt信息如下:\n[embedding_min_distance={embedding_min_distance}]\n{context_with_distance}")
            messages.append({'role':'user', 'content':context})
    history_result = loadHistoryChatGPT(config, app_id, redis, historyListKey, content, messages, now, preset)
    if history_result['result'] == 'error':
        return history_result['message']
    userHistoryList = history_result['data']
    for userHistory in userHistoryList:
        logging.info(f'userHistory:{userHistory}')
        messages.append(userHistory)
    messages.append({"role": "user", "content": content})
    preset['messages'] = messages
    calcMessagesTokens(messages, preset['model'])
    response = openai.ChatCompletion.create(**preset)
    logging.info(f"openai response:{response}")
    add_message_statistic(app_id, config, preset, response, openai_key_type)
    reply = response.choices[0].message.content.strip()
    command = None
    try:
        command = json.loads(reply)['lanying-connector']
        pass
    except Exception as e:
        pass
    if command:
        if is_debug:
            lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG]收到如下JSON:\n{reply}")
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
                lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, command['preset_welcome'])
            return handle_chat_message(msg, config, retry_times - 1)
        else:
            return ''
    reply_ext = {}
    if add_reference == 'body' or add_reference == "both":
        reply = reply + f"\nreference: {reference_list}"
    if add_reference == 'ext' or add_reference == "both":
        reply_ext = {'reference':reference_list}
    lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, reply, reply_ext)
    return ''

def multi_embedding_search(app_id, q_embedding, preset_embedding_infos, doc_id):
    list = []
    max_tokens = 0
    max_blocks = 0
    preset_idx = 0
    for preset_embedding_info in preset_embedding_infos:
        embedding_name = preset_embedding_info['embedding_name']
        if doc_id != "":
            embedding_uuid_from_doc_id = lanying_embedding.get_embedding_uuid_from_doc_id(doc_id)
            if not ('embedding_uuid' in preset_embedding_info and preset_embedding_info['embedding_uuid'] == embedding_uuid_from_doc_id):
                logging.info(f"skip embedding_name for doc_id: embedding_name:{embedding_name}, doc_id:{doc_id}")
                continue
            else:
                logging.info(f"choose embedding_name for doc_id: embedding_name:{embedding_name}, doc_id:{doc_id}")
        preset_idx = preset_idx + 1
        embedding_max_tokens = lanying_embedding.word_num_to_token_num(preset_embedding_info.get('embedding_max_tokens', 1024))
        embedding_max_blocks = preset_embedding_info.get('embedding_max_blocks', 2)
        if max_tokens < embedding_max_tokens:
            max_tokens = embedding_max_tokens
        if max_blocks < embedding_max_blocks:
            max_blocks = embedding_max_blocks
        docs = lanying_embedding.search_embeddings(app_id, embedding_name, doc_id, q_embedding, embedding_max_tokens, embedding_max_blocks)
        idx = 0
        for doc in docs:
            idx = idx+1
            list.append(((idx,float(doc.vector_score),preset_idx),doc))
    sorted_list = sorted(list)
    ret = []
    now_tokens = 0
    blocks_num = 0
    for _,doc in sorted_list:
        now_tokens += int(doc.num_of_tokens)
        blocks_num += 1
        logging.info(f"search_embeddings count token: now_tokens:{now_tokens}, num_of_tokens:{int(doc.num_of_tokens)},blocks_num:{blocks_num}")
        if now_tokens > max_tokens:
            break
        if blocks_num > max_blocks:
            break
        ret.append(doc)
    return ret

def loadHistoryChatGPT(config, app_id, redis, historyListKey, content, messages, now, preset):
    history_msg_count_min = ensure_even(config.get('history_msg_count_min', 1))
    history_msg_count_max = ensure_even(config.get('history_msg_count_max', 10))
    history_msg_size_max = config.get('history_msg_size_max', 4096)
    completionTokens = preset.get('max_tokens', 1024)
    uidHistoryList = []
    model = preset['model']
    token_limit = model_token_limit(model)
    messagesSize = calcMessagesTokens(messages, model)
    askMessage = {"role": "user", "content": content}
    nowSize = calcMessageTokens(askMessage, model) + messagesSize
    if nowSize + completionTokens >= token_limit:
        logging.info(f'stop history without history for max tokens: app_id={app_id}, now prompt size:{nowSize}, completionTokens:{completionTokens},token_limit:{token_limit}')
        return {'result':'error', 'message': lanying_config.get_message_too_long(app_id)}
    if redis:
        for historyStr in getHistoryList(redis, historyListKey):
            history = json.loads(historyStr)
            if history['time'] < now - expireSeconds:
                removeHistory(redis, historyListKey, historyStr)
            uidHistoryList.append(history)
    res = []
    history_bytes = 0
    history_count = 0
    for history in reversed(uidHistoryList):
        if 'list' in history:
            nowHistoryList = history['list']
        else:
            userMessage = {'role':'user', 'content': history['user']}
            assistantMessage = {'role':'assistant', 'content': history['assistant']}
            nowHistoryList = [userMessage, assistantMessage]
        history_count += len(nowHistoryList)
        historySize = 0
        for nowHistory in nowHistoryList:
            historySize += calcMessageTokens(nowHistory, model)
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
            logging.info(f'stop history for max tokens: app_id={app_id}, now prompt size:{nowSize}, completionTokens:{completionTokens}, token_limit:{token_limit}')
            break
    return {'result':'ok', 'data': reversed(res)}

def model_token_limit(model):
    if is_chatgpt_model_4_32k(model):
        return 32000
    if is_chatgpt_model_3_5_16k(model):
        return 16000
    if is_chatgpt_model_4(model):
        return 8000
    if is_embedding_model(model):
        return 8000
    return 4000

def historyListChatGPTKey(fromUserId, toUserId):
    return "lanying:connector:history:list:chatGPT:" + fromUserId + ":" + toUserId

def historyListGPT3Key(fromUserId, toUserId):
    return "lanying:connector:history:list:gpt3" + fromUserId + ":" + toUserId

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

def calcMessagesTokens(messages, model):
    try:
        encoding = tiktoken.encoding_for_model(model)
        num_tokens = 0
        for message in messages:
            num_tokens += 4
            for key, value in message.items():
                num_tokens += len(encoding.encode(value))
                if key == "name":
                    num_tokens += -1
        num_tokens += 2
        return num_tokens
    except Exception as e:
        logging.exception(e)
        return MaxTotalTokens

def calcMessageTokens(message, model):
    try:
        encoding = tiktoken.encoding_for_model(model)
        num_tokens = 0
        num_tokens += 4
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += -1
        return num_tokens
    except Exception as e:
        logging.exception(e)
        return MaxTotalTokens

def get_openai_key(config, openai_key_type):
    openai_api_key = ''
    if openai_key_type == 'share':
        DefaultApiKey = lanying_config.get_lanying_connector_default_openai_api_key()
        if DefaultApiKey:
            openai_api_key = DefaultApiKey
    else:
        openai_api_key = config['openai_api_key']
    return openai_api_key

def reply_message_read_ack(config):
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    msgId = config['msg_id']
    appId = config['app_id']
    lanying_connector.sendReadAck(appId, toUserId, fromUserId, msgId)

def is_chatgpt_model(model):
    return is_chatgpt_model_3_5(model) or is_chatgpt_model_4(model)

def is_chatgpt_model_3_5(model):
    return model.startswith("gpt-3.5")

def is_chatgpt_model_3_5_16k(model):
    return model.startswith("gpt-3.5-turbo-16k")

def is_chatgpt_model_4(model):
    return model.startswith("gpt-4")

def is_chatgpt_model_4_32k(model):
    return model.startswith("gpt-4-32k")

def is_embedding_model(model):
    return model.startswith("text-embedding-ada-002")

def add_message_statistic(app_id, config, preset, response, openai_key_type):
    if 'usage' in response:
        usage = response['usage']
        completion_tokens = usage.get('completion_tokens',0)
        prompt_tokens = usage.get('prompt_tokens', 0)
        total_tokens = usage.get('total_tokens', 0)
        text_size = calc_used_text_size(preset, response)
        model = preset['model']
        message_count_quota = calc_message_quota(model, text_size)
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
        new_quota = redis.hincrby(key, field_float, round(quota * 100))
        if new_quota >= 100:
            increment = new_quota // 100
            redis.hincrby(key, field_float, - increment * 100)
            return redis.hincrby(key, field, increment)
        else:
            return redis.hincrby(key, field, 0)

def check_message_limit(app_id, config):
    message_per_month = config.get('message_per_month', 0)
    enable_extra_price = False
    if config.get('enable_extra_price', 0) == 1:
        enable_extra_price = True
    product_id = config.get('product_id', 0)
    if product_id == 0:
        return {'result':'ok', 'openai_key_type':'self'}
    redis = lanying_redis.get_redis_connection()
    if redis:
        key = get_message_statistic_keys(config, app_id)[0]
        message_count_quota = redis.hincrby(key, 'message_count_quota', 0)
        if message_count_quota < message_per_month:
            return {'result':'ok', 'openai_key_type':'share'}
        else:
            if enable_extra_price:
                openai_api_key = config.get('openai_api_key', '')
                if len(openai_api_key) > 0:
                    return {'result':'ok', 'openai_key_type':'self'}
                else:
                    return {'result':'ok', 'openai_key_type':'share'}
            else:
                return {'result':'error', 'msg': lanying_config.get_message_no_quota(app_id)}
    else:
        return {'result':'error', 'msg':lanying_config.get_message_404(app_id)}

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
        return {'result':'error', 'msg':lanying_config.get_message_deduct_failed(app_id)}
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

def check_model_allow(model):
    if is_chatgpt_model(model):
        return {'result':'ok'}
    if is_embedding_model(model):
        return {'result':'ok'}
    return {'result':'error', 'msg':f'model {model} is not supported'}

def calc_message_quota(model, text_size):
    multi = 1
    if is_chatgpt_model_4(model):
        multi = 20
    if is_chatgpt_model_4_32k(model):
        multi = 40
    if is_chatgpt_model_3_5_16k(model):
        multi = 2
    if is_embedding_model(model):
        multi = 0.05
    count = round(text_size / 1024)
    if  count < 1:
        count = 1
    return count * multi

def calc_used_text_size(preset, response):
    text_size = 0
    model = preset['model']
    if is_chatgpt_model(model):
        for message in preset['messages']:
            text_size += text_byte_size(message.get('content', ''))
        text_size += text_byte_size(response['choices'][0]['message']['content'].strip())
    elif is_embedding_model(model):
        text_size += text_byte_size(preset['input'])
    else:
        text_size += text_byte_size(preset['prompt'])
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

def calcMessageTokens(message, model):
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = 0
    num_tokens += 4
    for key, value in message.items():
        num_tokens += len(encoding.encode(value))
        if key == "name":
            num_tokens += -1
    return num_tokens

def init_preset_defaults(preset, preset_default):
    for k,v in preset_default.items():
        if k not in ['presets', 'ext'] and k not in preset:
            preset[k] = v

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

def create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name):
    return lanying_embedding.create_embedding(app_id, embedding_name, max_block_size, algo, admin_user_ids, preset_name)

def configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name):
    return lanying_embedding.configure_embedding(app_id, embedding_name, admin_user_ids, preset_name, embedding_max_tokens, embedding_max_blocks, embedding_content, new_embedding_name)

def list_embeddings(app_id):
    return lanying_embedding.list_embeddings(app_id)

def get_embedding_doc_info_list(app_id, embedding_name, start, end):
    return lanying_embedding.get_embedding_doc_info_list(app_id, embedding_name, start, end)
     
def fetch_embeddings(app_id, config, openai_key_type, text):
    logging.info(f"fetch_embeddings: app_id={app_id}, text={text}")
    response = openai.Embedding.create(input=text, engine='text-embedding-ada-002')
    embedding = response['data'][0]['embedding']
    preset = {'model':'text-embedding-ada-002', 'input':text}
    add_message_statistic(app_id, config, preset, response, openai_key_type)
    return embedding

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
    file_id = save_attachment(from_user_id, attachment_str)
    return f'上传文件成功， 文件ID:{file_id}'

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
        add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token']])
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
    embedding_infos = lanying_embedding.get_preset_embedding_infos(app_id, preset_name)
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
    embedding_infos = lanying_embedding.get_preset_embedding_infos(app_id, preset_name)
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

def search_by_preset(msg, config, preset_name, new_content):
    return {'result':'continue', 'command_ext':{'preset_name':preset_name, "new_content":new_content}}

def add_doc_to_embedding(app_id, embedding_name, dname, url, type):
    config = lanying_config.get_lanying_connector(app_id)
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': config['lanying_user_id']}
    trace_id = lanying_embedding.create_trace_id()
    add_embedding_file.apply_async(args = [trace_id, app_id, embedding_name, url, headers, dname, config['access_token'], type])

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
        allow_exts  = [".html", ".htm", ".zip", ".csv", ".txt", ".md", ".pdf"]
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

def calc_embedding_query_text(content, historyListKey, embedding_history_num, is_debug, app_id, toUserId, fromUserId):
    if embedding_history_num <= 0:
        return content
    result = [content]
    now = int(time.time())
    history_count = 0
    history_size = lanying_embedding.num_of_tokens(content)
    model = 'text-embedding-ada-002'
    token_limit = model_token_limit(model)
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
        lanying_connector.sendMessage(app_id, toUserId, fromUserId, f"[LanyingConnector DEBUG] 使用问题历史算向量:\n{embedding_query_text}")
    return embedding_query_text
