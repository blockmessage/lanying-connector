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
import openai_doc_gen
import copy
expireSeconds = 86400 * 3
presetNameExpireSeconds = 86400 * 3
using_embedding_expire_seconds = 86400 * 3
maxUserHistoryLen = 20
MaxTotalTokens = 4000

def handle_request(request):
    text = request.get_data(as_text=True)
    logging.debug(f"receive api request: {text}")
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
    model_res = check_model_allow(preset['model'])
    if model_res['result'] == 'error':
        logging.info(f"check_model_allow deny: app_id={app_id}, msg={model_res['msg']}")
        return model_res
    limit_res = check_message_limit(app_id, config)
    if limit_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={limit_res['msg']}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    logging.debug(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    openai_key = get_openai_key(config, openai_key_type)
    response = forward_request(app_id, request, openai_key)
    if response.status_code == 200:
        response_content = json.loads(response.content)
        add_message_statistic(app_id, config, preset, response_content, openai_key_type)
    else:
        logging.info("forward request: bad response | status_code: {response.status_code}, response_content:{response.content}")
    return {'result':'ok', 'response':response}

def forward_request(app_id, request, openai_key):
    url = "https://api.openai.com" + request.path
    data = request.get_data()
    headers = {"Content-Type":"application/json", "Authorization":"Bearer " + openai_key}
    logging.debug(f"forward request start: app_id:{app_id}, url:{url}, data:{data}")
    response = requests.post(url, data=data, headers=headers)
    logging.debug(f"forward request finish: app_id:{app_id}, status_code: {response.status_code}, response_content:{response.content}")
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
    if ctype == 'TEXT':
        pass
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
        if lcExt['preset_name']:
            preset = preset['presets'][lcExt['preset_name']]
            init_preset_defaults(preset, config['preset'])
    except Exception as e:
        lcExt = {}
    lastChoosePresetName = get_preset_name(redis, fromUserId, toUserId)
    logging.debug(f"lastChoosePresetName:{lastChoosePresetName}")
    if lastChoosePresetName:
        try:
            preset = preset['presets'][lastChoosePresetName]
            logging.debug(f"using preset_name:{lastChoosePresetName}")
            init_preset_defaults(preset, config['preset'])
        except Exception as e:
            logging.exception(e)
            pass
    if 'presets' in preset:
        del preset['presets']
    if 'ext' in preset:
        presetExt = copy.deepcopy(preset['ext'])
        del preset['ext']
    logging.debug(f"lanying-connector:ext={json.dumps(lcExt, ensure_ascii=False)}")
    isChatGPT = is_chatgpt_model(preset['model'])
    if isChatGPT:
        return handle_chat_message_chatgpt(msg, config, preset, lcExt, presetExt, retry_times)
    else:
        return ''

def handle_chat_message_chatgpt(msg, config, preset, lcExt, presetExt, retry_times):
    app_id = msg['appId']
    check_res = check_message_limit(app_id, config)
    if check_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={check_res['msg']}")
        return check_res['msg']
    openai_key_type = check_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    content = msg['content']
    openai_api_key = get_openai_key(config, openai_key_type)
    openai.api_key = openai_api_key
    messages = preset.get('messages',[])
    now = int(time.time())
    history = {'time':now}
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    historyListKey = historyListChatGPTKey(fromUserId, toUserId)
    redis = lanying_redis.get_redis_connection()
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
    if 'embedding_name' in presetExt:
        embedding_name = presetExt['embedding_name']
        embedding_content = presetExt.get('embedding_content', "请严格按照下面的知识回答我之后的所有问题:")
        embedding_max_tokens = presetExt.get('embedding_max_tokens', 1024)
        embedding_max_blocks = presetExt.get('embedding_max_blocks', 2)
        embedding_info = get_embedding_info(redis, fromUserId, toUserId)
        embedding_max_distance = presetExt.get('embedding_max_distance', 0.2)
        logging.debug(f"use embeddings: {embedding_name}, embedding_max_tokens:{embedding_max_tokens},embedding_max_blocks:{embedding_max_blocks}")
        context = ""
        context_with_distance = ""
        is_use_old_embeddings = False
        using_embedding = embedding_info.get('using_embedding', 'auto')
        last_embedding_name = embedding_info.get('last_embedding_name', '')
        last_embedding_text = embedding_info.get('last_embedding_text', '')
        logging.debug(f"using_embedding state: using_embedding={using_embedding}, last_embedding_name={last_embedding_name}, len(last_embedding_text)={len(last_embedding_text)}")
        embedding_min_distance = 1.0
        if using_embedding == 'once' and last_embedding_text != '' and last_embedding_name == embedding_name:
            context = last_embedding_text
            is_use_old_embeddings = True
        if context == '': 
            q_embedding = fetch_embeddings(content)
            for doc in openai_doc_gen.search_embeddings(embedding_name, q_embedding, embedding_max_tokens, embedding_max_blocks):
                embedding_content_type = presetExt.get('embedding_content_type', 'text')
                now_distance = float(doc.vector_score)
                if embedding_min_distance > now_distance:
                    embedding_min_distance = now_distance
                if embedding_content_type == 'summary':
                    context = context + doc.summary + "\n\n"
                    context_with_distance = context_with_distance + f"[{now_distance}]" + doc.summary + "\n\n"
                else:
                    context = context + doc.text + "\n\n"
                    context_with_distance = context_with_distance + f"[{now_distance}]" + doc.text + "\n\n"
            if using_embedding == 'auto':
                if last_embedding_name != embedding_name or last_embedding_text == '' or embedding_min_distance <= embedding_max_distance:
                    embedding_info['last_embedding_name'] = embedding_name
                    embedding_info['last_embedding_text'] = context
                    set_embedding_info(redis, fromUserId, toUserId, embedding_info)
                else:
                    context = last_embedding_text
                    is_use_old_embeddings = True
            elif using_embedding == 'once':
                embedding_info['last_embedding_name'] = embedding_name
                embedding_info['last_embedding_text'] = context
                set_embedding_info(redis, fromUserId, toUserId, embedding_info)
        context = f"{embedding_content}\n\n{context}"
        if 'debug' in presetExt and presetExt['debug'] == True:
            if is_use_old_embeddings:
                lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] 使用之前存储的embeddings:\n[embedding_min_distance={embedding_min_distance}]\n{context}")
            else:
                lanying_connector.sendMessage(config['app_id'], toUserId, fromUserId, f"[LanyingConnector DEBUG] prompt信息如下:\n[embedding_min_distance={embedding_min_distance}]\n{context_with_distance}")
        messages.append({'role':'user', 'content':context})
    userHistoryList = loadHistoryChatGPT(config, app_id, redis, historyListKey, content, messages, now, preset)
    for userHistory in userHistoryList:
        logging.debug(f'userHistory:{userHistory}')
        messages.append(userHistory)
    messages.append({"role": "user", "content": content})
    preset['messages'] = messages
    calcMessagesTokens(messages, preset['model'])
    response = openai.ChatCompletion.create(**preset)
    logging.debug(f"openai response:{response}")
    add_message_statistic(app_id, config, preset, response, openai_key_type)
    reply = response.choices[0].message.content.strip()
    command = None
    try:
        command = json.loads(reply)['lanying-connector']
        pass
    except Exception as e:
        pass
    if command:
        if 'debug' in presetExt and presetExt['debug'] == True:
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
    return reply

def loadHistory(redis, historyListKey, content, prompt, now, preset):
    maxPromptSize = 3024 - preset.get('max_tokens', 1024)
    uidHistoryList = []
    nowSize = len(content) + len(prompt)
    if redis:
        for historyStr in getHistoryList(redis, historyListKey):
            history = json.loads(historyStr)
            if history['time'] < now - expireSeconds:
                removeHistory(redis, historyListKey, historyStr)
            uidHistoryList.append(history)
    res = ""
    for history in reversed(uidHistoryList):
        if nowSize + len(history['text']) < maxPromptSize:
            res = history['text'] + res
            nowSize += len(history['text'])
            logging.debug(f'resLen:{len(res)}, nowSize:{nowSize}')
        else:
            break
    return res

def loadHistoryChatGPT(config, app_id, redis, historyListKey, content, messages, now, preset):
    history_msg_count_min = ensure_even(config.get('history_msg_count_min', 1))
    history_msg_count_max = ensure_even(config.get('history_msg_count_max', 10))
    history_msg_size_max = config.get('history_msg_size_max', 4096)
    completionTokens = preset.get('max_tokens', 1024)
    uidHistoryList = []
    model = preset['model']
    messagesSize = calcMessagesTokens(messages, model)
    askMessage = {"role": "user", "content": content}
    nowSize = calcMessageTokens(askMessage, model) + messagesSize
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
            now_history_bytes = len(now_history_content)
            history_bytes += now_history_bytes
            logging.debug(f"history_bytes: app_id={app_id}, content={now_history_content}, bytes={now_history_bytes}")
        if history_count > history_msg_count_min:
            if history_count > history_msg_count_max:
                logging.debug(f"stop history for history_msg_count_max: app_id={app_id}, history_msg_count_max={history_msg_count_max}, history_count={history_count}")
                break
            if history_bytes > history_msg_size_max:
                logging.debug(f"stop history for history_msg_size_max: app_id={app_id}, history_msg_size_max={history_msg_size_max}, history_count={history_count}")
                break
        if nowSize + historySize + completionTokens < MaxTotalTokens:
            for nowHistory in reversed(nowHistoryList):
                res.append(nowHistory)
            nowSize += historySize
            logging.debug(f'history state: app_id={app_id}, now_prompt_size={nowSize}, history_count={history_count}, history_bytes={history_bytes}')
        else:
            logging.debug(f'stop history for max tokens: app_id={app_id}, now prompt size:{nowSize}')
            break
    return reversed(res)

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

def is_chatgpt_model_4(model):
    return model.startswith("gpt-4")

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
    if is_embedding_model(model):
        multi = 0.2
    count = round(text_size / 1024)
    if  count < 1:
        count = 1
    return count * multi

def calc_used_text_size(preset, response):
    text_size = 0
    model = preset['model']
    if is_chatgpt_model(model):
        for message in preset['messages']:
            text_size += len(message.get('role', ''))
            text_size += len(message.get('content', ''))
        text_size += len(response['choices'][0]['message']['content'].strip())
    elif is_embedding_model(model):
        text_size += len(preset['input'])
    else:
        text_size += len(preset['prompt'])
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
    logging.debug(f"notify butler: app_id={app_id}, event={event}, data={json.dumps(data)}")
    endpoint = os.getenv('LANYING_BUTLER_ENDPOINT', 'https://butler.lanyingim.com')
    try:
        sendResponse = requests.post(f"{endpoint}/app/lanying_connector_event",
                                        headers={'app_id': app_id},
                                        json={'app_id':app_id, 'event':event, 'data':data})
        logging.debug(sendResponse)
    except Exception as e:
        logging.debug(e)

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

def upload(app_id, embedding_name, filename, file_uuid):
    openai_doc_gen.create_embedding(app_id, embedding_name, filename, file_uuid)

def fetch_embeddings(text):
    return openai.Embedding.create(input=text, engine='text-embedding-ada-002')['data'][0]['embedding']

def handle_chat_file(msg, config):
    app_id = msg['appId']
    attachment = json.loads(msg['attachment'])
    url = attachment['url']
    dname = attachment['dName']
    embedding_name,ext = os.path.splitext(dname)
    check_result = check_upload_embedding(msg, config, embedding_name, ext, app_id)
    if check_result['result'] == 'error':
        return check_result['message']
    headers = {'app_id': app_id, 'access-token': config['lanying_admin_token'], 'user_id': config['lanying_user_id']}
    response = requests.get(url, headers=headers)
    dir = os.getenv("LANYING_CONNECTOR_CHAT_FILE_DIR", '/data/upload')
    os.makedirs(dir, exist_ok=True)
    embedding_uuid = str(uuid.uuid4())
    filename = os.path.join(dir, embedding_uuid + ext)
    logging.debug(f"recevie embedding file from chat: app_id:{app_id}, url:{url}, embedding_name:{embedding_name}, embedding_uuid:{embedding_uuid}, filename:{filename}")
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
            return f'上传知识库成功，请等待后续处理. \n知识库名称:{embedding_name}, 知识库ID:{embedding_uuid}'
    return '上传知识库失败'

def check_upload_embedding(msg, config, embedding_name, ext, app_id):
    is_user_in_allow_upload = False
    allow_upload_embedding_names = []
    try:
        preset = copy.deepcopy(config['preset'])
        embeddings = preset.get('ext',{}).get('embeddings',[])
        from_user_id = int(msg['from']['uid'])
        for embedding in embeddings:
            allow_upload_user_ids = embedding.get('allow_upload_user_ids',[])
            if from_user_id in allow_upload_user_ids:
                is_user_in_allow_upload = True
                allow_upload_embedding_names.append(embedding['embedding_name'])
        for embedding in embeddings:
            allow_upload_user_ids = embedding.get('allow_upload_user_ids',[])
            if embedding['embedding_name'] == embedding_name:
                logging.debug(f"check_upload_embedding | app_id:{app_id}, embedding_name:{embedding_name}, from_user_id:{from_user_id}, allow_upload_user_ids:{allow_upload_user_ids}")
                if from_user_id in allow_upload_user_ids:
                    allow_exts  = [".html", ".htm", ".zip"]
                    if ext in allow_exts:
                        return {'result':'ok'}
                    else:
                        return {'result':'error', 'message': f'对不起，暂时只支持{allow_exts}格式的知识库'}
    except Exception as e:
        logging.exception(e)
        pass
    if is_user_in_allow_upload:
        return {'result':'error', 'message':f'对不起，您没有权限上传此知识库, 您可以上传的知识库有{allow_upload_embedding_names}'}
    return {'result':'error', 'message':'对不起，我无法处理文件消息'}
