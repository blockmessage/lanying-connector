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
import math
import requests
import os

expireSeconds = 86400 * 3
maxUserHistoryLen = 20
MaxTotalTokens = 4000

def handle_chat_message(msg, config):
    reply_message_read_ack(config)
    checkres = check_message_deduct_failed(msg, config)
    if checkres['result'] == 'error':
        return checkres['msg']
    checkres = check_message_per_month_per_user(msg, config)
    if checkres['result'] == 'error':
        return checkres['msg']
    preset = config['preset']
    lcExt = {}
    try:
        ext = json.loads(config['ext'])
        lcExt = ext['lanying_connector']
        if lcExt['preset_name']:
            preset = preset['presets'][lcExt['preset_name']]
    except Exception as e:
        lcExt = {}
    if 'presets' in preset:
        del preset['presets']
    logging.debug(f"lanying-connector:ext={json.dumps(lcExt, ensure_ascii=False)}")
    isChatGPT = is_chatgpt_model(preset['model'])
    if isChatGPT:
        return handle_chat_message_chatgpt(msg, config, preset, lcExt)
    else:
        return ''

def handle_chat_message_chatgpt(msg, config, preset, lcExt):
    app_id = msg['appId']
    check_res = check_message_limit(msg, config)
    if check_res['result'] == 'error':
        logging.info(f"check_message_limit deny: app_id={app_id}, msg={check_res['msg']}")
        return check_res['msg']
    openai_key_type = check_res['openai_key_type']
    logging.info(f"check_message_limit ok: app_id={app_id}, openai_key_type={openai_key_type}")
    content = msg['content']
    init_openai_key(config, openai_key_type)
    messages = preset.get('messages',[])
    now = int(time.time())
    history = {'time':now}
    fromUserId = config['from_user_id']
    toUserId = config['to_user_id']
    historyListKey = historyListChatGPTKey(fromUserId, toUserId)
    redis = lanying_redis.get_redis_connection()
    if 'reset_prompt' in lcExt and lcExt['reset_prompt'] == True:
        removeAllHistory(redis, historyListKey)
    if 'prompt_ext' in lcExt and lcExt['prompt_ext']:
        customHistoryList = []
        for customHistory in lcExt['prompt_ext']:
            if customHistory['role'] and customHistory['content']:
                customHistoryList.append({'role':customHistory['role'], 'content': customHistory['content']})
        addHistory(redis, historyListKey, {'list':customHistoryList, 'time':now})
    if 'need_reply' in lcExt and lcExt['need_reply'] == False:
        return ''
    if content == '#reset_prompt':
        removeAllHistory(redis, historyListKey)
        return 'prompt is reset'
    userHistoryList = loadHistoryChatGPT(config, app_id, redis, historyListKey, content, messages, now, preset)
    for userHistory in userHistoryList:
        logging.debug(f'userHistory:{userHistory}')
        messages.append(userHistory)
    messages.append({"role": "user", "content": content})
    preset['messages'] = messages
    calcMessagesTokens(messages, preset['model'])
    response = openai.ChatCompletion.create(**preset)
    logging.debug(f"openai response:{response}")
    add_message_statistic(msg, config, preset, response, openai_key_type)
    reply = response.choices[0].message.content.strip()
    history['user'] = content
    history['assistant'] = reply
    history['uid'] = fromUserId
    addHistory(redis, historyListKey, history)
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

def init_openai_key(config, openai_key_type):
    openai_api_key = ''
    if openai_key_type == 'share':
        DefaultApiKey = lanying_config.get_lanying_connector_default_openai_api_key()
        if DefaultApiKey:
            openai_api_key = DefaultApiKey
    else:
        openai_api_key = config['openai_api_key']
    openai.api_key = openai_api_key

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

def add_message_statistic(msg, config, preset, response, openai_key_type):
    app_id = msg['appId']
    if 'usage' in response:
        usage = response['usage']
        completion_tokens = usage['completion_tokens']
        prompt_tokens = usage['prompt_tokens']
        total_tokens = usage['total_tokens']
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
                    redis.hincrby(key, 'message_count_quota_share', message_count_quota)
                else:
                    redis.hincrby(key, 'message_count_quota_self', message_count_quota)
                new_message_count_quota = redis.hincrby(key, 'message_count_quota', message_count_quota)
                if key_count == 1 and new_message_count_quota > 100 and (new_message_count_quota+99) // 100 != (new_message_count_quota - message_count_quota+99) // 100:
                    notify_butler(app_id, 'message_count_quota_reached', get_message_limit_state(app_id))
        else:
            logging.error(f"fail to statistic message: app_id={app_id}, model={model}, completion_tokens={completion_tokens}, prompt_tokens={prompt_tokens}, total_tokens={total_tokens},text_size={text_size},message_count_quota={message_count_quota}, openai_key_type={openai_key_type}")

def check_message_limit(msg, config):
    app_id = msg['appId']
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

def check_message_deduct_failed(msg, config):
    app_id = msg['appId']
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

def calc_message_quota(model, text_size):
    multi = 1
    if is_chatgpt_model_4(model):
        multi = 20
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
        text_size += len(response.choices[0].message.content.strip())
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
