import requests
import logging
import tiktoken
import json
import lanying_redis
import hashlib

ACCESS_TOKEN_REFRESH_TIME = 86400
ASSISTANT_MESSAGE_DEFAULT = '好的'
USER_MESSAGE_DEFAULT = '继续'

def model_configs():
    return [
        {
            "model": 'ERNIE-3.5-8K',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.2,
            "token_limit": 5000,
            "token_limit_type": "prompt",
            'order': 1,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions',
            'function_call': True
        },
        {
            "model": 'ERNIE-3.5-4K-0205',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.2,
            "token_limit": 4000,
            'order': 99,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/ernie-3.5-4k-0205',
            'function_call': True
        },
        {
            "model": 'ERNIE-3.5-8K-0205',
            "type": "chat",
            "is_prefix": False,
            "quota": 4,
            "token_limit": 5000,
            'order': 3,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/ernie-3.5-8k-0205',
            'function_call': True
        },
        {
            "model": 'ERNIE-Bot-turbo',
            "type": "chat",
            "is_prefix": False,
            "quota": 1,
            "token_limit": 10000,
            'order': 4,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/eb-instant',
            'function_call': True
        },
        {
            "model": 'ERNIE-Bot',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.2,
            "token_limit": 5000,
            "token_limit_type": "prompt",
            'order': 5,
            'hidden': True,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions',
            'function_call': True
        },
        {
            "model": 'ERNIE-Bot-8K',
            "type": "chat",
            "is_prefix": False,
            "quota": 1.2,
            "token_limit": 5000,
            "token_limit_type": "prompt",
            'order': 6,
            'hidden': True,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions',
            'function_call': True
        },
        {
            "model": 'ERNIE-Bot-4',
            "type": "chat",
            "is_prefix": False,
            "quota": 12,
            "token_limit": 7000,
            "token_limit_type": "prompt",
            'order': 7,
            "url": 'https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions_pro',
            'function_call': True
        },
        {
            "model": 'Embedding-V1',
            "type": "embedding",
            "is_prefix": False,
            "quota": 0.15,
            'order': 1000,
            "token_limit": 10000,
            'dim': 1536,
            'dim_origin': 1536
        }
    ]

def prepare_chat(auth_info, preset):
    return {
        'api_key': auth_info['api_key'],
        'secret_key': auth_info['secret_key']
    }

def get_access_token(api_key, secret_key):
    key = access_token_key(api_key, secret_key)
    redis = lanying_redis.get_redis_connection()
    access_token = lanying_redis.redis_get(redis, key)
    if access_token:
        return access_token
    url = f"https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={api_key}&client_secret={secret_key}"
    payload = json.dumps("")
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        access_token = response.json().get("access_token")
        if access_token:
            redis.setex(key, ACCESS_TOKEN_REFRESH_TIME, access_token)
            logging.info("refresh baidu access_token: success")
        else:
            logging.info("refresh baidu access_token: fail")
        return access_token
    except Exception as e:
        logging.exception(e)
        logging.info("refresh baidu access_token: fail")
        return None

def chat(prepare_info, preset):
    api_key = prepare_info['api_key']
    secret_key = prepare_info['secret_key']
    access_token = get_access_token(api_key, secret_key)
    if access_token is None:
        logging.info("baidu chat_completion | failed to get access_token")
        return {
            'result': 'error',
            'reason': 'fail_to_get_access_token'
        }
    model_url = get_chat_model_url(preset['model'])
    url = f"{model_url}?access_token={access_token}"
    final_preset = format_preset(prepare_info, preset)
    headers = {"Content-Type": "application/json"}
    try:
        logging.info(f"baidu chat_completion start | model_url={model_url},preset={preset}, access_token_len={len(access_token)}")
        logging.info(f"baidu chat_completion final_preset: \n{json.dumps(final_preset, ensure_ascii=False, indent = 2)}")
        stream = final_preset.get("stream", False)
        if stream:
            response = requests.request("POST", url, headers=headers, json=final_preset, stream=True)
            logging.info(f"baidu chat_completion finish | code={response.status_code}, stream={stream}")
            if response.status_code == 200:
                def generator():
                    completion_tokens = 0
                    for line in response.iter_lines():
                        line_str = line.decode('utf-8')
                        #logging.info(f"stream got line:{line_str}|")
                        if line_str.startswith('data:'):
                            try:
                                data = json.loads(line_str[5:])
                                text = data.get('result','')
                                chunk_info = {'content': text}
                                if 'usage' in data:
                                    chunk_info['usage'] = data['usage']
                                if 'function_call' in data:
                                    chunk_info['function_call'] = {
                                        'name': data['function_call'].get('name'),
                                        'arguments': data['function_call'].get('arguments'),
                                    }
                                if 'finish_reason' in data:
                                    chunk_info['finish_reason'] = data['finish_reason']
                                yield chunk_info
                            except Exception as e:
                                pass
                        else:
                            try:
                                data = json.loads(line_str)
                                if 'error_code' in data and 'error_msg' in data:
                                    logging.info(f"baidu got error:{data}")
                            except Exception as e:
                                pass
                return {
                    'result': 'ok',
                    'reply' : '',
                    'reply_generator': generator(),
                    'usage' : {
                        'completion_tokens': 0,
                        'prompt_tokens': 0,
                        'total_tokens': 0
                    }
                }
            else:
                logging.info(f"fail to get stream: response:{response.text}")
                response_json = {}
                try:
                    response_json = response.json()
                except Exception as e:
                    pass
                return {
                    'result': 'error',
                    'reason': 'bad_status_code',
                    'response': response_json
                }
        else:
            response = requests.request("POST", url, headers=headers, json=final_preset)
            logging.info(f"baidu chat_completion finish | code={response.status_code}, response={response.text}")
            res = response.json()
            error_code = res.get('error_code')
            error_msg = res.get('error_msg')
            if error_code:
                return {
                    'result': 'error',
                    'reason': error_msg,
                    'code': error_code
                }
            usage = res.get('usage',{})
            function_call = None
            if 'function_call' in res:
                function_call = {
                    'name': res['function_call'].get('name'),
                    'arguments': res['function_call'].get('arguments'),
                }
            return {
                'result': 'ok',
                'reply': res['result'],
                'function_call': function_call,
                'finish_reason': res.get('finish_reason', ''),
                'usage': {
                    'completion_tokens' : usage.get('completion_tokens',0),
                    'prompt_tokens' : usage.get('prompt_tokens', 0),
                    'total_tokens' : usage.get('total_tokens', 0)
                }
            }
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to transform response:{response}")
        return {'result':'error',
                'reason':str(response.status_code),
                'response':response}

def prepare_embedding(auth_info, type):
    return {
        'api_key': auth_info['api_key'],
        'secret_key': auth_info['secret_key']
    }

def embedding(prepare_info, model, text):
    api_key = prepare_info['api_key']
    secret_key = prepare_info['secret_key']
    access_token = get_access_token(api_key, secret_key)
    model = "Embedding-V1"
    if access_token is None:
        logging.info("baidu embedding | failed to get access_token")
        return {
            'result': 'error',
            'reason': 'fail_to_get_access_token'
        }
    url = f"https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/embeddings/embedding-v1?access_token={access_token}"
    headers = {"Content-Type": "application/json"}
    body = {
        "input": [
            text
        ]
    }
    logging.info(f"baidu embedding start | access_token_len={len(access_token)}")
    response = requests.post(url, headers=headers, json = body)
    logging.info(f"baidu embedding finish | code={response.status_code}, response={response.text}")
    try:
        res = response.json()
        error_code = res.get('error_code')
        error_msg = res.get('error_msg')
        if error_code:
            return {
                'result': 'error',
                'reason': error_msg,
                'code': error_code
            }
        embedding = res['data'][0]['embedding']
        if len(embedding) < 1536:
            embedding = embedding + [0.0 for i in range(1536-len(embedding))]
        usage = res.get('usage',{})
        return {
            'result': 'ok',
            'model': model,
            'embedding': embedding,
            'usage': {
                'completion_tokens' : usage.get('completion_tokens',0),
                'prompt_tokens' : usage.get('prompt_tokens', 0),
                'total_tokens' : usage.get('total_tokens', 0)
            }
        }
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to transform response:{response}")
        return {
            'result': 'error',
            'reason': 'unknown',
            'model': model,
            'response': response
        }

def encoding_for_model(model): # for temp
    if model == "Embedding-V1":
        return tiktoken.get_encoding("cl100k_base")
    else:
        return tiktoken.encoding_for_model("gpt-3.5-turbo")

def format_preset(prepare_info, preset):
    support_fields = ["messages", "temperature", "top_p", "penalty_score", "user_id", "stream", "stop", "disable_search", "enable_citation", "functions"]
    ret = dict()
    for key in support_fields:
        if key in preset:
            if key == "messages":
                messages = []
                for message in preset['messages']:
                    if 'role' in message and 'content' in message:
                        role = message['role']
                        content = message['content']
                        if len(content) > 0 or 'function_call' in message:
                            if role == "system":
                                messages.append({'role':'user', 'content':content})
                                messages.append({'role':'assistant', 'content':ASSISTANT_MESSAGE_DEFAULT})
                            elif role == "user":
                                if len(messages) > 0 and messages[-1]['role'] == 'user':
                                    messages.append({'role':'assistant', 'content':ASSISTANT_MESSAGE_DEFAULT})
                                messages.append({'role':'user', 'content':content})
                            elif role == 'assistant':
                                if len(messages) > 0 and messages[-1]['role'] in ['user','function']:
                                    if 'function_call' in message:
                                        messages.append({'role':'assistant', 'content':content, 'function_call': message['function_call']})
                                    else:
                                        messages.append({'role':'assistant', 'content':content})
                                else:
                                    logging.info(f"dropping a assistant message: {message}")
                            elif role == 'function':
                                messages.append(message)
                ret[key] = messages
            elif key == "functions":
                functions = []
                for function in preset['functions']:
                    function_obj = {}
                    for k,v in function.items():
                        if k in ["name", "description", "parameters"]:
                            function_obj[k] = v
                    functions.append(function_obj)
                ret[key] = functions
            else:
                ret[key] = preset[key]
    return ret

def access_token_key(api_key, secret_key):
    sha_value = hashlib.sha256((api_key+secret_key).encode('utf-8')).hexdigest()
    return f"lanying-connector:baidu:access-token:{api_key}:{sha_value}"


def get_chat_model_url(model):
    for config in model_configs():
        if config['model'] == model:
            return config['url']
    return None
