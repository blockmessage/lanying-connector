from concurrent.futures import ThreadPoolExecutor
import lanying_config
import requests
import json
import logging

from lanying_connector import executor

def send_message_async(config, appId, fromUserId, toUserId, content, ext = {}):
    executor.submit(send_message_async_internal, (config, appId, fromUserId, toUserId, content, ext))

def send_message_async_internal(data):
    config, appId, fromUserId, toUserId, content, ext = data
    send_message(config, appId, fromUserId, toUserId, content, ext)

def send_message(config, appId, fromUserId, toUserId, content, ext = {}):
    adminToken = config['lanying_admin_token']
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