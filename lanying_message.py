from concurrent.futures import ThreadPoolExecutor
import lanying_config
import requests
import json
import logging

from lanying_connector import executor

def send_message_async(config, appId, fromUserId, toUserId, content, ext = {}, msg_config = {}):
    executor.submit(send_message_async_internal, (config, appId, fromUserId, toUserId, content, ext, msg_config))

def send_message_async_internal(data):
    config, appId, fromUserId, toUserId, content, ext, msg_config = data
    send_message(config, appId, fromUserId, toUserId, content, ext, msg_config)

def send_message(config, appId, fromUserId, toUserId, content, ext = {}, msg_config={}):
    adminToken = config['lanying_admin_token']
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        msg_config['antispam_prompt'] = message_antispam
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':1,
                                          'from_user_id':fromUserId,
                                          'targets':[toUserId],
                                          'content_type':0,
                                          'content': content, 
                                          'config': json.dumps(msg_config, ensure_ascii=False),
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else ''})
        logging.info(f"Send message, from={fromUserId} to={toUserId} content={content}")
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

def send_group_message_async(config, app_id, from_user_id, group_id, content, ext = {}, msg_config = {}):
    executor.submit(send_group_message_async_internal, config, app_id, from_user_id, group_id, content, ext, msg_config)

def send_group_message_async_internal(config, appId, fromUserId, groupId, content, ext = {}, msg_config={}):
    adminToken = config['lanying_admin_token']
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        msg_config['antispam_prompt'] = message_antispam
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':2,
                                          'from_user_id':fromUserId,
                                          'targets':[groupId],
                                          'content_type':0,
                                          'content': content, 
                                          'config': json.dumps(msg_config, ensure_ascii=False),
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else ''})
        logging.info(f"Send message, from={fromUserId} groupId={groupId} content={content}")
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