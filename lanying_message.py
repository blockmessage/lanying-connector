from concurrent.futures import ThreadPoolExecutor
import lanying_config
import requests
import json
import logging

from lanying_async import executor

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
                    logging.info(f"Send message response msg_ids:{msg_ids}")
                    return msg_ids[0]
        except Exception as e:
            pass
        return 0

def send_group_message_async(config, app_id, from_user_id, group_id, content, ext = {}, msg_config = {}):
    executor.submit(send_group_message_sync, config, app_id, from_user_id, group_id, content, ext, msg_config)

def send_group_message_sync(config, appId, fromUserId, groupId, content, ext = {}, msg_config={}):
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
                    logging.info(f"Send message response msg_ids:{msg_ids}")
                    return msg_ids[0]
        except Exception as e:
            pass
        return 0

def send_message_oper_async(config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    executor.submit(send_message_oper_sync, config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext, msg_config, online_only)

def send_message_oper_sync(config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    adminToken = config['lanying_admin_token']
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

def send_group_message_oper_async(config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    executor.submit(send_group_message_oper_sync, config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext, msg_config, online_only)

def send_group_message_oper_sync(config, appId, fromUserId, toUserId, relatedMid, ctype, content, ext = {}, msg_config = {}, online_only = False):
    adminToken = config['lanying_admin_token']
    apiEndpoint = lanying_config.get_lanying_api_endpoint(appId)
    message_antispam = lanying_config.get_message_antispam(appId)
    if adminToken:
        logging.info(f"Send group message oper, from={fromUserId} to={toUserId} ctype={ctype}, content={content}, ext:{ext}, msg_config:{msg_config}, online_only:{online_only}")
        msg_config['antispam_prompt'] = message_antispam
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': appId, 'access-token': adminToken},
                                    json={'type':2,
                                          'from_user_id':fromUserId,
                                          'targets':[toUserId],
                                          'content_type':ctype,
                                          'content': content,
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else '',
                                          'config': json.dumps(msg_config, ensure_ascii=False),
                                          'related_mid':relatedMid,
                                          'online_only': online_only})
        logging.info(sendResponse)
