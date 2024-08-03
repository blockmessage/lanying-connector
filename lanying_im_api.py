import lanying_config
import requests
import logging
import time
import lanying_file_storage
import json
from lanying_async import executor
import uuid
import os
import lanying_utils
import lanying_redis

def get_user_profile(app_id, user_id):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        return get_user_profile_with_token(app_id, user_id, adminToken)

def get_user_profile_with_token(app_id, user_id, adminToken):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    response = requests.get(apiEndpoint + '/user/profile',
                                headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                params={})
    result = response.json()
    logging.info(f"get user profile, app_id={app_id} user_id={user_id}, result:{result}")
    return result


def set_user_profile(app_id, user_id, description, nick_name, private_info, public_info = ''):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        body = {}
        if description and len(description) > 0:
            body['description'] = description
        if nick_name and len(nick_name) > 0:
            body['nick_name'] = nick_name
        if private_info and len(private_info) > 0:
            body['private_info'] = private_info
        if public_info and len(public_info) > 0:
            body['public_info'] = public_info
        if len(body) == 0:
            return None
        response = requests.post(apiEndpoint + '/user/profile',
                                    headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                    json=body)
        result = response.json()
        logging.info(f"set user profile, app_id={app_id} user_id={user_id}, result:{result}")
        return result

def set_user_avatar(app_id, user_id, avatar_url):
    if avatar_url and len(avatar_url) > 0:
        config = lanying_config.get_lanying_connector(app_id)
        if config:
            adminToken = config['lanying_admin_token']
            apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
            response = requests.post(apiEndpoint + '/user/avatar',
                                        headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                        json={
                                            'avatar': avatar_url
                                        })
            result = response.json()
            logging.info(f"set_user_avatar, app_id={app_id} user_id={user_id}, avatar={avatar_url}, result:{result}")
            return result

def get_user_avatar_upload_url(app_id, user_id):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        response = requests.get(apiEndpoint + '/file/upload/avatar/user',
                                    headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                    params={})
        result = response.json()
        logging.info(f"get_user_avatar_upload_url, app_id={app_id} user_id={user_id}, result:{result}")
        return result

def get_avatar_real_download_url(app_id, user_id, avatar_url):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        if avatar_url.startswith(apiEndpoint):
            response = requests.get(avatar_url,
                                    headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                    params={'image_type':"2"}, allow_redirects=False)
            if response.status_code == 302:
                # 处理重定向逻辑，获取重定向的地址等信息
                redirected_url = response.headers['Location']
                logging.info(f"get_user_avatar_upload_url, app_id={app_id} user_id={user_id}, redirected_url:{redirected_url}")
                return redirected_url
        else:
            return avatar_url

# extra = {'image_type': , 'format': amr/mp3}
def get_attachment_real_download_url(config, app_id, user_id, attachment_url, extra):
    adminToken = config['lanying_admin_token']
    if lanying_utils.is_lanying_url(attachment_url):
        response = requests.get(attachment_url,
                                headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                params=extra, allow_redirects=False)
        if response.status_code == 302:
            # 处理重定向逻辑，获取重定向的地址等信息
            redirected_url = response.headers['Location']
            logging.info(f"get_attachment_real_download_url, app_id={app_id} user_id={user_id}, redirected_url:{redirected_url}")
            return redirected_url
    return attachment_url

def get_attachment_real_download_url_with_cache(config, app_id, user_id, attachment_url, extra):
    redis = lanying_redis.get_redis_connection()
    cache_key = f"get_attachment_real_download_url_with_cache:{app_id}:{user_id}:{attachment_url}"
    info = lanying_redis.redis_get(redis, cache_key)
    if info is None:
        result = get_attachment_real_download_url(config, app_id, user_id, attachment_url, extra)
        redis.setex(cache_key, 25 * 60, result)
        return result
    return info

def set_group_name(app_id, group_id, name):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        body = {
            'group_id': group_id,
            'value': name
        }
        response = requests.post(apiEndpoint + '/group/info/name',
                                    headers={'app_id': app_id, 'access-token': adminToken, 'group_id': str(group_id)},
                                    json=body)
        result = response.json()
        logging.info(f"set_group_name, app_id={app_id} group_id={group_id}, result:{result}")
        return result

def get_user_file_upload_url(app_id, user_id, file_type, to_type, to_id):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        params = {
            'file_type': file_type,
            'to_type': to_type,
            'to_id': to_id
        }
        response = requests.get(apiEndpoint + '/file/upload/chat',
                                    headers={'app_id': app_id, 'access-token': adminToken, 'user_id': str(user_id)},
                                    params=params)
        result = response.json()
        logging.info(f"get_user_file_upload_url, app_id={app_id} user_id={user_id}, params={params}, result:{result}")
        return result

def download_url(config, app_id, user_id, url, filename, extra = {}):
    headers = {'app_id': app_id,
            'access-token': config['lanying_admin_token'],
            'user_id': str(user_id)}
    return lanying_file_storage.download_file_url(url, headers, filename, extra)

def download_url_and_upload_to_im(app_id, user_id, url, file_suffix, file_type, to_type, to_id):
    temp_filename = f"/tmp/{app_id}_{user_id}_{int(time.time())}_{uuid.uuid4()}.{file_suffix}"
    download_result = lanying_file_storage.download_file_url(url, {}, temp_filename)
    if download_result['result'] == 'ok':
        file_size = os.path.getsize(temp_filename)
        upload_info = get_user_file_upload_url(app_id, user_id, file_type, to_type, to_id)
        if upload_info and upload_info['code'] == 200:
            upload_info_data = upload_info.get('data', {})
            upload_url = upload_info_data.get('upload_url', '')
            download_url = upload_info_data.get('download_url', '')
            oss_body_param = upload_info_data.get('oss_body_param', {})
            files = {
                'file': (f'file.{file_suffix}', open(temp_filename, 'rb'), 'image/png'),
            }
            data = {
                'OSSAccessKeyId': oss_body_param.get('OSSAccessKeyId', ''),
                'policy': oss_body_param.get('policy', ''),
                'signature': oss_body_param.get('signature', ''),
                'callback': oss_body_param.get('callback', ''),
                'key': oss_body_param.get('key', ''),
            }
            response = requests.post(upload_url, headers={}, files=files, data=data)
            logging.info(f"upload to oss result | app_id:{app_id}, user_id:{user_id}, response.status_code:{response.status_code}, response_text:{response.text}")
            if response.status_code == 200:
                return {'result': 'ok', 'url': download_url, "file_size": file_size}
            else:
                return {'result': 'error', 'message': 'fail to upload'}
        else:
            return {'result': 'error', 'message': 'fail to get upload url'}
    return {'result': 'error', 'message': 'fail to download from url'}

def upload_chat_file(app_id, user_id, file_suffix, file_content_type, file_type, to_type, to_id, filename):
    upload_info = get_user_file_upload_url(app_id, user_id, file_type, to_type, to_id)
    if upload_info and upload_info['code'] == 200:
        upload_info_data = upload_info.get('data', {})
        upload_url = upload_info_data.get('upload_url', '')
        download_url = upload_info_data.get('download_url', '')
        oss_body_param = upload_info_data.get('oss_body_param', {})
        files = {
            'file': (f'file.{file_suffix}', open(filename, 'rb'), file_content_type),
        }
        data = {
            'OSSAccessKeyId': oss_body_param.get('OSSAccessKeyId', ''),
            'policy': oss_body_param.get('policy', ''),
            'signature': oss_body_param.get('signature', ''),
            'callback': oss_body_param.get('callback', ''),
            'key': oss_body_param.get('key', ''),
        }
        response = requests.post(upload_url, headers={}, files=files, data=data)
        logging.info(f"upload to oss result | app_id:{app_id}, user_id:{user_id}, response.status_code:{response.status_code}, response_text:{response.text}")
        if response.status_code == 200:
            return {'result': 'ok', 'url': download_url}
        else:
            return {'result': 'error', 'message': 'fail to upload'}
    else:
        return {'result': 'error', 'message': 'fail to get upload url'}
    

def send_message_async(config, app_id, from_user_id, to_user_id, type, content_type, content, extra = {}):
    executor.submit(send_message_sync, config, app_id, from_user_id, to_user_id, type, content_type, content, extra)

def send_message_sync(config, app_id, from_user_id, to_user_id, type, content_type, content, extra = {}):
    logging.info(f"Send message received, from={from_user_id} to={to_user_id} type={type}, content_type={content_type} content={content} extra={extra}")
    ext = extra.get('ext', {})
    attachment = extra.get('attachment', {})
    msg_config = extra.get('msg_config', {})
    online_only = extra.get('online_only', False)
    related_mid = extra.get('related_mid', 0)
    adminToken = config['lanying_admin_token']
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    message_antispam = lanying_config.get_message_antispam(app_id)
    if 'download_args' in extra:
        download_args = extra['download_args']
        upload_res = download_url_and_upload_to_im(*download_args)
        logging.info(f"send_message_sync download url upload_res:{upload_res}")
        if upload_res['result'] == 'ok':
            download_url = upload_res['url']
            attachment['fLen'] = upload_res['file_size']
            attachment['url'] = download_url
    if adminToken:
        msg_config['antispam_prompt'] = message_antispam
        logging.info(f"Send message start post, from={from_user_id} to={to_user_id} type={type}, content_type={content_type} content={content} extra={extra}, attachment={attachment}")
        sendResponse = requests.post(apiEndpoint + '/message/send',
                                    headers={'app_id': app_id, 'access-token': adminToken},
                                    json={'type':type,
                                          'from_user_id':from_user_id,
                                          'targets':[to_user_id],
                                          'content_type':content_type,
                                          'content': content,
                                          'attachment': json.dumps(attachment, ensure_ascii=False) if attachment else '',
                                          'config': json.dumps(msg_config, ensure_ascii=False),
                                          'ext': json.dumps(ext, ensure_ascii=False) if ext else '',
                                          'online_only': online_only,
                                          'related_mid': related_mid})
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

def get_wechat_official_account_access_token(config, app_id):
    wechat_app_id = config['wechat_app_id']
    wechat_app_secret = config['wechat_app_secret']
    if wechat_app_id and wechat_app_secret:
        adminToken = config['lanying_admin_token']
        apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
        signature = lanying_utils.sha256(wechat_app_id + ":" + wechat_app_secret)
        params = {
            'signature': signature
        }
        response = requests.get(apiEndpoint + '/app/official_account/access_token',
                                    headers={'app_id': app_id, 'access-token': adminToken},
                                    params=params)
        result = response.json()
        logging.info(f"get_wechat_official_account_access_token, app_id={app_id}, status_code:{response.status_code}")
        return result
    else:
        logging.info(f"get_wechat_official_account_access_token cannot get wechat app_id or secret:app_id:{app_id}")
    return {}
