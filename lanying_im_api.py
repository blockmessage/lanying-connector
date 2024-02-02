import lanying_config
import requests
import logging

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


def set_user_profile(app_id, user_id, description, nick_name, private_info):
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
