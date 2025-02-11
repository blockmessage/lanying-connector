import os
import json
import base64
import lanying_api_proxy
import logging
import time

module_name = 'lanying_google_analytics'
service_account_info = None

def list_accounts():
    creds = get_credentials()
    from googleapiclient.discovery import build
    
    # 使用服务对象创建 API 客户端
    service = build('analyticsadmin', 'v1beta', credentials=creds)
    
    try:
        # 调用 API 获取账户列表
        accounts = service.accounts().list().execute()
        
        print(f"accounts:{accounts}")
        
        if 'accounts' in accounts:
            for account in accounts['accounts']:
                print(f"Account Name: {account['displayName']}, Account ID: {account['name']}")
        else:
            print('No accounts found.')
    except Exception as e:
        print(f'An error occurred: {e}')

def create_properties(app_id, tenement_id, site_id):
    logging.info(f"create_properties start | app_id:{app_id}, tenement_id:{tenement_id}, site_id:{site_id}")
    if lanying_api_proxy.client_enabled():
        args = {
            'app_id': app_id,
            'tenement_id': tenement_id,
            'site_id': site_id
        }
        return lanying_api_proxy.proxy_request(module_name,  'create_properties', args)
    else:
        try:
            service = get_service()

            # 创建属性的请求体
            property_body = {
                "parent": f"accounts/{get_account_id()}",
                "displayName": f"grow_ai_{tenement_id}_{app_id}_{site_id}",  # 属性名称
                "timeZone": "Asia/Shanghai",  # 时区
                "currencyCode": "CNY"  # 货币代码
            }

            # 调用 API 创建属性
            response = service.properties().create(body=property_body).execute()
            logging.info(f"Created Property:{response}, {type(response)}")
            return {
                'result': 'ok',
                'data': response
            }
        except Exception as e:
            logging.exception(e)
            return {
                'result': 'error',
                'message': 'fail_to_create'
            }

def create_stream(app_id, tenement_id, site_id, property_name, site_url):
    logging.info(f"create_stream start | property_name:{property_name}")
    if lanying_api_proxy.client_enabled():
        args = {
            'app_id': app_id,
            'tenement_id': tenement_id,
            'site_id': site_id,
            'property_name': property_name,
            'site_url': site_url
        }
        return lanying_api_proxy.proxy_request(module_name,  'create_stream', args)
    else:
        try:
            service = get_service()
            now = int(time.time())
            stream_body = {
                'displayName': f"stream_{now}",  # 数据流的显示名称
                'type': 'WEB_DATA_STREAM',  # 流类型：Web
                'webStreamData': {
                    'defaultUri': site_url,
                }
            }
            # 调用 API 创建 Web 数据流
            response = service.properties().dataStreams().create(parent=property_name, body=stream_body).execute()
            logging.info(f"Created Web Data Stream:{response}, {type(response)}")
            return {
                'result': 'ok',
                'data': response
            }
        except Exception as e:
            logging.exception(e)
            return {
                'result': 'error',
                'message': 'fail_to_create'
            }

def get_credentials():
    global service_account_info
    if service_account_info is None:
        service_account_json_str = base64.b64decode(os.getenv('LANYING_CONNECTOR_GOOGLE_ANALYTICS_AUTH_INFO'))
        service_account_info = json.loads(service_account_json_str)
    scopes = [
        'https://www.googleapis.com/auth/analytics.edit'
    ]
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(service_account_info,scopes=scopes)
    return credentials

def get_service():
    credentials = get_credentials()
    from googleapiclient.discovery import build
    # 构建 Admin API 客户端
    service = build("analyticsadmin", "v1beta", credentials=credentials)
    return service

def get_account_id():
    return os.getenv('LANYING_CONNECTOR_GOOGLE_ANALYTICS_ACCOUNT_ID','')
