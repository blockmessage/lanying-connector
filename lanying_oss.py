import os
import datetime
import json
import base64
from hashlib import sha1 as sha
import hmac
import time

access_key_id = os.getenv('ALIYUN_OSS_ACCESS_KEY_ID', '')
access_key_secret = os.getenv('ALIYUN_OSS_ACCESS_KEY_SECRET', '')
bucket = os.getenv('ALIYUN_OSS_BUCKET_NAME', '')
endpoint = os.getenv('ALIYUN_OSS_ENDPOINT', '')
cdn_url = os.getenv('ALIYUN_OSS_CDN_URL', '')

upload_url =  endpoint.replace('https://', f'https://{bucket}.')
# 指定过期时间，单位为秒。
expire_time = 20 * 60
max_file_size = 2 * 1024 * 1024


def generate_expiration(seconds):
    """
    通过指定有效的时长（秒）生成过期时间。
    :param seconds: 有效时长（秒）。
    :return: ISO8601 时间字符串，如："2014-12-01T12:00:00.000Z"。
    """
    now = int(time.time())
    expiration_time = now + seconds
    gmt = datetime.datetime.utcfromtimestamp(expiration_time).isoformat()
    gmt += 'Z'
    return gmt


def generate_signature(access_key_secret, expiration, conditions, policy_extra_props=None):
    """
    生成签名字符串Signature。
    :param access_key_secret: 有权限访问目标Bucket的AccessKeySecret。
    :param expiration: 签名过期时间，按照ISO8601标准表示，并需要使用UTC时间，格式为yyyy-MM-ddTHH:mm:ssZ。示例值："2014-12-01T12:00:00.000Z"。
    :param conditions: 策略条件，用于限制上传表单时允许设置的值。
    :param policy_extra_props: 额外的policy参数，后续如果policy新增参数支持，可以在通过dict传入额外的参数。
    :return: signature，签名字符串。
    """
    policy_dict = {
        'expiration': expiration,
        'conditions': conditions
    }
    if policy_extra_props is not None:
        policy_dict.update(policy_extra_props)
    policy = json.dumps(policy_dict).strip()
    policy_encode = base64.b64encode(policy.encode())
    h = hmac.new(access_key_secret.encode(), policy_encode, sha)
    sign_result = base64.b64encode(h.digest()).strip()
    return sign_result.decode()

def sign_upload(object_name):
    policy = {
        # 有效期。
        "expiration": generate_expiration(expire_time),
        # 约束条件。
        "conditions": [
            #["eq", "$success_action_status", "200"],
             ["eq", "$key", object_name],
            # 限制上传Object的最小和最大允许大小，单位为字节。
            ["content-length-range", 1, max_file_size],
            # 限制上传的文件为指定的图片类型
            #["in", "$content-type", ["image/jpg", "image/png"]]
        ]
    }
    signature = generate_signature(access_key_secret, policy.get('expiration'), policy.get('conditions'))
    download_url = os.path.join(cdn_url,object_name)
    return {
        'result': 'ok',
        'data':{
            'download_url': download_url,
            'upload_url': upload_url,
            'oss_body_param':{
                'OSSAccessKeyId': access_key_id,
                'key': object_name,
                'policy': base64.b64encode(json.dumps(policy).encode('utf-8')).decode(),
                'signature': signature
            }
        }
    }

