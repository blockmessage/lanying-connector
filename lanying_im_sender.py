"""IM message sending shared by the HTTP service and Celery workers.

This module deliberately has no Flask application dependency.  Background
workers can therefore send replies without importing ``lanying_connector``
and triggering the web application's logging and blueprint initialization.
"""

import json
import logging

import requests

import lanying_config
from lanying_async import executor


def send_message_async(app_id, from_user_id, to_user_id, content, ext=None):
    executor.submit(
        send_message, app_id, from_user_id, to_user_id, content, ext or {})


def send_message(app_id, from_user_id, to_user_id, content, ext=None):
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    message_antispam = lanying_config.get_message_antispam(app_id)
    if not admin_token:
        return None
    ext = ext or {}
    logging.info(
        "Send message, from=%s to=%s content=%s, ext:%s",
        from_user_id, to_user_id, content, ext)
    response = requests.post(
        api_endpoint + '/message/send',
        headers={'app_id': app_id, 'access-token': admin_token},
        json={
            'type': 1,
            'from_user_id': from_user_id,
            'targets': [to_user_id],
            'content_type': 0,
            'content': content,
            'config': json.dumps(
                {'antispam_prompt': message_antispam}, ensure_ascii=False),
            'ext': json.dumps(ext, ensure_ascii=False) if ext else '',
        })
    logging.info(response)
    try:
        result = response.json()
        message_ids = result.get('msg_ids', [])
        if message_ids:
            return message_ids[0]
    except Exception:
        pass
    return 0


def send_read_ack_async(app_id, from_user_id, to_user_id, related_mid):
    executor.submit(
        send_read_ack, app_id, from_user_id, to_user_id, related_mid)


def send_read_ack(app_id, from_user_id, to_user_id, related_mid):
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    message_antispam = lanying_config.get_message_antispam(app_id)
    if not admin_token:
        return None
    response = requests.post(
        api_endpoint + '/message/send',
        headers={'app_id': app_id, 'access-token': admin_token},
        json={
            'type': 1,
            'from_user_id': from_user_id,
            'targets': [to_user_id],
            'content_type': 9,
            'content': '',
            'config': json.dumps(
                {'antispam_prompt': message_antispam}, ensure_ascii=False),
            'related_mid': related_mid,
        })
    logging.info(response)


def send_message_oper_async(
        app_id, from_user_id, to_user_id, related_mid, content_type, content,
        ext=None, msg_config=None, online_only=False):
    executor.submit(
        send_message_oper, app_id, from_user_id, to_user_id, related_mid,
        content_type, content, ext or {}, msg_config or {}, online_only)


def send_message_oper(
        app_id, from_user_id, to_user_id, related_mid, content_type, content,
        ext=None, msg_config=None, online_only=False):
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    message_antispam = lanying_config.get_message_antispam(app_id)
    if not admin_token:
        return None
    ext = ext or {}
    msg_config = msg_config or {}
    msg_config['antispam_prompt'] = message_antispam
    logging.info(
        "Send message oper, from=%s to=%s ctype=%s, content=%s, ext:%s, "
        "msg_config:%s, online_only:%s",
        from_user_id, to_user_id, content_type, content, ext, msg_config,
        online_only)
    response = requests.post(
        api_endpoint + '/message/send',
        headers={'app_id': app_id, 'access-token': admin_token},
        json={
            'type': 1,
            'from_user_id': from_user_id,
            'targets': [to_user_id],
            'content_type': content_type,
            'content': content,
            'ext': json.dumps(ext, ensure_ascii=False) if ext else '',
            'config': json.dumps(msg_config, ensure_ascii=False),
            'related_mid': related_mid,
            'online_only': online_only,
        })
    logging.info(response)
