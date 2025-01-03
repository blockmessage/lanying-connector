from lanying_async import executor
import requests
import os
import logging
import lanying_redis
from datetime import datetime

def send_message(text, retry_times):
    url = os.getenv('LANYING_CONNECTOR_SLACK_NOTIFY_URL')
    if url is None:
        return
    logging.info(f"send slack message start | text={text}, retry_times={retry_times}")
    try:
        headers = {"Content-Type": "application/json"}
        body = {
            'text': text
        }
        response = requests.post(url, headers=headers, json=body)
        if response.status_code == 200:
            logging.info(f"send slack message success | text={text}, retry_times={retry_times}")
            return
        logging.info(f"send slack message bad status | text={text}, retry_times={retry_times}, status:{response.status_code}, resp:{response.text}")
    except Exception as e:
        pass
    if retry_times > 0:
        logging.info(f"send slack message retrying | text={text}, retry_times={retry_times}")
        executor.submit(send_message, retry_times - 1)
    else:
        logging.info(f"send slack message finally failed | text={text}, retry_times={retry_times}")

def async_send_message(text):
    executor.submit(send_message, text, 3)

def async_send_message_with_filter(text, filter_name):
    executor.submit(send_message_with_filter, text, filter_name)

def send_message_with_filter(text, filter_name):
    date_str = datetime.now().strftime('%Y%m%d')
    redis = lanying_redis.get_redis_connection()
    key = f"lanying_connector:slack_notify:{date_str}:{filter_name}"
    count = redis.incr(key)
    text = f"{text} [count: {count}]"
    if count == 1:
        redis.expire(key, 86400 * 3)
    if count < 20:
        async_send_message(text)
    elif count < 1000:
        if count % 10 == 0:
            async_send_message(text)
    else:
        if count % 100 == 0:
            async_send_message(text)

        