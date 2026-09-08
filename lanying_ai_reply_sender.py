import json
import logging
import time
from dataclasses import dataclass

from lanying_ai_chat_pipeline import ChatHandlerResult


@dataclass(frozen=True)
class ReplySenderDependencies:
    build_openclaw_reply_ext: object
    add_group_history_metadata: object
    make_metadata_for_text: object
    get_is_sync_mode: object
    save_pending_reply: object
    reply_message_async: object
    redis_provider: object
    group_history_key: object
    add_group_reply_history: object
    add_debug_message: object
    sleep: object = time.sleep


def normalize_chat_handler_result(reply):
    error_code = ''
    error_message = ''
    if isinstance(reply, list):
        logging.info(f"got list reply | {reply}")
        reply_list = reply
    elif isinstance(reply, dict):
        if reply.get('result') == 'error':
            error_code = reply.get('code', '')
            error_message = reply.get('msg', '') or reply.get('message', '')
            reply_list = reply.get('msg_list', [error_message])
        else:
            reply_list = []
    else:
        reply_list = [reply]
    return ChatHandlerResult(reply_list, error_code, error_message)


class ReplySender:
    def emit(self, config, msg, result, dependencies):
        app_id = msg['appId']
        msg_type = msg['type']
        count = 0
        for reply in result.replies:
            if len(reply) == 0:
                continue
            count += 1
            lc_ext = {}
            try:
                ext = json.loads(config['ext'])
                if 'ai' in ext:
                    lc_ext = ext['ai']
                elif 'lanying_connector' in ext:
                    lc_ext = ext['lanying_connector']
            except Exception:
                pass
            reply_ext = {
                'ai': {
                    'stream': False,
                    'role': 'ai',
                    'result': 'error',
                    'error_code': result.error_code,
                    'error_message': (
                        reply if result.error_message == ''
                        else result.error_message),
                }
            }
            reply_ext.update(dependencies.build_openclaw_reply_ext(msg))
            if 'feedback' in lc_ext:
                reply_ext['ai']['feedback'] = lc_ext['feedback']
            group_history = None
            if (count == 1 and msg_type == 'GROUPCHAT'
                    and 'reply_msg_type' in config):
                group_history = {
                    'time': int(time.time()),
                    'type': 'group',
                    'content': reply,
                    'group_id': config['reply_to'],
                    'from': config['reply_from'],
                }
                if 'send_from' in config:
                    group_history['mention_list'] = [int(config['send_from'])]
                dependencies.add_group_history_metadata(
                    group_history, dependencies.make_metadata_for_text())
                if not dependencies.get_is_sync_mode(config):
                    config.setdefault('app_id', app_id)
                    dependencies.save_pending_reply(config, group_history)
            dependencies.reply_message_async(config, reply, reply_ext)
            if (group_history is not None
                    and dependencies.get_is_sync_mode(config)):
                logging.info(f"ADD HISTORY CONFIG:{config}")
                redis = dependencies.redis_provider()
                history_list_key = dependencies.group_history_key(
                    app_id, config['reply_to'])
                dependencies.add_group_reply_history(
                    config, redis, history_list_key, group_history)
            dependencies.sleep(0.5)
        dependencies.sleep(0.5)
        if not config.get('defer_debug_finish_to_openclaw', False):
            dependencies.add_debug_message(
                config, "处理完成", {'is_last_msg': True})
