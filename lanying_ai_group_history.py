import copy
import json
import time


class GroupHistoryRepository:
    pending_reply_expire_seconds = 3600

    def __init__(self, redis_provider, safe_json_loads,
                 get_message_ai_ext, add_group_history_metadata,
                 make_metadata_from_msg, add_history, history_key):
        self.redis_provider = redis_provider
        self.safe_json_loads = safe_json_loads
        self.get_message_ai_ext = get_message_ai_ext
        self.add_group_history_metadata = add_group_history_metadata
        self.make_metadata_from_msg = make_metadata_from_msg
        self.add_history = add_history
        self.history_key = history_key

    def pending_reply_key(self, app_id, group_id, chatbot_user_id,
                          request_msg_id):
        return (
            f"lanying:connector:history:pending:group:{app_id}:{group_id}:"
            f"{chatbot_user_id}:{request_msg_id}"
        )

    def save_pending_reply(self, config, history):
        request_msg_id = str(config.get('request_msg_id', '')).strip()
        if request_msg_id == '':
            return
        key = self.pending_reply_key(
            config['app_id'], config['reply_to'], config['reply_from'],
            request_msg_id)
        pending = {
            field: copy.deepcopy(history[field])
            for field in [
                'function_messages', 'function_messages_owner',
                'subsequent_messages', 'subsequent_messages_owner',
                'mention_list',
            ]
            if field in history
        }
        redis = self.redis_provider()
        if redis:
            pipeline = redis.pipeline(transaction=True)
            pipeline.delete(key)
            pipeline.rpush(key, json.dumps(pending, ensure_ascii=False))
            pipeline.expire(key, self.pending_reply_expire_seconds)
            pipeline.execute()

    def load_pending_reply(self, redis, msg):
        if not redis:
            return {}
        ai_ext = self.get_message_ai_ext(msg)
        request_msg_id = str(ai_ext.get('request_msg_id', '')).strip()
        if request_msg_id == '':
            return {}
        key = self.pending_reply_key(
            msg['appId'], msg['to']['uid'], msg['from']['uid'], request_msg_id)
        pending = self.safe_json_loads(redis.lpop(key), {})
        return pending if isinstance(pending, dict) else {}

    def record_received_message(self, config, msg):
        app_id = msg['appId']
        group_id = msg['to']['uid']
        msg_config = self.safe_json_loads(msg.get('config')) or {}
        history = {
            'time': int(time.time()),
            'type': 'group',
            'content': msg.get('content', ''),
            'group_id': group_id,
            'from': msg['from']['uid'],
            'mention_list': msg_config.get('mentionList', []),
            'mention_all': msg_config.get(
                'mentionAll', msg_config.get('mention_all', False)) is True,
        }
        self.add_group_history_metadata(
            history, self.make_metadata_from_msg(msg))
        redis = self.redis_provider()
        if self.get_message_ai_ext(msg).get('role') == 'ai':
            pending = self.load_pending_reply(redis, msg)
            history.update(pending)
        self.add_history(
            redis, self.history_key(str(app_id), str(group_id)), history)
