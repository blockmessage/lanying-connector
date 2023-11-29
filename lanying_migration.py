import lanying_chatbot
import lanying_redis
import logging
import json

def info(format):
    print(format)
    logging.info(format)

def transform_chatbot_preset(dry_run):
    redis = lanying_redis.get_redis_connection()
    keys = lanying_redis.redis_keys(redis, 'lanying_connector:chatbot:*')
    for key in keys:
        fields = key.split(':')
        if len(fields) == 4:
            app_id = fields[2]
            chatbot_id = fields[3]
            transform_chatbot_preset_one(app_id, chatbot_id, dry_run)

def transform_chatbot_preset_one(app_id, chatbot_id, dry_run):
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot:
        preset = chatbot.get('preset')
        if preset:
            info(f"start check app_id:{app_id}, chatbot_id:{chatbot_id}")
            old_preset_str = json.dumps(preset, ensure_ascii=False)
            changed = False
            if 'stream' not in preset:
                info(f"transform stream to true")
                preset['stream'] = True
                changed = True
            else:
                if preset['stream'] == False:
                    info(f"app_id:{app_id}, chatbot_id:{chatbot_id} stream is False")
            if 'ext' not in preset:
                info(f"Add default ext")
                preset['ext'] = {
                    'debug': False,
                    'stream_interval': 3
                }
                changed = True
            else:
                ext = preset.get('ext')
                if 'stream_interval' not in ext:
                    info(f"add stream_interval")
                    ext['stream_interval'] = 3
                    preset['ext'] = ext
                    changed = True
            if changed:
                preset_str = json.dumps(preset, ensure_ascii=False)
                if dry_run:
                    info(f"finish transform with dry run: app_id:{app_id}, chatbot_id:{chatbot_id}, old_preset_str:{old_preset_str}, preset_str:{preset_str}")
                else:
                    lanying_chatbot.set_chatbot_field(app_id, chatbot_id, "preset", preset_str)
                    info(f"finish transform: app_id:{app_id}, chatbot_id:{chatbot_id}, old_preset_str:{old_preset_str}, preset_str:{preset_str}")
