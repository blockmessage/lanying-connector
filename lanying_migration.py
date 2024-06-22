import lanying_chatbot
import lanying_redis
import logging
import json
import lanying_ai_capsule
from lanying_grow_ai import GitBookSummary
import os
import re
import shutil

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

def transform_capsule_income_app_ids():
    redis = lanying_redis.get_redis_connection()
    keys = lanying_redis.redis_keys(redis, 'lanying:connector:statistics:capsule:everymonth:v2:*')
    for key in keys:
        fields = key.split(':')
        if len(fields) == 8:
            app_id = fields[6]
            date_month = fields[7]
            app_ids_key = f"lanying:connector:statistics:capsule_app_ids:everymonth:{date_month}"
            info(f"add app_id{app_id}, to {date_month}")
            redis.hincrby(app_ids_key, app_id, 0)

def transform_summary_remove_date_directory(base_dir):
    summary_file = os.path.join(base_dir, 'SUMMARY.md')
    with open(summary_file, 'r') as f:
        summary_text = f.read()
    gitbook_summary = GitBookSummary(summary_text=summary_text)
    new_summary_list = []
    for summary in gitbook_summary.summary_list:
        type = summary['type']
        if type == 'link':
            link = summary['link']
            pattern = re.compile(r'/(\d{8})/\d+_\d+')
            match = pattern.search(link)
            if match:
                new_link = re.sub(r'/\d{8}/', '/', link)
                print(f"move: {link}, {new_link}")
                old_path = os.path.join(base_dir, link)
                new_path = os.path.join(base_dir, new_link)
                shutil.move(old_path, new_path)
                summary['link'] = new_link
                new_summary_list.append(summary)
            else:
                new_summary_list.append(summary)
        else:
            new_summary_list.append(summary)
    with open(summary_file, 'w') as f:
        f.write(gitbook_summary.to_markdown())
    return gitbook_summary
