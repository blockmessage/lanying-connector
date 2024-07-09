import lanying_chatbot
import lanying_redis
import logging
import json
import lanying_ai_capsule
from lanying_grow_ai import GitBookSummary
import os
import re
import shutil
import requests

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

def add_page_keywords_and_description(directory, max_process_count = 5):
    process_queue = []
    article_id = 0
    for root, dirs, files in os.walk(directory):
        if article_id >= max_process_count:
            break
        for file in files:
            if file.endswith(".md") and file not in ['SUMMARY.md']:
                full_file = os.path.join(root, file)
                with open(full_file) as f:
                    content = f.read()
                    lines = content.splitlines()
                    if len(lines) <= 2:
                        print(f'skip for line too less: {full_file}')
                        continue
                    if re.search(f'^description: .*', content, re.MULTILINE):
                        print(f'skip for description exist: {full_file}')
                        continue
                    match = re.search(r'^(#|title:) (.*)', content, re.MULTILINE)
                    if match:
                        title = match.group(2).strip('" ')
                    else:
                        title = ''
                    summary = extract_summary(content)
                    article_id += 1
                    process_info = {'file': file, 'full_file': full_file, 'content': content, 'summary': summary, 'article_id': article_id, 'title': title}
                    process_queue.append(process_info)
                    if article_id >= max_process_count:
                        break
                    if len(process_queue) >= 10:
                        process_queue_page_keywords_and_description(process_queue)
                        process_queue = []
    if len(process_queue) > 0:
        process_queue_page_keywords_and_description(process_queue)

def process_queue_page_keywords_and_description(process_queue):
    article_infos = []
    for process_info in process_queue:
        article_infos.append({'article_id': process_info['article_id'],'content': process_info['summary']})
    article_infos
    prompt_lines = ['我会给你多个网页的ID和主要内容，请根据输入的网页ID和网页主要内容， 生成每个网页的元数据。请直接返回JSON格式的列表，前后不要有额外内容。',
              '元数据包括：网页ID，网页的描述，网页的搜索引擎SEO关键词，网页的搜索引擎SEO额外关键词。',
              '网页的描述: 用于告知搜索引擎SEO优化的网页描述，默认使用中文，请根据我提供的网页主要内容来总结，150个字符以内，单段不换行，请保持语句通顺完整，结尾有标点表示句子结束。',
              '网页的搜索引擎SEO关键词: 2 个关键词, 字符串类型，多个关键词请以英文逗号和空格连接。',
              '网页的搜索引擎SEO额外关键词: 请从提供的可能的关键词列表中选出 2 个最适合作为本文关键词且不在[网页搜索引擎SEO关键词]里的关键词，可能的关键词列表为：IM SDK,即时通讯SDK,APP内聊天功能,IM开源,IM云服务,PUSH SDK,第三方推送,RTC SDK,实时音视频,Chat AI SDK,企业级AI,AI Agent,AI智能体。',
              '输出格式为:[{"article_id": 1, "description": "", "keywords": "", "extra_keywords":""},{"article_id": 2, "description": "", "keywords": "", "extra_keywords":""}, ...]',
              f'网页主要内容列表为：{json.dumps(article_infos, ensure_ascii=False)}']
    prompt = "\n".join(prompt_lines)
    # print(f'prompt: {prompt}\n')
    url = 'https://connector-preview.lanyingim.com/v1/chat/completions'
    api_key = os.getenv('openapi_key')
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    body = {
        "model": "gpt-3.5-turbo",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 4096
    }
    print("request to openai")
    response = requests.request("POST", url, headers=headers, json=body)
    print(f'respose: {response.text}')
    res = response.json()
    response_message = res['choices'][0]['message']
    reply = response_message.get('content', "")
    metadata_list = json.loads(reply)
    for metadata in metadata_list:
        article_id = metadata.get('article_id',0)
        description = metadata.get('description', '')
        keywords = metadata.get('keywords', '')
        extra_keywords = metadata.get('extra_keywords', '')
        if not(article_id > 0 and description != '' and keywords != '' and extra_keywords != ''):
            print(f"bad metadata: {metadata}")
            continue
        process_info = next((item for item in process_queue if item["article_id"] == article_id), None)
        if process_info is None:
            print(f'bad article_id:{metadata}')
            continue
        full_file = process_info['full_file']
        content = process_info['content']
        title = process_info['title']
        markdown_header = f'---\ndescription: {description}\nkeywords: {keywords}, {extra_keywords}\n---\n'
        print(f'update file:{full_file}\ntitle:{title}\nheader:{markdown_header}')
        with open(full_file, 'w') as f:
            f.write(f'{markdown_header}{content}')

def extract_summary(content):
    lines = []
    # 提取1、2、3级标题
    headers = re.findall(r'^#{1,3}\s+(.*)$', content, flags=re.MULTILINE)
    count = 0
    max_count = 500
    for header in headers:
        header = header.strip()
        count += len(header)
        if count <= max_count or len(lines) == 0:
            lines.append(header)
    if len(lines) < 2:
        lines = [content[:300]]
    return '\n'.join(lines)

def add_readme_file_dir_prefix(base_dir):
    summary_file = os.path.join(base_dir, 'SUMMARY.md')
    with open(summary_file, 'r') as f:
        summary_text = f.read()
    gitbook_summary = GitBookSummary(summary_text=summary_text)
    for summary in gitbook_summary.summary_list:
        type = summary['type']
        if type == 'link':
            link = summary['link']
            pattern = r'/(\d{8}|latest)/README.md'
            match = re.search(pattern, link)
            if match:
                path = os.path.join(base_dir, link)
                print(f"found: {path}")
                with open(path, 'r') as f:
                    content = f.read()
                    lines = content.splitlines()
                    if len(lines) == 1 and '/' not in content:
                        prefix = link.split('/')[0].capitalize()
                        new_content = re.sub(r'# (.*)', r'# {}/\1'.format(prefix), content)
                        print(f"change content: path:{path}\ncontent:{content}\nnew_content:{new_content}")
                        with open(path, 'w') as ff:
                            ff.write(new_content)
