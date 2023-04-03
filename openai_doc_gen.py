import json
import os
import re
import tiktoken
import pandas as pd
import openai
import time
import numpy as np
from markdownify import MarkdownConverter
from openai.embeddings_utils import distances_from_embeddings, cosine_similarity

tokenizer = tiktoken.get_encoding("cl100k_base")
fetch_sleep_time = 1.0
fetch_last_time = 0
fetch_count = 0
fetch_max = 0
file_data_frames = {}

class IgnoringScriptConverter(MarkdownConverter):
    """
    Create a custom MarkdownConverter that ignores script tags
    """
    def convert_script(self, el, text, convert_as_inline):
        return ''
    def convert_style(self, el, text, convert_as_inline):
        return ''

# Create shorthand method for conversion
def md(html, **options):
    return IgnoringScriptConverter(**options).convert(html)

def html(file):
    with open(file, "r") as f:
        html = f.read()
        print(md(html))

def generate(names):
    configs = load_configs()
    found = False
    for config in configs:
        if names is None or config['name'] in names:
            found = True
            config['blocks'] = []
            config['block_index'] = 0
            config['total_tokens'] = 0
            process_config(config)
            save_blocks(config)
    if not found:
        print(f"{names} not found in config")
    else:
        total_tokens = 0
        total_block = 0
        for config in configs:
            if 'total_tokens' in config:
                total_tokens += config['total_tokens']
                block_cnt = len(config['blocks'])
                total_block += block_cnt
                print(f"{config['name']}: {block_cnt} blocks, {config['total_tokens']} tokens, filename is {get_csv_filename(config)}")
        print(f"total: {total_block} blocks, {total_tokens} tokens")

def fill_embeddings(names):
    print(f"fill_embeddings:names={names}")
    configs = load_configs()
    found = False
    for config in configs:
        if names is None or config['name'] in names:
            found = True
            fill_embeddings_by_config(config)
    if not found:
        print(f"{names} not found in config")

def search_prompt(filename, text, openai_api_key, max_tokens = 2048, max_blocks = 1000, use_cache = False):
    if use_cache:
        global file_data_frames
        if filename in file_data_frames:
            df = file_data_frames[filename]
        else:
            df=pd.read_csv(filename, index_col=0)
            df['embeddings'] = df['embeddings'].apply(eval).apply(np.array)
            file_data_frames[filename] = df
    else:
        df=pd.read_csv(filename, index_col=0)
        df['embeddings'] = df['embeddings'].apply(eval).apply(np.array)
    openai.api_key = openai_api_key
    q_embeddings = openai.Embedding.create(input=text, engine='text-embedding-ada-002')['data'][0]['embedding']
    df['distances'] = distances_from_embeddings(q_embeddings, df['embeddings'].values, distance_metric='cosine')
    blocks = []
    now_tokens = 0
    blocks_num = 0
    for i, row in df.sort_values('distances', ascending=True).iterrows():
        now_tokens += row['num_of_tokens']
        blocks_num += 1
        if now_tokens > max_tokens:
            break
        if blocks_num > max_blocks:
            break
        blocks.append({'text':row['text'], 'distance':row['distances'], 'num_of_tokens': row['num_of_tokens'], 'summary': row.get('summary', "{}")})
        #print(f"--------------distance:{row['distances']}------------\n{row['text']}")
    return blocks

def load_configs():
     with open("openai_doc_gen.json", "r") as f:
        configs = json.load(f)
        for config in configs['docs']:
            for k,v in configs.items():
                if k != "docs" and k not in config:
                    config[k] = v
        return configs['docs']

def fill_embeddings_by_config(config):
    openai.api_key = config['openai_api_key']
    global fetch_sleep_time, fetch_max
    fetch_sleep_time = 60 / config.get('embedding_requests_per_minute', 30)
    rpm = config.get('embedding_requests_per_minute',30)
    filename = get_csv_filename(config)
    df = pd.read_csv(filename, index_col=0)
    print(f"start fill_embeddings: name={config['name']}, filename={filename}, embedding_requests_per_minute={rpm}")
    fetch_max = len(df.index)
    df['embeddings'] = df.text.apply(fetch_embeddings)
    df.to_csv(filename)
    print(df.head())

def fetch_embeddings(text):
    global fetch_last_time, fetch_count, fetch_max
    now = time.time()
    if now < fetch_last_time + fetch_sleep_time:
        time.sleep(fetch_last_time + fetch_sleep_time - now)
    fetch_last_time = time.time()
    fetch_count += 1
    print(f"fetch_embeddings: start progress={fetch_count}/{fetch_max}")
    for i in range(1, 12):
        try:
            result = openai.Embedding.create(input=text, engine='text-embedding-ada-002')['data'][0]['embedding']
            print("fetch_embeddings: result=ok")
            return result
        except Exception as e:
            print("fetch_embeddings: result=error")
            print(e)
            time.sleep(10)
    return None

def save_blocks(config):
    df = pd.DataFrame(config['blocks'], columns = ['id', 'parent_id', 'num_of_tokens', 'text', 'filename', 'summary'])
    df['embeddings'] = None
    filename = get_csv_filename(config)
    df.to_csv(filename)

def get_csv_filename(config):
    out_dir = config.get('out_dir', 'embeddings')
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    return f"{out_dir}/{config['name']}.csv"

def process_config(config):
    for file in config['files']:
        path = file['path']
        summary = file.get('summary', {})
        if os.path.isfile(path):
            process_file(config, path, summary)
        else:
            for root, _, files in os.walk(path):
                for filename in files:
                    fullfilename = os.path.join(root, filename)
                    process_file(config, fullfilename, summary)

def process_file(config, fullfilename, summary):
    match = re.match(".*(\\.html$|\\.htm$)", fullfilename)
    if match:
        print(f"processing file: {fullfilename}")
        with open(fullfilename, "r") as f:
            html = f.read()
            process_markdown(config, md(html), fullfilename, summary)
    else:
        print(f"ignore file:{fullfilename}")

def process_markdown(config, markdown, fullfilename, summary):
    markdown = pre_remove(config, markdown)
    markdown = remove_space_line(markdown)
    # print(f"========================= MARKDOWN CONTENT ===============================\n{markdown}")
    rule = config.get('block_split_rule',"^#{1,3} ")
    for block in re.split(rule ,markdown, flags=re.MULTILINE):
        process_block(config, block, fullfilename, summary)

def process_block(config, block, fullfilename, summary):
    parent_block_index = config['block_index'] + 1
    lines = []
    tokenCnt = 0
    max_block_size = config.get('max_block_size', 500)
    for line in block.split('\n'):
        lineTokenCnt = num_of_tokens(line)
        if tokenCnt + lineTokenCnt > max_block_size:
            if tokenCnt > 0:
                config['block_index'] += 1
                now_block = "\n".join(lines) + "\n"
                config['blocks'].append((config['block_index'], parent_block_index, tokenCnt, now_block, fullfilename, summary))
                config['total_tokens'] += tokenCnt
                print(f"-- FOUND BLOCK: num_of_tokens:{tokenCnt} index:{config['block_index']}, parent:{parent_block_index}, total_token:{config['total_tokens']} --")
                # print(now_block)
                lines = []
                tokenCnt = 0
        if lineTokenCnt > max_block_size:
            print(f"drop too long line: {line}")
            continue
        lines.append(line)
        tokenCnt += lineTokenCnt
    if tokenCnt > 0:
        config['block_index'] += 1
        now_block = "\n".join(lines) + "\n"
        config['blocks'].append((config['block_index'], parent_block_index, tokenCnt, now_block, fullfilename, summary))
        config['total_tokens'] += tokenCnt
        print(f"-- FOUND BLOCK: num_of_tokens:{tokenCnt} index:{config['block_index']}, parent:{parent_block_index}, total_token:{config['total_tokens']} --")
        # print(now_block)

def remove_space_line(text):
    lines = text.split('\n')
    new_lines = [line for line in lines if not re.match(r'^\s*$', line)]
    return '\n'.join(new_lines)

def pre_remove(config, markdown):
    for rule in config.get('markdown_replace_rules',[]):
        print(f"markdown_replace_rule: {rule.get('name', 'no_name')}")
        markdown = re.sub(rule['patten'], rule.get('replacement',''), markdown, flags=re.DOTALL)
    return markdown

def num_of_tokens(str):
    return len(tokenizer.encode(str))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='MyTool')
    subparsers = parser.add_subparsers()
    generate_parser = subparsers.add_parser('generate')
    generate_parser.add_argument('--names', nargs='+')
    generate_parser.set_defaults(subcommand='generate')
    fill_parser = subparsers.add_parser('fill-embeddings')
    fill_parser.add_argument('--names', nargs='+')
    fill_parser.set_defaults(subcommand='fill-embeddings')
    args = parser.parse_args()
    if args.subcommand == 'generate':
        generate(args.names)
    elif args.subcommand == 'fill-embeddings':
        fill_embeddings(args.names)
    else:
        print(f"Command not found")