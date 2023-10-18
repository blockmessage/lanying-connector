from typing import Iterator
from urllib.parse import urlparse
import requests
import logging
from langchain.docstore.document import Document
from bs4 import BeautifulSoup
import uuid
import lanying_redis
from urllib.parse import urljoin
import json
import os

# this file is edit from source of langchain.document_loaders.recursive_url_loader

def create_task(urls):
    redis = lanying_redis.get_redis_stack_connection()
    task_id = str(uuid.uuid4())
    for url in urls:
        if url.startswith("http://") or url.startswith("https://"):
            redis.rpush(to_visit_key(task_id), to_json({'url':url, 'depth':0}))
            redis.sadd(visited_key(task_id), url)
    return task_id

def clean_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    redis.delete(to_visit_key(task_id), visited_key(task_id), found_key(task_id))

def info_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    return {
        'to_visit': redis.llen(to_visit_key(task_id)) + redis.llen(to_visit_key_old(task_id)),
        'visited': redis.scard(visited_key(task_id)) + redis.scard(visited_key_old(task_id)),
        'found': redis.scard(found_key(task_id))
    }

def do_task(task_id, urls, max_depth, filters)-> Iterator[Document]:
    if len(urls) == 0:
        return
    first_url = urls[0]
    redis = lanying_redis.get_redis_stack_connection()
    parse_first_url= urlparse(first_url.strip(' '))
    root_url = f"{parse_first_url.scheme}://{parse_first_url.netloc}/"
    http_root_url = f"http://{parse_first_url.netloc}/"
    while(True):
        to_visit_bytes = redis.lpop(to_visit_key(task_id))
        if to_visit_bytes is None:
            break
        to_visit_info = from_json(to_visit_bytes)
        url = to_visit_info['url']
        depth = to_visit_info['depth']
        if redis.sismember(found_key(task_id), url):
            continue
        logging.info(f"load_url:  task_id:{task_id}, visit url:{url}, root_url:{root_url},depth:{depth}, max_depth:{max_depth}, filters:{filters}")
        try:
            response = load_url_content(url)
            content_type = response.headers.get('Content-Type')
            if response.status_code == 200 and (content_type.startswith('text/html') or content_type.startswith('text/plain')):
                response_url = response.url
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = [link.get("href") for link in soup.find_all("a")]
                for li in soup.find_all("li", attrs={'data-link': True}):
                    all_links.append(li['data-link'])

                absolute_paths = list(
                    {
                        format_link(urljoin(response_url, link))
                        for link in all_links
                    }
                )

                for link in absolute_paths:
                    # logging.info(f"sub link:{link}, visit_filter_url:{visit_filter_url},filter_url:{filter_url}")
                    if depth < max_depth and (link.startswith(root_url) or link.startswith(http_root_url)) and not redis.sismember(visited_key(task_id), link):
                        logging.info(f"add to_visit:  task_id:{task_id}, visit url:{url}, link:{link}, root_url:{root_url}, child_depth:{depth + 1}")
                        redis.rpush(to_visit_key(task_id), to_json({'url':link, 'depth': depth + 1}))
                        redis.sadd(visited_key(task_id), link)
                found = False
                if filters == []:
                    found = True
                else:
                    for filter in filters:
                        if len(filter) > 0 and (url.startswith(filter) or url.startswith(filter.replace('https://','http://'))):
                            found = True
                            break
                if found:
                    redis.sadd(found_key(task_id), url)
                    yield Document(page_content=response.text, metadata={'source':url, 'page_bytes':response.content})
                else:
                    yield None
            else:
                logging.info(f"skip for status_code:{response.status_code}, url:{url}, content_type:{content_type}")
        except Exception as e:
            logging.exception(e)

def load_url_content(url):
    engine = os.getenv("URL_LOAD_ENGINE", "requests")
    if engine == "splash":
        scroll_site_list = ['mp.weixin.qq.com/mp/appmsgalbum?']
        for site in scroll_site_list:
            if site in url:
                return load_url_content_with_scroll(url)
        splash_site_list = ['tgo.infoq.cn', 'www.ceair.com', "mafengwo.cn"]
        for site in splash_site_list:
            if site in url:
                return load_url_content_with_splash(url)
        return requests.get(url,timeout=(20.0, 60.0))
    else:
        return requests.get(url,timeout=(20.0, 60.0))

def load_url_content_with_splash(url):
    splash_url = os.getenv("SPLASH_URL")
    params = {
        'url': url,
        'wait': 5,
    }
    try:
        return requests.get(splash_url + '/render.html', params=params, timeout=(20.0, 60.0))
    except Exception as e:
        logging.info("fail to load by splash: fallback to requests")
        logging.exception(e)
        return requests.get(url,timeout=(20.0, 60.0))

def load_url_content_with_scroll(url):
    engine = os.getenv("URL_LOAD_ENGINE", "requests")
    if engine == "splash":
        splash_url = os.getenv("SPLASH_URL")
        scroll_js = """
function main(splash, args)
    local start_time = os.clock()
    assert(splash:go(args.url))
    assert(splash:wait(1.0))  -- 等待页面加载

    local previous_html = ""  -- 用于存储上一个页面的HTML内容
    local current_html = splash:html()  -- 获取当前页面的HTML内容
    local scroll_count = 0  -- 用于计数滚动次数

    while current_html ~= previous_html and scroll_count < 100 do
        -- 模拟滚动，这里滚动到页面底部
        splash:runjs("window.scroll(0, document.body.scrollHeight);")
        assert(splash:wait(2.0))  -- 等待新内容加载

        previous_html = current_html  -- 更新上一个页面的HTML内容
        current_html = splash:html()  -- 获取当前页面的HTML内容

        scroll_count = scroll_count + 1  -- 增加滚动次数计数器

    end

    local execution_time = os.clock() - start_time
    
    return current_html
end
"""
        params = {
            'url': url,
            'lua_source': scroll_js,
            'wait': 2,
            'timeout': 290
        }
        try:
            return requests.get(splash_url + '/execute', params=params, timeout=(20.0, 300.0))
        except Exception as e:
            logging.info("fail to load by splash: fallback to requests")
            logging.exception(e)
            return requests.get(url,timeout=(20.0, 60.0))
    else:
        return requests.get(url,timeout=(20.0, 60.0))


def load_url_content_with_click(url):
    engine = os.getenv("URL_LOAD_ENGINE", "requests")
    if engine == "splash":
        splash_url = os.getenv("SPLASH_URL")
        scroll_js = """


function main(splash, args)
  -- 打开要测试的网页
  assert(splash:go(args.url))

  -- 等待页面加载完成
  assert(splash:wait(2))

  -- 获取页面的HTML
  local html = splash:html()

  -- 创建一个表来存储不重复的URL
  local unique_urls = {}

  -- 获取页面中的所有元素
  local elements = splash:select_all("*")

  -- 遍历每个元素并模拟点击
  for i, element in ipairs(elements) do
    local bounds = element:bounds()

    if bounds.width > 0 and bounds.height > 0 then
      local clicked = splash:mouse_click(bounds.left + bounds.width / 2, bounds.top + bounds.height / 2)
      splash:wait(2)  -- 等待页面加载完成

      if clicked then
        -- 获取新的URL
        local new_url = splash:evaljs("window.location.href")
        -- 添加新URL到表中
        table.insert(unique_urls, new_url)
        -- 返回到原始页面
        splash:go(args.url)
        splash:wait(2)
      end
    end
  end

  -- 返回收集到的不重复URL
  return {
    html = html,
    unique_urls = unique_urls
  }
end



"""
        params = {
            'url': url,
            'lua_source': scroll_js,
            'wait': 2,
            'timeout': 290
        }
        try:
            return requests.get(splash_url + '/execute', params=params, timeout=(20.0, 300.0))
        except Exception as e:
            logging.info("fail to load by splash: fallback to requests")
            logging.exception(e)
            return requests.get(url,timeout=(20.0, 60.0))
    else:
        return requests.get(url,timeout=(20.0, 60.0))


def format_link(url):
    url = url.strip(" ")
    index = url.find("#")
    if index != -1:
        return url[:index]
    return url

def to_visit_key_old(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:to_visit"

def to_visit_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:to_visit:v2"


def visited_key_old(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visited"

def visited_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visited:"

def found_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:found"

def to_json(dict):
    return json.dumps(dict, ensure_ascii=False)
def from_json(text):
    return json.loads(text)
