from typing import Iterator
from urllib.parse import urlparse
import requests
import logging
from langchain.docstore.document import Document
from bs4 import BeautifulSoup
import uuid
import lanying_redis

# this file is edit from source of langchain.document_loaders.recursive_url_loader

def create_task(url):
    redis = lanying_redis.get_redis_stack_connection()
    task_id = str(uuid.uuid4())
    redis.rpush(to_visit_key(task_id), url)
    redis.sadd(visited_key(task_id), url)
    return task_id

def clean_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    redis.delete(to_visit_key(task_id), visited_key(task_id), found_key(task_id))

def info_task(task_id):
    redis = lanying_redis.get_redis_stack_connection()
    return {
        'to_visit': redis.llen(to_visit_key(task_id)),
        'visited': redis.scard(visited_key(task_id)),
        'found': redis.scard(found_key(task_id))
    }

def do_task(task_id, root_url)-> Iterator[Document]:
    redis = lanying_redis.get_redis_stack_connection()
    base_path = urlparse(root_url).path
    while(True):
        url = redis.lpop(to_visit_key(task_id))
        if url is None:
            if redis.scard(found_key(task_id)) < 10:
                parsed_url = urlparse(root_url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                if not base_url.endswith("/"):
                    base_url += "/"
                if not redis.sismember(visited_key(task_id), base_url):
                    logging.info(f"try find more | task_id:{task_id}, root_url:{root_url}, base_url:{base_url}")
                    redis.rpush(to_visit_key(task_id), base_url)
                    redis.sadd(visited_key(task_id), base_url)
                    continue
            break
        url = url.decode("utf-8")
        if redis.sismember(found_key(task_id), url):
            continue
        # Construct the base and parent URLs
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        parent_url = "/".join(parsed_url.path.split("/")[:-1])
        current_path = parsed_url.path
        logging.info(f"load_url:  visit url:{url}, task_id:{task_id}, root_url:{root_url}, base_url:{base_url}, parent_url:{parent_url},base_path:{base_path}, current_path:{current_path}")

        # Add a trailing slash if not present
        if not base_url.endswith("/"):
            base_url += "/"
        if not parent_url.endswith("/"):
            parent_url += "/"

        # Get all links that are relative to the root of the website
        try:
            response = requests.get(url,timeout=(10.0, 60.0))
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = [link.get("href") for link in soup.find_all("a")]

                # Extract only the links that are children of the current URL
                child_links = list(
                    {
                        format_link(link)
                        for link in all_links
                        if link and link.startswith("/")
                    }
                )

                # Get absolute path for all root relative links listed
                absolute_paths = [
                    f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}{link}"
                    for link in child_links
                ]

                # Store the visited links and recursively visit the children
                for link in absolute_paths:
                    # Check all unvisited links
                    if not redis.sismember(visited_key(task_id), link):
                        redis.rpush(to_visit_key(task_id), link)
                        redis.sadd(visited_key(task_id), link)
                if current_path.startswith(base_path):
                    redis.sadd(found_key(task_id), url)
                    yield Document(page_content=response.text, metadata={'source':url, 'page_bytes':response.content})
            else:
                logging.info(f"skip for status_code:{response.status_code}, url:{url}")
        except Exception as e:
            logging.exception(e)

def format_link(url):
    url = url.strip(" ")
    index = url.find("#")
    if index != -1:
        return url[:index]
    return url

def to_visit_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:to_visit"

def visited_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:visited"

def found_key(task_id):
    return f"lanying-embedding:url-loader:task:{task_id}:found"