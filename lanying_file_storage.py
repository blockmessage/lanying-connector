from minio import Minio
import os
import logging
import requests
from langchain.document_loaders import WebBaseLoader
import re
import lanying_url_loader

server = os.getenv("FILE_STORAGE_SERVER", "localhost:9000")
accesskey = os.getenv("FILE_STORAGE_ACCESS_KEY")
screctkey = os.getenv("FILE_STORAGE_SCRECT_KEY")
secure = os.getenv("FILE_STORAGE_SECURE", "false").lower() == "true"
bucket_name = os.getenv("FILE_STORAGE_BUCKET_NAME", "embedding-file")
max_upload_file_size = int(os.getenv("FILE_STORAGE_MAX_UPLOAD_FILE_SIZE", "10737418240"))
client = None
if server:
    if accesskey:
        if screctkey:
            client = Minio(server, access_key=accesskey, secret_key=screctkey, secure=secure)

def put_object(object_name, file_ref, file_size):
    try:
        client.put_object(bucket_name, object_name, file_ref, file_size)
        return {"result":"ok"}
    except Exception as err:
        logging.error(f"Upload zip_file to {object_name} failed:", err)
    return {"result":"error", "message":"fail to upload file"}

def upload(object_name, filename):
    try:
        with open(filename, 'rb') as f:
            file_stat = os.stat(filename)
            file_size = file_stat.st_size
            client.put_object(bucket_name, object_name, f, file_size)
            logging.info(f"Upload {filename} to {object_name} successful")
            return {"result":"ok"}
    except Exception as err:
        logging.error(f"Upload  {filename} to {object_name} failed:", err)
    return {"result":"error", "message":"fail to upload file"}

def download(object_name, filename):
    try:
        data = client.get_object(bucket_name, object_name)
        with open(filename, 'wb') as f:
            for d in data.stream(1024*1024):
                f.write(d)
                logging.info(f'Downloaded {filename} %.2f MB' % (f.tell() / 1024 / 1024))
            logging.info(f'Download {filename} complete')
            return {"result":"ok"}
    except Exception as err:
        logging.error(f"download {filename} failed:", err)
    return {"result":"error", "message":"fail to download file"}

def download_url(url, headers, filename):
    try:
        if len(headers) == 0:
            response = lanying_url_loader.load_url_content(url)
        else:
            headers['Range'] = 'bytes=0-'
            response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 206:
            file_size = int(response.headers.get('Content-Length', "0"))
            if file_size > max_upload_file_size:
                logging.info(f"Download {filename} failed, for file_size:{file_size}")
                return {"result":"error", "message":"file too large"}
            logging.info(f"Download {filename} started, file_size:{file_size}")
            with open(filename, 'wb') as f:
                chunk_size = 1024 * 1024
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
                    logging.info(f'Downloaded {filename} %.2f MB' % (f.tell() / 1024 / 1024))
                logging.info(f'Download {filename} complete')
                return {"result":"ok"}
        elif response.status_code == 200:
            file_size = int(response.headers.get('Content-Length', "0"))
            if file_size > max_upload_file_size:
                logging.info(f"Download {filename} failed, for file_size:{file_size}")
                return {"result":"error", "message":"file too large"}
            logging.info(f"Download {filename} started, file_size:{file_size}")
            with open(filename, 'wb') as f:
                f.write(response.content)
                return {"result":"ok"}
        logging.info(f"download {filename} failed, response.status_code:{response.status_code}")
    except Exception as e:
        logging.exception(e)
    return {"result":"error", "message":"fail to download file"}

def download_url_in_text_format(url, filename):
    try:
        doc = WebBaseLoader(url).load()[0]
        content = remove_space_line(doc.page_content).encode('utf-8')
        with open(filename, 'wb') as f:
            f.write(content)
            return {"result":"ok"}
    except Exception as e:
        logging.exception(e)
    return {"result":"error", "message":"fail to download file"}

def remove_space_line(text):
    lines = text.split('\n')
    new_lines = [line for line in lines if not re.match(r'^\s*$', line)]
    return '\n'.join(new_lines)
