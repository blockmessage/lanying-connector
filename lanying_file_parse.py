
import lanying_redis
import logging
import uuid
import time
import re
import tiktoken
from markdownify import MarkdownConverter
import requests
import os
import random
import numpy as np
from redis.commands.search.query import Query
import pandas as pd
from pdfminer.high_level import extract_text
import hashlib
import subprocess
import docx2txt
from langchain.text_splitter import RecursiveCharacterTextSplitter
import lanying_url_loader
import json
import pdfplumber
import lanying_config
import lanying_pgvector
from urllib.parse import urlparse
from openai_token_counter import openai_token_counter
import lanying_chatbot
import lanying_config
import lanying_ai_capsule

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

def extract_text(filename, file_extension):
    content = ''
    try:
        if file_extension in [".html", ".htm"]:
            content = parse_html(filename)
        elif file_extension in [".csv"]:
            content = parse_csv(filename)
        elif file_extension in [".txt"]:
            content = parse_txt(filename)
        elif file_extension in [".pdf"]:
            content = parse_pdf(filename)
        elif file_extension in [".md"]:
            content = parse_markdown(filename)
        elif file_extension in [".doc"]:
            content = parse_doc(filename)
        elif file_extension in [".docx"]:
            content = parse_docx(filename)
        elif file_extension in [".xlsx", ".xls"]:
            content = parse_xlsx(filename)
        return format_text(content)
    except Exception as e:
        logging.error("fail to extract_text for file:{filename}, file_extension:{file_extension}")
        logging.exception(e)
        return ''

def parse_html(filename):
    with open(filename, "r") as f:
        html = f.read()
        return md(html)

def format_text(text):
    text = remove_space_line(text)
    return text.replace('\0','')

def remove_space_line(text):
    lines = text.split('\n')
    new_lines = [line for line in lines if not re.match(r'^\s*$', line)]
    return '\n'.join(new_lines)

def parse_csv(filename):
    df = pd.read_csv(filename, header=None)
    df = df.fillna('')
    header = ''
    rows = []
    for i, row in df.iterrows():
        if i == 0:
            header = f"Table Header:{row}\n"
            header = header[:1024]
        else:
            if i % 10 == 0:
                rows.append(header)
            rows.append(f"Row({i}):{row}\n")
    return '\n'.join(rows)

def parse_txt(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        return content

def parse_pdf(filename):
    return extract_pdf(filename)

def extract_pdf(filename):
    try:
        with pdfplumber.open(filename) as pdf:
            texts = []
            tables = []
            for page in pdf.pages:
                texts.append(page.extract_text())
                for table in page.extract_tables():
                    rows = []
                    for row in table:
                        rows.append(f"{row}")
                    tables.append("\n".join(rows))
            return "\n".join(texts) + "\n\n" + "\n\n".join(tables)
    except Exception as e:
        logging.info("failed to extract pdf by pdfplumber")
        logging.exception(e)
        return extract_text(filename)

def parse_markdown(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        return content

def parse_doc(filename):
    try:
        output = subprocess.check_output(['antiword', filename])
        text = output.decode('utf-8', 'ignore')
        return text
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to convert the document: filename:{filename}")
        raise e

def parse_docx(filename):
    text = docx2txt.process(filename)
    return text

def parse_xlsx(filename):
    xl_file = pd.ExcelFile(filename)
    for sheet_name in xl_file.sheet_names:
        df = pd.read_excel(filename, sheet_name=sheet_name, header=None)
        df = df.fillna('')
        