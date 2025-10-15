import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup, Comment
import re
import time
import os
import random
import requests

SELENIUM_URL = os.getenv('LANYING_CONNECTOR_SELENIUM_URL')

def load_url_content(url):
    driver = get_driver()
    try:
        driver.get(url)
        time.sleep(random.uniform(5, 7))  # 等待页面加载
        return driver_to_response(driver)
    except Exception as e:
        logging.info("error:", e)
        return make_error_response(driver)
    finally:
        driver.quit()

def get_driver():
    options = webdriver.ChromeOptions()

    # ✅ 你的基础配置（无头模式 + 规避 Chrome 限制）
    options.add_argument("--no-sandbox")  # 防止沙盒问题
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")

    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    # ✅ 启动远程 Selenium
    driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)

    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        driver.execute_script("window.chrome = { runtime: {} }")

        driver.execute_script("navigator.plugins.length = 3")
        driver.set_page_load_timeout(10)   # 页面加载最多 10 秒
        driver.set_script_timeout(10)      # 脚本执行最多 10 秒
        driver.implicitly_wait(5)          # **隐式等待 5 秒**（减少 find_element 失败）
    except Exception as e:
        pass
    return driver

def driver_to_response(driver):
    """
    将 Selenium 的 driver 当前页面内容封装成类似 requests.Response 的对象
    """
    resp = requests.Response()

    # 网页 URL
    resp.url = driver.current_url

    # 网页源码内容（text / content）
    html = driver.page_source
    resp._content = html.encode("utf-8")
    resp.encoding = "utf-8"

    # 状态码 —— Selenium 无法直接取到，默认 200（可手动补充）
    resp.status_code = 200

    # 响应头 —— 由于 Selenium 拿不到 HTTP header，这里伪造常见部分
    resp.headers = {
        "Content-Type": "text/html; charset=utf-8",
        "User-Agent": driver.execute_script("return navigator.userAgent;"),
    }
    return resp

def make_error_response(driver):
    resp = requests.Response()
    resp.url = driver.current_url
    resp._content = "error"
    resp.encoding = "utf-8"
    resp.status_code = 500
    resp.headers = {
        "Content-Type": "text/html; charset=utf-8"
    }
    return resp

def minimize_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")  # 或 "html.parser"

    # 删除 <script>, <style>, <noscript>
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # 删除 link 引用 CSS 或非内容 link
    for tag in soup.find_all("link"):
        if tag.get("rel") == ["stylesheet"] or tag.get("href"):
            tag.decompose()

    # 删除 meta 标签
    for tag in soup.find_all("meta"):
        tag.decompose()

    # 删除 HTML 注释
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # 删除内联事件属性 onclick, onmouseover, etc
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr.lower().startswith("on")]
        for attr in attrs_to_remove:
            del tag[attr]

        # 删除 class 和 style 属性
        if "class" in tag.attrs:
            del tag["class"]
        if "style" in tag.attrs:
            del tag["style"]

    # 删除空标签（不破坏结构的前提下）
    for tag in soup.find_all():
        if not tag.contents and tag.name not in ["br", "hr", "img", "input"]:
            tag.decompose()

    return str(soup)