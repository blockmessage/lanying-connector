import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
import re
import time
import os
import random

SELENIUM_URL = os.getenv('LANYING_CONNECTOR_SELENIUM_URL')

def extract_number(text):
    """ 从字符串中提取数字并转换为整数 """
    match = re.search(r"(\d[\d,]*)", text)
    return int(match.group(1).replace(",", "")) if match else 0

def get_driver():
    options = webdriver.ChromeOptions()

    # ✅ 你的基础配置（无头模式 + 规避 Chrome 限制）
    options.add_argument("--incognito")  # 🚀 开启无痕模式
    options.add_argument("--disable-application-cache")  # 禁用应用缓存
    options.add_argument("--disable-cache")  # 禁用缓存
    options.add_argument("--no-sandbox")  # 防止沙盒问题
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
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

def extract_indexed_count(page_source):
    # 方式1（优先）: "该网站共有 <b>2,848</b> 个网页被百度收录"
    match1 = re.search(r"该网站共有\s*<b[^>]*>([\d,]+)</b>\s*个网页被百度收录", page_source)
    count1 = int(match1.group(1).replace(",", "")) if match1 else 0

    # 方式2（备用）: "找到相关结果数约 32 个"
    match2 = re.search(r"找到相关结果数约\s*([\d,]+)\s*个", page_source)
    count2 = int(match2.group(1).replace(",", "")) if match2 else 0

    return max(count1, count2) if (count1 or count2) else None

def check_baidu_index(domain):
    driver = get_driver()
    try:
        # 1️⃣ **访问百度首页**
        driver.get("https://www.baidu.com")
        time.sleep(random.uniform(2, 4))

        # 2️⃣ **找到搜索框并输入 `site:xxxx`**
        search_box = driver.find_element(By.ID, "kw")
        search_box.send_keys(f"site:{domain}")
        time.sleep(random.uniform(1, 2))

        # 3️⃣ **按回车键或点击搜索按钮**
        search_box.send_keys(Keys.RETURN)
        time.sleep(random.uniform(3, 5))

        # 4️⃣ **解析搜索结果**
        page_source = driver.page_source

        # **打印网页HTML**（可选）
        logging.info(f"check_baidu_index | html:{page_source}\n")

        # **检查是否有搜索结果**
        results = driver.find_elements(By.TAG_NAME, "h3")
        if results:
            indexed_count = extract_indexed_count(page_source)
            if indexed_count:
                return {
                    'result': 'ok',
                    'count': indexed_count
                }
            else:
                return {
                    'result': 'error',
                    'message': 'index_num_not_found'
                }

        # **检查是否未收录**
        if "未找到相关结果" in page_source:
            return {
                'result': 'ok',
                'count': 0
            }
        return {
            'result': 'error',
            'message': 'page_unknown'
        }
    except Exception as e:
        logging.exception(e)
        return {
            'result': 'error',
            'message': 'page_exception'
        }
    finally:
        try:
            driver.quit()
        except Exception as ee:
            pass
