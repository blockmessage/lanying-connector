import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
import re
import time
import os
import random
import lanying_api_proxy

SELENIUM_URL = os.getenv('LANYING_CONNECTOR_SELENIUM_URL')
MODULE_NAME = 'lanying_google'

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

def check_index(domain):
    logging.info(f"lanying_google check_index start | domain:{domain}")
    if lanying_api_proxy.client_enabled():
        args = {
            'domain': domain
        }
        return lanying_api_proxy.proxy_request(MODULE_NAME,  'check_index', args)
    else:
        driver = get_driver()
        try:
            # 先打开 Google 首页
            driver.get("https://www.google.com")
            time.sleep(random.uniform(2, 4))  # 等待页面加载
            
            # 在搜索框输入 site:domain 并回车
            search_box = driver.find_element(By.NAME, "q")
            search_box.send_keys(f"site:{domain}")
            time.sleep(random.uniform(1, 2))
            search_box.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5)) # 等待搜索结果加载
            # 4️⃣ **解析搜索结果**
            page_source = driver.page_source

            # **打印网页HTML**（可选）
            logging.info(f"check_baidu_index | html:{page_source}\n")
            
            # 获取结果统计文本
            result_stats = driver.find_element(By.ID, "result-stats").get_attribute("innerText")
            logging.info(f"Google 返回的统计信息:{result_stats}")
            
            # 解析收录数量
            match = re.search(r"About ([\d,]+) results", result_stats)
            if match:
                count = int(match.group(1).replace(",", ""))
                return {
                    'result': 'ok',
                    'count': count
                }
            else:
                return {
                    'result': 'error',
                    'message': 'index_num_not_found'
                }
        except Exception as e:
            logging.info("error:", e)
            return {
                'result': 'error',
                'message': 'page_exception'
            }
        finally:
            driver.quit()
