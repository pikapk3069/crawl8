import requests
from bs4 import BeautifulSoup
import csv
import os
import subprocess
import logging
import time
import random
import re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# 取消 selenium 注释以启用动态加载支持
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_debug.log'),
        logging.StreamHandler()
    ]
)

# 环境变量
forum_url = os.getenv("FORUM_URL", "https://hjd2048.com/2048/thread.php?fid=3")
fid = os.getenv("FID", "3")
csv_file = os.getenv("CSV_FILE", f"fid_{fid}.csv")
base_url = forum_url.rstrip('/') + '&page='
MAX_RETRIES = 3
RETRY_DELAY = 0.5
COMMIT_INTERVAL = 500
TIMEOUT = 10
MAX_WORKERS = 5

# 请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://hjd2048.com/"
}

# 配置会话，带重试机制
session = requests.Session()
retries = Retry(total=MAX_RETRIES, backoff_factor=RETRY_DELAY, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def login():
    """执行登录以获取受限页面访问权限"""
    login_url = "https://hjd2048.com/2048/login.php"  # 需确认实际登录URL
    credentials = {
        "username": os.getenv("USERNAME", "your_username"),  # 从环境变量获取
        "password": os.getenv("PASSWORD", "your_password"),
        "questionid": "0",  # 无安全问题，参考 1.html
        "answer": ""
    }
    logging.info(f"尝试登录: {login_url}")
    try:
        response = session.post(login_url, data=credentials, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        if "login" not in response.text.lower() and "access denied" not in response.text.lower():
            logging.info("登录成功")
            return True
        else:
            logging.error("登录失败，可能是凭据错误或页面仍重定向")
            return False
    except Exception as e:
        logging.error(f"登录失败: {e}")
        return False

def clean_title(title):
    """清洗标题，保留英文部分并去除无效字符"""
    try:
        parts = [part.strip() for part in re.split(r'[／/|]', title)]
        logging.debug(f"标题分割: 原始='{title}', 分割后={parts}")
        
        valid_parts = []
        for part in parts:
            match = re.match(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+$', part)
            if match:
                cleaned = match.group(0).strip()
                if len(cleaned) > 3 and not re.match(r'^(720p|1080p|4K)$', cleaned, re.I):
                    valid_parts.append(cleaned)
        
        if valid_parts:
            cleaned = max(valid_parts, key=len)
            logging.debug(f"清洗标题: 原始='{title}', 清洗后='{cleaned}'")
            return cleaned
        
        match = re.search(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+', title)
        if match:
            cleaned = match.group(0).strip()
            if len(cleaned) > 3 and not re.match(r'^(720p|1080p|4K)$', cleaned, re.I):
                logging.debug(f"回退清洗: 原始='{title}', 清洗后='{cleaned}'")
                return cleaned
        
        logging.warning(f"标题无有效英文部分，保留原始: {title}")
        return title
    except Exception as e:
        logging.error(f"清洗标题失败: {title}, 错误: {e}")
        return title

def init_csv():
    """初始化CSV文件，仅当文件不存在时创建"""
    try:
        if not os.path.exists(csv_file):
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"新建CSV文件: {csv_file}")
        else:
            file_size = os.path.getsize(csv_file)
            with open(csv_file, 'r', encoding='utf-8') as file:
                line_count = sum(1 for line in file)
            logging.info(f"CSV文件已存在: {csv_file}, 大小: {file_size}字节, 行数: {line_count}")
    except Exception as e:
        logging.error(f"初始化CSV文件失败: {e}")
        raise

def configure_git_lfs():
    """配置Git LFS跟踪"""
    try:
        result = subprocess.run(["git", "lfs", "track", csv_file], check=True, capture_output=True, text=True)
        logging.debug(f"Git LFS配置成功: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git LFS配置失败: {e.stderr}")
        raise

def git_commit(message):
    """提交CSV文件到Git仓库"""
    logging.info(f"准备提交: {message}")
    try:
        result_add = subprocess.run(["git", "add", csv_file], capture_output=True, text=True, check=True)
        logging.debug(f"Git add 输出: {result_add.stdout}, 错误: {result_add.stderr}")
        
        result_commit = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        logging.debug(f"Git commit 输出: {result_commit.stdout}, 错误: {result_commit.stderr}")
        
        if result_commit.returncode == 0:
            result_push = subprocess.run(["git", "push"], capture_output=True, text=True, check=True)
            logging.info(f"Git提交成功: {message}")
            logging.debug(f"Git push 输出: {result_push.stdout}, 错误: {result_push.stderr}")
        else:
            logging.warning(f"无更改需要提交: {result_commit.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git操作失败: {e.stderr}")
        raise

def get_topic_id(url):
    """从URL提取话题ID"""
    match = re.search(r'tid=(\d+)', url)
    topic_id = match.group(1) if match else None
    logging.debug(f"提取话题ID: URL={url}, ID={topic_id}")
    return topic_id

def hash_to_magnet(hash_value):
    """将哈希值转换为磁力链接"""
    if hash_value and re.match(r'^[0-9a-fA-F]{40}$', hash_value):
        magnet = f"magnet:?xt=urn:btih:{hash_value}"
        logging.debug(f"转换哈希到磁力链接: {hash_value} -> {magnet}")
        return magnet
    logging.warning(f"无效哈希值: {hash_value}")
    return ""

def get_magnet_links(topic_url):
    """从话题页面提取磁力链接"""
    logging.debug(f"获取磁力链接: {topic_url}")
    try:
        response = session.get(topic_url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        if "login" in response.text.lower() or "access denied" in response.text.lower():
            logging.error(f"话题页面可能需要登录: {topic_url}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        magnet_links = []
        
        for a in soup.select('a[href^="magnet:?xt=urn:btih:"]'):
            magnet_links.append(a['href'])
        
        content = soup.get_text()
        hash_pattern = r'[0-9a-fA-F]{40}'
        hashes = re.findall(hash_pattern, content)
        magnet_links.extend([hash_to_magnet(h) for h in hashes if hash_to_magnet(h)])
        
        magnet_links = list(dict.fromkeys(magnet_links))
        
        if not magnet_links:
            logging.warning(f"未找到磁力链接或哈希值: {topic_url}")
            return []
        
        logging.debug(f"在 {topic_url} 找到 {len(magnet_links)} 个磁力链接")
        return magnet_links
    except Exception as e:
        logging.error(f"获取磁力链接失败: {topic_url}, 错误: {e}")
        return []

def get_max_page():
    """从论坛首页提取最大页数"""
    logging.info(f"获取最大页数: {base_url}1")
    try:
        response = session.get(f"{base_url}1", headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        if "login" in response.text.lower() or "access denied" in response.text.lower():
            logging.error("获取最大页数失败：首页需要登录")
            return 1
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_links = soup.select('a[href*="page="]')
        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        
        logging.info(f"提取到最大页数: {max_page}")
        return max_page
    except Exception as e:
        logging.error(f"提取最大页数失败: {e}")
        logging.warning("默认使用页数=1")
        return 1

def crawl_page(page_number, retries=0):
    """爬取单页数据"""
    try:
        url = f"{base_url}{page_number}"
        logging.info(f"爬取页面 {page_number}: {url}")
        response = session.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        # 调试：保存页面 HTML
        with open(f"debug_page_{page_number}.html", "w", encoding="utf-8") as f:
            f.write(response.text)
    
        
        soup = BeautifulSoup(response.text, 'html.parser')
        # 尝试多种选择器
        torrent_rows = soup.select('tr.tr3.t_one') or soup.select('tr[class*="tr3"]') or soup.select('tr')
        if not torrent_rows:
            logging.warning(f"页面 {page_number} 未找到torrent行，尝试了多种选择器")
            tr_classes = [tr.get('class', []) for tr in soup.select('tr')]
            logging.debug(f"页面 {page_number} 的 <tr> 类名: {tr_classes}")
            return []
        
        results = []
        for row in torrent_rows:
            try:
                title_elem = row.select_one('a.subject')
                if not title_elem:
                    logging.debug(f"页面 {page_number} 的行缺少标题元素，跳过")
                    continue
                raw_title = title_elem.get_text(strip=True)
                title = clean_title(raw_title)
                
                topic_url = urljoin("https://hjd2048.com/2048/", title_elem['href'])
                
                publisher_elem = row.select_one('td.tal.y-style a.bl')
                publisher = publisher_elem.get_text(strip=True) if publisher_elem else "Unknown"
                
                magnet_links = get_magnet_links(topic_url)
                link = ";".join(magnet_links) if magnet_links else ""
                
                result = {
                    "Page": page_number,
                    "Title": title,
                    "URL": topic_url,
                    "Publisher": publisher,
                    "Link": link
                }
                results.append(result)
                logging.debug(f"页面 {page_number} 添加记录: {title}")
                
            except Exception as e:
                logging.error(f"处理页面 {page_number} 的行时出错: {e}")
                continue
        
        logging.info(f"页面 {page_number}: 找到 {len(results)} 条记录")
        return results
    
    except requests.RequestException as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"页面 {page_number} 重试 {retries + 1}/{MAX_RETRIES}，等待 {delay}秒: {e}")
            time.sleep(delay)
            return crawl_page(page_number, retries + 1)
        logging.error(f"爬取页面 {page_number} 失败，尝试 {MAX_RETRIES} 次: {e}")
        return []

def crawl_pages(start_page, end_page):
    """主爬取逻辑"""
    logging.info(f"开始爬取，从页面 {start_page} 到 {end_page}")
    try:
        # 登录
        if not login():
            logging.error("登录失败，退出爬取")
            return
        
        configure_git_lfs()
        
        if start_page == 0:
            logging.info("start_page为0，清空CSV并提取最大页数")
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"已清空CSV文件: {csv_file}")
            start_page = get_max_page()
            logging.info(f"设置start_page为 {start_page}")
        
        init_csv()
        
        total_records = 0
        pages = list(range(start_page, end_page - 1, -1))
        logging.debug(f"页面列表: {pages}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {executor.submit(crawl_page, page): page for page in pages}
            
            for future in tqdm(future_to_page, desc="爬取页面", total=len(pages)):
                page_number = future_to_page[future]
                try:
                    results = future.result()
                    logging.debug(f"页面 {page_number} 返回 {len(results)} 条记录")
                    if results:
                        try:
                            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                for data in results:
                                    writer.writerow([data["Page"], data["Title"], data["URL"], 
                                                  data["Publisher"], data["Link"]])
                                    total_records += 1
                                logging.info(f"页面 {page_number}: 写入 {len(results)} 条记录到 {csv_file}")
                        except Exception as e:
                            logging.error(f"写入CSV失败，页面 {page_number}: {e}")
                            continue
                    else:
                        logging.warning(f"页面 {page_number}: 无数据写入，记录为空")
                    
                    logging.debug(f"当前累计记录数: {total_records}")
                    if total_records >= COMMIT_INTERVAL:
                        logging.info(f"达到提交间隔 {COMMIT_INTERVAL}，提交记录")
                        git_commit(f"更新 {total_records} 条记录至页面 {page_number}")
                        total_records = 0
                            
                except Exception as e:
                    logging.error(f"处理页面 {page_number} 时出错: {e}")
                    continue
                
                time.sleep(random.uniform(1.0, 3.0))
        
        if total_records > 0:
            logging.info(f"最后提交剩余 {total_records} 条记录")
            git_commit(f"最后更新剩余 {total_records} 条记录")
        
        file_size = os.path.getsize(csv_file)
        with open(csv_file, 'r', encoding='utf-8') as file:
            line_count = sum(1 for line in file)
        logging.info(f"最终CSV状态: {csv_file}, 大小: {file_size}字节, 行数: {line_count}")
    
    except Exception as e:
        logging.error(f"crawl_pages 中发生未预期错误: {e}")
        raise
    finally:
        logging.info("完成爬取流程")

if __name__ == "__main__":
    logging.info("脚本启动")
    try:
        session.get("https://hjd2048.com/2048/", headers=headers, timeout=TIMEOUT)
        logging.info("已初始化会话")
    except requests.RequestException as e:
        logging.warning(f"初始化会话失败: {e}")
    
    start_page = int(os.getenv("START_PAGE", 926))
    end_page = int(os.getenv("END_PAGE", 1))
    logging.info(f"开始爬取论坛 fid={fid}，从页面 {start_page} 到 {end_page}")
    crawl_pages(start_page, end_page)
    logging.info(f"数据已保存至 {csv_file}")
    session.close()
    logging.info("脚本结束")
