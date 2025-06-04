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
# Optional Selenium support
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_debug.log'),
        logging.StreamHandler()
    ]
)

# Environment variables
forum_url = os.getenv("FORUM_URL", "https://hjd2048.com/2048/thread.php?fid=3")
fid = os.getenv("FID", "3")
csv_file = os.getenv("CSV_FILE", f"fid_{fid}.csv")
base_url = forum_url.rstrip('/') + '&page='
MAX_RETRIES = 3
RETRY_DELAY = 0.5
COMMIT_INTERVAL = 500
TIMEOUT = 10
MAX_WORKERS = 5

# Random User-Agents to avoid bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]

# Configure session with retries
session = requests.Session()
retries = Retry(total=MAX_RETRIES, backoff_factor=RETRY_DELAY, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def clean_title(title):
    """Clean title, keeping English parts and removing invalid characters"""
    try:
        parts = [part.strip() for part in re.split(r'[／/|]', title)]
        logging.debug(f"Title split: raw='{title}', parts={parts}")
        
        valid_parts = []
        for part in parts:
            match = re.match(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+$', part)
            if match:
                cleaned = match.group(0).strip()
                if len(cleaned) > 3 and not re.match(r'^(720p|1080p|4K)$', cleaned, re.I):
                    valid_parts.append(cleaned)
        
        if valid_parts:
            cleaned = max(valid_parts, key=len)
            logging.debug(f"Cleaned title: raw='{title}', cleaned='{cleaned}'")
            return cleaned
        
        match = re.search(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+', title)
        if match:
            cleaned = match.group(0).strip()
            if len(cleaned) > 3 and not re.match(r'^(720p|1080p|4K)$', cleaned, re.I):
                logging.debug(f"Fallback cleaning: raw='{title}', cleaned='{cleaned}'")
                return cleaned
        
        logging.warning(f"No valid English part in title, keeping raw: {title}")
        return title
    except Exception as e:
        logging.error(f"Title cleaning failed: {title}, error: {e}")
        return title

def init_csv():
    """Initialize CSV file if it doesn't exist"""
    try:
        if not os.path.exists(csv_file):
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"Created CSV file: {csv_file}")
        else:
            file_size = os.path.getsize(csv_file)
            with open(csv_file, 'r', encoding='utf-8') as file:
                line_count = sum(1 for line in file)
            logging.info(f"CSV file exists: {csv_file}, size: {file_size} bytes, lines: {line_count}")
    except Exception as e:
        logging.error(f"Failed to initialize CSV: {e}")
        raise

def configure_git_lfs():
    """Configure Git LFS tracking"""
    try:
        result = subprocess.run(["git", "lfs", "track", csv_file], check=True, capture_output=True, text=True)
        logging.debug(f"Git LFS configured: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git LFS configuration failed: {e.stderr}")
        raise

def git_commit(message):
    """Commit CSV to Git repository"""
    logging.info(f"Preparing commit: {message}")
    try:
        result_add = subprocess.run(["git", "add", csv_file], capture_output=True, text=True, check=True)
        logging.debug(f"Git add: stdout={result_add.stdout}, stderr={result_add.stderr}")
        
        result_commit = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        logging.debug(f"Git commit: stdout={result_commit.stdout}, stderr={result_commit.stderr}")
        
        if result_commit.returncode == 0:
            result_push = subprocess.run(["git", "push"], capture_output=True, text=True, check=True)
            logging.info(f"Git commit successful: {message}")
            logging.debug(f"Git push: stdout={result_push.stdout}, stderr={result_push.stderr}")
        else:
            logging.warning(f"No changes to commit: {result_commit.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git operation failed: {e.stderr}")
        raise

def get_topic_id(url):
    """Extract topic ID from URL"""
    match = re.search(r'tid=(\d+)', url)
    topic_id = match.group(1) if match else None
    logging.debug(f"Extracted topic ID: URL={url}, ID={topic_id}")
    return topic_id

def hash_to_magnet(hash_value):
    """Convert hash to magnet link"""
    if hash_value and re.match(r'^[0-9a-fA-F]{40}$', hash_value):
        magnet = f"magnet:?xt=urn:btih:{hash_value.lower()}"
        logging.debug(f"Converted hash to magnet: {hash_value} -> {magnet}")
        return magnet
    logging.warning(f"Invalid hash: {hash_value}")
    return ""

def get_thread_max_page(topic_url):
    """Get the maximum page number for a thread"""
    try:
        headers_copy = headers.copy()
        headers_copy["Referer"] = forum_url
        headers_copy["User-Agent"] = random.choice(USER_AGENTS)
        response = session.get(topic_url, headers=headers_copy, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_links = soup.select('a[href*="page="]')
        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        
        logging.debug(f"Thread max page: {topic_url}, max_page={max_page}")
        return max_page
    except Exception as e:
        logging.error(f"Failed to get thread max page: {topic_url}, error: {e}")
        return 1

def get_magnet_links(topic_url):
    """Extract magnet links from all pages of a thread, deduplicated"""
    logging.debug(f"Fetching magnet links: {topic_url}")
    try:
        magnet_links = set()
        max_page = get_thread_max_page(topic_url)
        
        for page in range(1, max_page + 1):
            page_url = urljoin("https://hjd2048.com/2048/", f"{topic_url.split('&page=')[0]}&page={page}")
            headers_copy = headers.copy()
            headers_copy["Referer"] = forum_url
            headers_copy["User-Agent"] = random.choice(USER_AGENTS)
            
            response = session.get(page_url, headers=headers_copy, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Save debug HTML
            with open(f"debug_thread_{get_topic_id(page_url)}_page_{page}.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract <a> tags with magnet links
            for a in soup.select('a[href^="magnet:?xt=urn:btih:"]'):
                magnet = a['href'].split('&')[0].lower()
                magnet_links.add(magnet)
            
            # Extract <input> or <textarea> with magnet links
            for elem in soup.select('input[value^="magnet:?xt=urn:btih:"], textarea'):
                if elem.name == 'input':
                    magnet = elem.get('value', '').split('&')[0].lower()
                else:
                    magnet = elem.get_text().strip().split('&')[0].lower()
                if magnet.startswith('magnet:?xt=urn:btih:'):
                    magnet_links.add(magnet)
            
            # Extract 40-char hashes from text
            content = soup.get_text()
            hash_pattern = r'[0-9a-fA-F]{40}'
            hashes = re.findall(hash_pattern, content)
            for h in hashes:
                magnet = hash_to_magnet(h)
                if magnet:
                    magnet_links.add(magnet)
            
            logging.debug(f"Found {len(magnet_links)} magnet links on {page_url}")
        
        magnet_links = list(magnet_links)
        if not magnet_links:
            logging.warning(f"No magnet links or hashes found: {topic_url}")
        else:
            logging.info(f"Found {len(magnet_links)} unique magnet links in {topic_url}")
        return magnet_links
    except Exception as e:
        logging.error(f"Failed to get magnet links: {topic_url}, error: {e}")
        return []

def get_max_page():
    """Extract maximum page number from forum index"""
    logging.info(f"Fetching max page: {base_url}1")
    try:
        headers_copy = headers.copy()
        headers_copy["User-Agent"] = random.choice(USER_AGENTS)
        response = session.get(f"{base_url}1", headers=headers_copy, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_links = soup.select('a[href*="page="], a.last')
        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        
        logging.info(f"Extracted max page: {max_page}")
        return max_page
    except Exception as e:
        logging.error(f"Failed to extract max page: {e}")
        logging.warning("Defaulting to page=1")
        return 1

def crawl_page(page_number, retries=0):
    """Crawl a single forum page"""
    try:
        url = f"{base_url}{page_number}"
        logging.info(f"Crawling page {page_number}: {url}")
        headers_copy = headers.copy()
        headers_copy["User-Agent"] = random.choice(USER_AGENTS)
        response = session.get(url, headers=headers_copy, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Save debug HTML
        with open(f"debug_page_{page_number}.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        # Robust selector for thread rows
        torrent_rows = soup.select('tr[class~="tr3"][class~="t_one"]')
        if not torrent_rows:
            logging.warning(f"No thread rows found on page {page_number}, trying fallback selectors")
            torrent_rows = soup.select('tr:has(a[href*="read.php?tid="])')
        
        if not torrent_rows:
            logging.error(f"No valid thread rows on page {page_number}")
            tr_classes = [tr.get('class', []) for tr in soup.select('tr')]
            logging.debug(f"Page {page_number} <tr> classes: {tr_classes}")
            return []
        
        results = []
        for row in torrent_rows:
            try:
                title_elem = row.select_one('a.subject, a[href*="read.php?tid="]')
                if not title_elem:
                    logging.debug(f"Page {page_number}: Row missing title, skipping")
                    continue
                raw_title = title_elem.get_text(strip=True)
                title = clean_title(raw_title)
                
                topic_url = urljoin("https://hjd2048.com/2048/", title_elem['href'])
                
                publisher_elem = row.select_one('td.tal.y-style a.bl, td.tal.y-style a')
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
                logging.debug(f"Page {page_number}: Added record: {title}")
                
            except Exception as e:
                logging.error(f"Error processing row on page {page_number}: {e}")
                continue
        
        logging.info(f"Page {page_number}: Found {len(results)} records")
        return results
    
    except requests.RequestException as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"Retrying page {page_number} ({retries + 1}/{MAX_RETRIES}), waiting {delay}s: {e}")
            time.sleep(delay)
            return crawl_page(page_number, retries + 1)
        logging.error(f"Failed to crawl page {page_number} after {MAX_RETRIES} retries: {e}")
        return []

def crawl_pages(start_page, end_page):
    """Main crawling logic"""
    logging.info(f"Starting crawl from page {start_page} to {end_page}")
    try:
        configure_git_lfs()
        
        if start_page == 0:
            logging.info("start_page=0, clearing CSV and getting max page")
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"Cleared CSV file: {csv_file}")
            start_page = get_max_page()
            logging.info(f"Set start_page to {start_page}")
        
        init_csv()
        
        total_records = 0
        pages = list(range(start_page, end_page - 1, -1))
        logging.debug(f"Page list: {pages}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {executor.submit(crawl_page, page): page for page in pages}
            
            for future in tqdm(future_to_page, desc="Crawling pages", total=len(pages)):
                page_number = future_to_page[future]
                try:
                    results = future.result()
                    logging.debug(f"Page {page_number}: Returned {len(results)} records")
                    if results:
                        try:
                            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                for data in results:
                                    writer.writerow([data["Page"], data["Title"], data["URL"], 
                                                  data["Publisher"], data["Link"]])
                                    total_records += 1
                                logging.info(f"Page {page_number}: Wrote {len(results)} records to {csv_file}")
                        except Exception as e:
                            logging.error(f"Failed to write CSV for page {page_number}: {e}")
                            continue
                    else:
                        logging.warning(f"Page {page_number}: No data written, empty results")
                    
                    logging.debug(f"Current total records: {total_records}")
                    if total_records >= COMMIT_INTERVAL:
                        logging.info(f"Reached commit interval {COMMIT_INTERVAL}, committing")
                        git_commit(f"Updated {total_records} records up to page {page_number}")
                        total_records = 0
                            
                except Exception as e:
                    logging.error(f"Error processing page {page_number}: {e}")
                    continue
                
                time.sleep(random.uniform(1.0, 3.0))
        
        if total_records > 0:
            logging.info(f"Final commit for {total_records} remaining records")
            git_commit(f"Final update for {total_records} remaining records")
        
        file_size = os.path.getsize(csv_file)
        with open(csv_file, 'r', encoding='utf-8') as file:
            line_count = sum(1 for line in file)
        logging.info(f"Final CSV status: {csv_file}, size: {file_size} bytes, lines: {line_count}")
    
    except Exception as e:
        logging.error(f"Unexpected error in crawl_pages: {e}")
        raise
    finally:
        logging.info("Crawl process completed")

if __name__ == "__main__":
    logging.info("Script started")
    try:
        headers_copy = headers.copy()
        headers_copy["User-Agent"] = random.choice(USER_AGENTS)
        session.get("https://hjd2048.com/2048/", headers=headers_copy, timeout=TIMEOUT)
        logging.info("Session initialized")
    except requests.RequestException as e:
        logging.warning(f"Failed to initialize session: {e}")
    
    start_page = int(os.getenv("START_PAGE", 926))
    end_page = int(os.getenv("END_PAGE", 925))  # Adjusted for testing
    logging.info(f"Crawling forum fid={fid} from page {start_page} to {end_page}")
    crawl_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
    session.close()
    logging.info("Script ended")
