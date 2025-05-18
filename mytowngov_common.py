#!/usr/bin/env python3
import os
import yaml
import logging
import hashlib
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from PIL import Image
import img2pdf
import requests

# Logging setup
def setup_logging(config):
    log_file = config.get('log_file', 'Hardwick_Data/scraper.log')
    error_log_file = config.get('error_log_file', 'Hardwick_Data/errors.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    os.makedirs(os.path.dirname(error_log_file), exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(console_formatter)
    
    error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(console_formatter)
    
    logger.handlers = [console_handler, file_handler, error_handler]
    return logger

# Load configuration
def load_config(config_path='config.yaml'):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    setup_logging(config)
    return config

# Cache utilities
class Cache:
    def __init__(self, config):
        self.enabled = config.get('cache', {}).get('enabled', False)
        self.cache_dir = config.get('cache', {}).get('directory', 'cache')
        self.ttl_hours = config.get('cache', {}).get('ttl_hours', 24)
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_cache_key(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def cache_path(self, key, ext='html'):
        return os.path.join(self.cache_dir, f"{key}.{ext}")

    def is_cached(self, url):
        if not self.enabled:
            return False
        cache_file = self.cache_path(self.get_cache_key(url))
        if not os.path.exists(cache_file):
            return False
        cache_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return (datetime.now() - cache_time) < timedelta(hours=self.ttl_hours)

    def get_cached(self, url):
        cache_file = self.cache_path(self.get_cache_key(url))
        with open(cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    def cache_content(self, url, content):
        if not self.enabled:
            return
        cache_file = self.cache_path(self.get_cache_key(url))
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(content)

    def invalidate_cache(self, url):
        cache_file = self.cache_path(self.get_cache_key(url))
        if os.path.exists(cache_file):
            os.remove(cache_file)
            logger.info(f"Invalidated cache for {url}: {cache_file}")

    def is_valid_cached_content(self, url):
        if not self.is_cached(url):
            return False
        content = self.get_cached(url)
        if not content or '<body></body>' in content or len(content.strip()) < 100:
            logger.warning(f"Cached content for {url} is empty or invalid, invalidating cache")
            self.invalidate_cache(url)
            return False
        if "Past Meetings" not in content and "Upcoming Meetings" not in content and "Boards and Committees" not in content:
            logger.warning(f"Cached content for {url} lacks meaningful data, invalidating cache")
            self.invalidate_cache(url)
            return False
        return True

# Iframe utilities
def has_iframe(driver, iframe_name='content'):
    try:
        driver.find_element(By.ID, iframe_name)
        return True
    except:
        try:
            driver.find_element(By.NAME, iframe_name)
            return True
        except:
            return False

# Screenshot utilities
def take_full_screenshot(driver, screenshot_dir, config, prefix='screenshot', board_name=None, date_str=None):
    logger = logging.getLogger(__name__)
    logger.debug(f"Taking full screenshot with prefix={prefix}, board_name={board_name}, date_str={date_str}")
    os.makedirs(screenshot_dir, exist_ok=True)
    try:
        filename_base = prefix
        if board_name:
            safe_board_name = board_name.replace(" ", "_").replace("/", "_")
            filename_base += f"_{safe_board_name}"
        if date_str:
            date_clean = date_str.replace(" ", "").replace(",", "").replace("-", "")
            if date_clean.startswith("Sep") or date_clean.startswith("Aug"):
                date_clean = datetime.strptime(date_str, "%b %d, %Y %I:%M %p %Z").strftime("%Y%m%d")
            filename_base += f"_{date_clean}"

        png_path = os.path.join(screenshot_dir, f"{filename_base}.png")
        pdf_path = os.path.join(screenshot_dir, f"{filename_base}.pdf")

        # Calculate height based on visible content (div.content)
        try:
            content_element = driver.find_element(By.CSS_SELECTOR, "div.content")
            # Use getBoundingClientRect to get the actual rendered height, including children
            content_height = driver.execute_script(
                "return arguments[0].getBoundingClientRect().bottom", content_element
            )
            logger.debug(f"Visible content height (div.content): {content_height}")
        except NoSuchElementException:
            logger.warning("Could not find div.content, falling back to document.body.scrollHeight")
            content_height = driver.execute_script("return document.body.scrollHeight")
            logger.debug(f"Fallback content height (document.body.scrollHeight): {content_height}")

        max_height = content_height
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                iframe_height = driver.execute_script("return document.body.scrollHeight")
                max_height = max(max_height, iframe_height)
                logger.debug(f"Iframe height: {iframe_height}")
                driver.switch_to.default_content()
            except Exception as e:
                logger.debug(f"Error getting iframe height: {e}")
                driver.switch_to.default_content()

        # Ensure a minimum height to avoid overly small screenshots
        max_height = max(max_height, 600)
        logger.debug(f"Final content height: {max_height}")

        # Calculate content width based on visible content (div.content)
        try:
            content_element = driver.find_element(By.CSS_SELECTOR, "div.content")
            content_width = driver.execute_script("return arguments[0].offsetWidth", content_element)
            logger.debug(f"Visible content width (div.content): {content_width}")
        except NoSuchElementException:
            logger.warning("Could not find div.content, falling back to document.body.scrollWidth")
            content_width = driver.execute_script("return document.body.scrollWidth")
            logger.debug(f"Fallback content width (document.body.scrollWidth): {content_width}")

        # Ensure the viewport width matches the content width, with a minimum of 1200px (consistent with homepage)
        viewport_width = max(content_width, 1200)
        logger.debug(f"Setting viewport width to: {viewport_width}")

        driver.execute_script("window.scrollTo(0, 0);")
        logger.debug("Scrolled back to top of page")

        # Add a small buffer (100px) to the height to ensure all content is captured
        driver.set_window_size(viewport_width, int(max_height) + 100)
        logger.debug(f"Window size set to {viewport_width}x{int(max_height) + 100}")

        driver.save_screenshot(png_path)
        logger.info(f"Saved PNG screenshot: {png_path}")
        
        with open(png_path, 'rb') as f:
            pdf_bytes = img2pdf.convert(f.read())
        with open(pdf_path, 'wb') as f:
            f.write(pdf_bytes)
        logger.info(f"Saved PDF screenshot: {pdf_path}")
        
        return png_path, pdf_path
    except Exception as e:
        logger.error(f"Error taking screenshot: {str(e)}", exc_info=True)
        return None, None

def capture_screenshot(driver, screenshot_dir, config, prefix='screenshot', board_name=None, date_str=None, wait_selector=None):
    logger = logging.getLogger(__name__)
    logger.debug(f"Capturing screenshot with prefix={prefix}, board_name={board_name}, date_str={date_str}, wait_selector={wait_selector}")
    
    try:
        # Check for content iframe with retries
        iframe_exists = False
        for attempt in range(3):
            if has_iframe(driver, 'content'):
                try:
                    driver.switch_to.frame('content')
                    logger.debug(f"Switched to content iframe on attempt {attempt + 1}")
                    iframe_exists = True
                    break
                except Exception as e:
                    logger.warning(f"Failed to switch to content iframe on attempt {attempt + 1}: {e}")
                    driver.switch_to.default_content()
                    time.sleep(2)
            else:
                logger.debug("No content iframe found")
                driver.switch_to.default_content()
                break

        # Wait for content to load
        wait = WebDriverWait(driver, 20)
        if wait_selector:
            try:
                wait.until(EC.presence_of_element_located(wait_selector))
                logger.debug(f"Wait condition met: {wait_selector}")
            except TimeoutException:
                logger.warning(f"Timeout waiting for {wait_selector}, proceeding with screenshot")
        else:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            logger.debug("Body element found, page loaded")

        # Scroll to bottom to load dynamic content
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        logger.debug(f"Final scroll height: {last_height}")

        # Take screenshot
        png_path, pdf_path = take_full_screenshot(driver, screenshot_dir, config, prefix, board_name, date_str)
        if not png_path or not pdf_path:
            raise Exception("Failed to save screenshot")

        return png_path, pdf_path
    except Exception as e:
        logger.error(f"Error capturing screenshot: {e}", exc_info=True)
        debug_path = os.path.join(screenshot_dir, "debug", f"screenshot_error_{int(time.time())}.html")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info(f"Saved debug HTML for screenshot error: {debug_path}")
        return None, None
    finally:
        if iframe_exists:
            try:
                driver.switch_to.default_content()
                logger.debug("Switched back to default content")
            except Exception as e:
                logger.error(f"Error switching back to default content: {e}")

# Selenium setup
def setup_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,4000')
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# Fetch page with caching and retries
def fetch_page(driver, url, cache, retries=3, delay=5, bypass_cache=False):
    logger = logging.getLogger(__name__)
    logger.debug(f"Fetching page: {url}, bypass_cache={bypass_cache}")

    if not bypass_cache and cache.is_valid_cached_content(url):
        logger.info(f"Using cached content for {url}")
        content = cache.get_cached(url)
    else:
        content = None
        if cache.is_cached(url):
            logger.info(f"Invalid cached content for {url}, forcing fresh fetch")
        for attempt in range(retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{retries} to fetch {url}")
                driver.get(url)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )
                time.sleep(2)
                
                body_content = driver.find_element(By.TAG_NAME, 'body').get_attribute('innerHTML').strip()
                if not body_content or '<body></body>' in driver.page_source:
                    raise ValueError("Page loaded but contains no meaningful content")

                debug_path = os.path.join(cache.cache_dir, 'debug', f"fetch_{hashlib.md5(url.encode()).hexdigest()}_{int(time.time())}.html")
                os.makedirs(os.path.dirname(debug_path), exist_ok=True)
                with open(debug_path, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                logger.info(f"Saved debug HTML: {debug_path}")
                
                iframe_exists = has_iframe(driver, 'content')
                if iframe_exists:
                    logger.debug("Switching to content iframe")
                    driver.switch_to.frame('content')
                    content = driver.page_source
                    driver.switch_to.default_content()
                else:
                    logger.debug("No content iframe found, using default content")
                    content = driver.page_source
                
                if '<body></body>' in content or len(content.strip()) < 100:
                    raise ValueError("Fetched content is empty or invalid")

                cache.cache_content(url, content)
                break
            except Exception as e:
                logger.warning(f"Fetch attempt {attempt + 1}/{retries} failed for {url}: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts: {str(e)}", exc_info=True)
                    raise
    return content