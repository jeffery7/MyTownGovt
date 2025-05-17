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
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # File handler for general logs
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(console_formatter)
    
    # File handler for errors
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

    def is_valid_cached_content(self, url):
        """Check if the cached content is valid (not empty and contains meaningful data)."""
        if not self.is_cached(url):
            return False
        content = self.get_cached(url)
        # Check if the content is empty or just a blank HTML page
        if not content or '<body></body>' in content or len(content.strip()) < 100:
            logger.warning(f"Cached content for {url} is empty or invalid")
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
def take_full_screenshot(driver, output_path, config, prefix='screenshot'):
    logger = logging.getLogger(__name__)
    logger.debug("Taking full screenshot")
    os.makedirs(output_path, exist_ok=True)
    try:
        # Check for content iframe
        iframe_exists = has_iframe(driver, 'content')
        if iframe_exists:
            logger.debug("Switching to content iframe")
            driver.switch_to.frame('content')
        
        # Get full scroll height
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        driver.set_window_size(1920, scroll_height + 100)  # Add padding
        
        # Scroll to capture full page
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)  # Wait for rendering
        
        # Save PNG
        timestamp = int(time.time())
        png_path = os.path.join(output_path, f"{prefix}_{timestamp}.png")
        driver.save_screenshot(png_path)
        logger.info(f"Saved PNG screenshot: {png_path}")
        
        # Convert to PDF
        pdf_path = png_path.replace('.png', '.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(img2pdf.convert(png_path))
        logger.info(f"Saved PDF screenshot: {pdf_path}")
        
        # Switch back to default content if iframe was used
        if iframe_exists:
            driver.switch_to.default_content()
        return png_path, pdf_path
    except Exception as e:
        logger.error(f"Error taking screenshot: {str(e)}", exc_info=True)
        return None, None

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
    logger.debug(f"Fetching page: {url}")

    # Check cache first, unless bypassing
    if not bypass_cache and cache.is_valid_cached_content(url):
        logger.info(f"Using cached content for {url}")
        return cache.get_cached(url)
    
    for attempt in range(retries):
        try:
            driver.get(url)
            # Wait for the page to load and verify content
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
            # Additional wait for JavaScript rendering
            time.sleep(2)
            
            # Verify that the page has meaningful content
            body_content = driver.find_element(By.TAG_NAME, 'body').get_attribute('innerHTML').strip()
            if not body_content or '<body></body>' in driver.page_source:
                raise ValueError("Page loaded but contains no meaningful content")

            # Save raw HTML for debugging
            debug_path = os.path.join(cache.cache_dir, 'debug', f"fetch_{hashlib.md5(url.encode()).hexdigest()}_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            
            # Check for content iframe
            iframe_exists = has_iframe(driver, 'content')
            if iframe_exists:
                logger.debug("Switching to content iframe")
                driver.switch_to.frame('content')
                content = driver.page_source
                driver.switch_to.default_content()
            else:
                logger.debug("No content iframe found, using default content")
                content = driver.page_source
            
            # Verify content before caching
            if '<body></body>' in content or len(content.strip()) < 100:
                raise ValueError("Fetched content is empty or invalid")

            cache.cache_content(url, content)
            return content
        except Exception as e:
            logger.warning(f"Fetch attempt {attempt + 1}/{retries} failed for {url}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to fetch {url} after {retries} attempts: {str(e)}", exc_info=True)
                raise