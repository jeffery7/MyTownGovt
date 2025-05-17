#!/usr/bin/env python3
import os
import csv
import time
import logging
import pandas as pd
import shutil
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Logging setup
logger = logging.getLogger(__name__)

class MeetingScraper:
    def __init__(self, config):
        self.config = config
        self.data_dir = config['data_dir']
        self.base_url = config['base_url']
        self.cache = Cache(config)
        self.driver = setup_driver()
        self.screenshots_enabled = config.get('screenshots', {}).get('enabled', False)
        self.board_dir = os.path.join(self.data_dir, 'Planning_Board')
        os.makedirs(self.board_dir, exist_ok=True)

    def scrape_meeting(self, meeting_url, meeting_date):
        logger.info(f"Scraping meeting: {meeting_url}")
        try:
            # Fetch page with caching
            content = fetch_page(self.driver, meeting_url, self.cache)
            
            # Take screenshot
            screenshot_data = {}
            if self.screenshots_enabled:
                png_path, pdf_path = take_full_screenshot(self.driver, self.board_dir, self.config, prefix='meeting_screenshot')
                if png_path and pdf_path:
                    screenshot_data = {'png': png_path, 'pdf': pdf_path}
                    logger.info(f"Screenshot saved: PNG={png_path}, PDF={pdf_path}")
            
            # Check for content iframe
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                logger.debug("Switching to content iframe")
                self.driver.switch_to.frame('content')
            
            # Parse meeting details
            details = {
                'url': meeting_url,
                'date': meeting_date,
                'title': self._get_element_text('.meeting-title'),
                'location': self._get_element_text('.meeting-location'),
                'status': self._get_element_text('.meeting-status'),
                'documents': [],
                'screenshots': screenshot_data
            }
            
            # Scrape documents
            doc_elements = self.driver.find_elements(By.CSS_SELECTOR, '.document-link')
            for doc in doc_elements:
                doc_url = doc.get_attribute('href')
                doc_name = doc.text.strip()
                doc_path = self._download_document(doc_url, doc_name)
                if doc_path:
                    details['documents'].append({'name': doc_name, 'path': doc_path})
            
            if iframe_exists:
                self.driver.switch_to.default_content()
            logger.debug(f"Scraped meeting details: {details}")
            return details
        except Exception as e:
            logger.error(f"Error scraping meeting {meeting_url}: {str(e)}", exc_info=True)
            # Save raw HTML for debugging
            debug_path = os.path.join(self.board_dir, 'debug', f"meeting_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            return None

    def _get_element_text(self, selector):
        try:
            element = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return element.text.strip()
        except Exception:
            return ''

    def _download_document(self, url, name):
        try:
            cache_key = self.cache.get_cache_key(url)
            cache_path = self.cache.cache_path(cache_key, ext='pdf')
            if self.cache.is_cached(url):
                logger.info(f"Using cached document: {url}")
                return cache_path
            
            response = requests.get(url)
            response.raise_for_status()
            timestamp = int(datetime.now().timestamp())
            doc_path = os.path.join(self.board_dir, f"{name}_{timestamp}.pdf")
            with open(doc_path, 'wb') as f:
                f.write(response.content)
            
            # Merge with screenshot if available
            merged_path = doc_path.replace('.pdf', '_merged.pdf')
            if os.path.exists(doc_path):
                shutil.copy(doc_path, merged_path)  # Placeholder for merging logic
                logger.info(f"Merged document with screenshot: {merged_path}")
            
            self.cache.cache_content(url, response.content)
            logger.info(f"Downloaded document: {doc_path}")
            return doc_path
        except Exception as e:
            logger.error(f"Error downloading document {url}: {str(e)}", exc_info=True)
            return None

    def save_meeting_data(self, meetings):
        if not meetings:
            logger.warning("No meeting data to save")
            return
        csv_path = os.path.join(self.board_dir, 'meeting_data.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'date', 'title', 'location', 'status', 'documents', 'screenshots'])
            writer.writeheader()
            for meeting in meetings:
                writer.writerow(meeting)
        logger.info(f"Saved meeting data to {csv_path}")

    def close(self):
        self.driver.quit()

def main():
    config = load_config()
    scraper = MeetingScraper(config)
    try:
        # Focus on Planning Board, Sep 10, 2024
        meeting = config['boards'][0]['meetings'][0]
        meeting_data = scraper.scrape_meeting(meeting['url'], meeting['date'])
        if meeting_data:
            scraper.save_meeting_data([meeting_data])
    finally:
        scraper.close()

if __name__ == "__main__":
    main()