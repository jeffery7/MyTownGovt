#!/usr/bin/env python3
import os
import csv
import time
import logging
import pandas as pd
import shutil
import requests
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Logging setup
logger = logging.getLogger(__name__)

class MeetingScraper:
    def __init__(self, config_path='config.yaml'):
        self.config = load_config(config_path)
        self.data_dir = self.config['data_dir']
        self.cache = Cache(self.config)
        self.driver = setup_driver()
        self.wait = WebDriverWait(self.driver, 15)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.input_csv = self.config['meetings_input_csv']
        self.output_csv = self.config['meetings_output_csv']

    def scrape_meetings(self):
        if not os.path.exists(self.input_csv):
            logger.error(f"Meeting CSV not found: {self.input_csv}")
            return None

        meetings_df = pd.read_csv(self.input_csv)
        expected_columns = ['board_name', 'date', 'details_url']
        if not all(col in meetings_df.columns for col in expected_columns):
            logger.error(f"CSV {self.input_csv} missing required columns: {expected_columns}, found: {list(meetings_df.columns)}")
            return None

        logger.debug(f"Meeting CSV contents:\n{meetings_df.to_string()}")
        meeting_data = []

        meetings_to_scrape = []
        if self.config.get('focus_mode_meetings', False):
            focus_date = self.config.get('focus_date')
            meetings_to_scrape = meetings_df[meetings_df['date'].str.startswith(focus_date)].to_dict('records')
            logger.info(f"Focus mode enabled, scraping only meetings on: {focus_date}")
        else:
            meetings_to_scrape = meetings_df.to_dict('records')
            logger.info("Scraping all meetings")

        for meeting in meetings_to_scrape:
            board_name = meeting['board_name']
            meeting_url = meeting['details_url']
            meeting_date = meeting['date']
            logger.info(f"Scraping meeting: {meeting_url}")
            result = self.scrape_meeting(board_name, meeting_url, meeting_date)
            if result:
                meeting_data.append(result)

        if meeting_data:
            self.save_meeting_data(meeting_data)
        return meeting_data

    def scrape_meeting(self, board_name, meeting_url, meeting_date):
        logger.info(f"Scraping meeting: {meeting_url}")
        try:
            # Create meeting-specific directory
            board_dir = os.path.join(self.data_dir, board_name)
            date_prefix = meeting_date.split(' ')[0].replace('-', '')
            meeting_dir = os.path.join(board_dir, date_prefix)
            os.makedirs(meeting_dir, exist_ok=True)

            content = fetch_page(self.driver, meeting_url, self.cache)
            
            screenshot_data = {}
            if self.screenshots_enabled:
                date_prefix = meeting_date.split(' ')[0].replace('-', '')
                png_path, pdf_path = take_full_screenshot(self.driver, meeting_dir, self.config, prefix=f"meeting_{date_prefix}")
                if png_path and pdf_path:
                    screenshot_data = {'png': png_path, 'pdf': pdf_path}
                    logger.info(f"Screenshot saved: PNG={png_path}, PDF={pdf_path}")
            
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                logger.debug("Switching to content iframe")
                self.driver.switch_to.frame('content')
            
            details = {
                'url': meeting_url,
                'date': meeting_date,
                'title': self._get_element_text('.meeting-title'),
                'location': self._get_element_text('.meeting-location'),
                'status': self._get_element_text('.meeting-status'),
                'documents': [],
                'screenshots': screenshot_data
            }
            
            doc_elements = self.driver.find_elements(By.CSS_SELECTOR, '.document-link')
            for doc in doc_elements:
                doc_url = doc.get_attribute('href')
                doc_name = doc.text.strip()
                doc_path = self._download_document(doc_url, doc_name, meeting_dir)
                if doc_path:
                    details['documents'].append({'name': doc_name, 'path': doc_path})
            
            if iframe_exists:
                self.driver.switch_to.default_content()
            logger.debug(f"Scraped meeting details: {details}")
            return details
        except Exception as e:
            logger.error(f"Error scraping meeting {meeting_url}: {str(e)}", exc_info=True)
            debug_path = os.path.join(meeting_dir, 'debug', f"meeting_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            return None

    def _get_element_text(self, selector):
        try:
            element = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return element.text.strip()
        except Exception:
            return ''

    def _download_document(self, url, name, meeting_dir):
        try:
            cache_key = self.cache.get_cache_key(url)
            cache_path = self.cache.cache_path(cache_key, ext='pdf')
            if self.cache.is_cached(url):
                logger.info(f"Using cached document: {url}")
                return cache_path
            
            response = requests.get(url)
            response.raise_for_status()
            timestamp = int(datetime.now().timestamp())
            safe_name = name.replace(' ', '_').replace('/', '_')
            doc_path = os.path.join(meeting_dir, f"{safe_name}_{timestamp}.pdf")
            with open(doc_path, 'wb') as f:
                f.write(response.content)
            
            merged_path = doc_path.replace('.pdf', '_merged.pdf')
            if os.path.exists(doc_path):
                shutil.copy(doc_path, merged_path)
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
        csv_path = self.config['meetings_output_csv']
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'date', 'title', 'location', 'status', 'documents', 'screenshots'])
            writer.writeheader()
            for meeting in meetings:
                writer.writerow(meeting)
        logger.info(f"Saved meeting data to {csv_path}")

    def close(self):
        self.driver.quit()

def main():
    scraper = MeetingScraper()
    try:
        result = scraper.scrape_meetings()
        if result:
            logger.info("Meeting scraping completed successfully")
        else:
            logger.error("Meeting scraping failed")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()