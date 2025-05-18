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
        logger.debug(f"Loaded configuration: {self.config}")
        self.data_dir = self.config['data_dir']
        self.use_cache = self.config.get('use_cache', True)  # Respect use_cache setting
        self.cache = Cache(self.config)
        self.driver = setup_driver()
        self.wait = WebDriverWait(self.driver, 15)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.output_csv = self.config['meetings_output_csv']
        self.focus_mode = self.config.get('focus_mode_meetings', False)
        self.focus_date = self.config.get('focus_date')
        self.focus_board = self.config.get('focus_board', None)

    def scrape_meetings(self):
        logger.info("Starting meeting scraping")
        meeting_data = []

        # If in focus mode, only process the specified board
        boards = [self.focus_board] if self.focus_mode and self.focus_board else self._get_all_boards()

        for board_name in boards:
            input_csv = os.path.join(self.data_dir, board_name, 'board_meeting_data.csv')
            logger.debug(f"Checking for meeting CSV: {input_csv}")

            if not os.path.exists(input_csv):
                logger.error(f"Meeting CSV not found for board {board_name}: {input_csv}")
                continue

            try:
                meetings_df = pd.read_csv(input_csv)
                logger.debug(f"Loaded CSV for {board_name}:\n{meetings_df.to_string()}")
            except Exception as e:
                logger.error(f"Error reading CSV {input_csv}: {e}")
                continue

            expected_columns = ['board_name', 'date', 'details_url']
            if not all(col in meetings_df.columns for col in expected_columns):
                logger.error(f"CSV {input_csv} missing required columns: {expected_columns}, found: {list(meetings_df.columns)}")
                continue

            meetings_to_scrape = meetings_df
            if self.focus_mode and self.focus_date:
                meetings_to_scrape = meetings_df[meetings_df['date'].str.contains(self.focus_date, na=False)]
                logger.debug(f"Focus mode enabled, filtered to {len(meetings_to_scrape)} meetings for date: {self.focus_date}")

            for meeting in meetings_to_scrape.to_dict('records'):
                board_name = meeting['board_name']
                meeting_url = meeting['details_url']
                meeting_date = meeting['date']
                logger.info(f"Scraping meeting for {board_name}: {meeting_url}")
                result = self.scrape_meeting(board_name, meeting_url, meeting_date)
                if result:
                    meeting_data.append(result)

        if meeting_data:
            self.save_meeting_data(meeting_data)
            logger.info(f"Scraped {len(meeting_data)} meetings successfully")
        else:
            logger.warning("No meeting data scraped")

        return meeting_data

    def _get_all_boards(self):
        """Get list of all board directories in data_dir."""
        try:
            board_dirs = [d for d in os.listdir(self.data_dir) if os.path.isdir(os.path.join(self.data_dir, d))]
            logger.debug(f"Found board directories: {board_dirs}")
            return board_dirs
        except Exception as e:
            logger.error(f"Error listing board directories in {self.data_dir}: {e}")
            return []

    def scrape_meeting(self, board_name, meeting_url, meeting_date):
        logger.info(f"Scraping meeting: {meeting_url}")
        try:
            # Create meeting-specific directory
            board_dir = os.path.join(self.data_dir, board_name)
            date_prefix = meeting_date.split(' ')[0].replace('-', '')
            meeting_dir = os.path.join(board_dir, date_prefix)
            os.makedirs(meeting_dir, exist_ok=True)

            content = fetch_page(self.driver, meeting_url, self.cache, bypass_cache=not self.use_cache)
            
            screenshot_data = {}
            if self.screenshots_enabled:
                # Use updated take_full_screenshot with board_name and date
                prefix = "meeting"
                png_path, pdf_path = take_full_screenshot(
                    self.driver, meeting_dir, self.config, 
                    prefix=prefix, board_name=board_name, date_str=meeting_date
                )
                if png_path and pdf_path:
                    screenshot_data = {'png': png_path, 'pdf': pdf_path}
                    logger.info(f"Screenshot saved: PNG={png_path}, PDF={pdf_path}")
                else:
                    logger.error(f"Failed to save screenshot for {prefix}_{board_name}_{meeting_date}")
            
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
        except Exception as e:
            logger.debug(f"Failed to get text for selector {selector}: {e}")
            return ''

    def _download_document(self, url, name, meeting_dir):
        try:
            cache_key = self.cache.get_cache_key(url)
            cache_path = self.cache.cache_path(cache_key, ext='pdf')
            if self.use_cache and self.cache.is_cached(url):
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
        csv_path = self.output_csv
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
    except Exception as e:
        logger.error(f"Meeting scraping failed: {e}", exc_info=True)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()