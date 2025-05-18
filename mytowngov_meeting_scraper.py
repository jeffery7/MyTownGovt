#!/usr/bin/env python3

import os
import logging
import pandas as pd
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import datetime
import sys
from urllib.parse import urljoin
from mytowngov_common import load_config, setup_driver, fetch_page, capture_screenshot, Cache, has_iframe

# Configure logging
logger = logging.getLogger(__name__)

class MeetingScraper:
    def __init__(self, config_path='config.yaml', headless=True, bypass_cache=True):
        self.config = load_config(config_path)
        self.base_url = self.config['base_url']
        self.data_dir = self.config['data_dir']
        self.boards_data_dir = self.config['boards_data_dir']
        self.use_cache = self.config.get('use_cache', True)
        self.cache = Cache(self.config)
        self.driver = setup_driver(headless=headless)
        self.wait = WebDriverWait(self.driver, 15)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.focus_mode = self.config.get('focus_mode_meetings', False)
        self.focus_date = self.config.get('focus_date', None)
        self.focus_board = self.config.get('focus_board', None)
        self.bypass_cache = bypass_cache

    def _log_page_state(self, context="unknown"):
        try:
            logger.debug(f"Logging page state ({context})")
            logger.debug(f"Current URL: {self.driver.current_url}")
            logger.debug(f"Page title: {self.driver.title}")
            page_content = self.driver.page_source[:1000]
            logger.debug(f"Page content (first 1000 chars): {page_content}")
        except Exception as e:
            logger.error(f"Error logging page state: {e}")

    def _download_attachment(self, url, meeting_dir, filename=None):
        try:
            if filename:
                safe_filename = filename.replace('?', '_').replace('&', '_').replace('/', '_')
            else:
                safe_filename = url.split('/')[-1].replace('?', '_').replace('&', '_')
            attachment_path = os.path.join(meeting_dir, safe_filename)
            os.makedirs(os.path.dirname(attachment_path), exist_ok=True)
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(attachment_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded attachment: {attachment_path}")
            return attachment_path
        except Exception as e:
            logger.error(f"Error downloading attachment {url}: {e}")
            return None

    def _scrape_meeting_details(self, board_name, meeting_date, details_url):
        details = {
            'board_name': board_name,
            'meeting_date': meeting_date,
            'details_url': details_url,
            'agenda': '',
            'minutes': '',
            'location': '',
            'documents': []
        }
        safe_board_name = board_name.replace(" ", "_").replace("/", "_")
        meeting_dir = os.path.join(self.data_dir, board_name, f"meeting_{safe_board_name}_{meeting_date}")
        os.makedirs(meeting_dir, exist_ok=True)

        try:
            content = fetch_page(self.driver, details_url, self.cache, bypass_cache=self.bypass_cache)

            current_url = self.driver.current_url
            if 'data:,' in current_url:
                logger.error(f"Page failed to load properly for {details_url}, current URL: {current_url}")
                return details, meeting_dir

            iframe_exists = has_iframe(self.driver, "content")
            if iframe_exists:
                try:
                    self.driver.switch_to.frame("content")
                    logger.debug("Switched to content iframe for meeting details")
                except Exception as e:
                    logger.error(f"Failed to switch to content iframe: {e}")
                    iframe_exists = False
            else:
                logger.warning("No content iframe found")

            self._log_page_state("after loading meeting page")

            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(2)

            logger.debug("Scrolling to load content")
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            logger.debug(f"Final scroll height: {last_height}")

            debug_path = os.path.join(meeting_dir, f"meeting_{safe_board_name}_{meeting_date}_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved meeting debug HTML: {debug_path}")

            try:
                title_element = self.driver.find_element(By.TAG_NAME, 'h1')
                details['title'] = title_element.text.strip()
                logger.debug(f"Found meeting title: {details['title']}")
            except NoSuchElementException:
                logger.warning("Meeting title not found")

            try:
                location_element = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Location:')]")
                details['location'] = location_element.text.replace('Location:', '').strip()
                logger.debug(f"Found location: {details['location']}")
            except NoSuchElementException:
                logger.debug("Location not found")

            try:
                agenda_label = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Agenda')]")
                agenda_text = None
                for sibling in agenda_label.find_elements(By.XPATH, "following-sibling::*"):
                    if sibling.tag_name in ['p', 'div', 'textarea']:
                        agenda_text = sibling.text.strip()
                        if agenda_text:
                            break
                if not agenda_text:
                    agenda_text = self.driver.find_element(By.XPATH, "//textarea[contains(@name, 'agenda')] | //div[contains(@class, 'agenda')]").text.strip()
                details['agenda'] = agenda_text
                logger.debug(f"Found agenda text: {details['agenda'][:100]}...")
            except NoSuchElementException:
                logger.debug("Agenda text not found")

            try:
                minutes_element = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Minutes')]")
                details['minutes'] = minutes_element.get_attribute('href')
                logger.debug(f"Found minutes: {details['minutes']}")
                if details['minutes']:
                    attachment_path = self._download_attachment(details['minutes'], meeting_dir)
                    if attachment_path:
                        details['documents'].append(attachment_path)
            except NoSuchElementException:
                logger.debug("Minutes link not found")

            try:
                viewer_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'viewer')]")
                for viewer_link in viewer_links:
                    try:
                        file_name = viewer_link.text.strip()
                        if not file_name:
                            logger.debug("Viewer link has no text, skipping")
                            continue
                        logger.debug(f"Found viewer link with file name: {file_name}")

                        download_link = viewer_link.find_element(By.XPATH, "following-sibling::a[contains(@href, 'download')] | preceding-sibling::a[contains(@href, 'download')]")
                        download_url = download_link.get_attribute('href')
                        logger.debug(f"Found download link: {download_url}")

                        attachment_path = self._download_attachment(download_url, meeting_dir, filename=file_name)
                        if attachment_path:
                            details['documents'].append(attachment_path)
                    except NoSuchElementException:
                        logger.debug(f"No download link found for viewer link: {viewer_link.get_attribute('href')}")
                        continue
            except Exception as e:
                logger.error(f"Error scraping documents: {e}")

            if self.screenshots_enabled:
                wait_selector = (By.TAG_NAME, "h1")
                png_path, pdf_path = capture_screenshot(
                    self.driver, meeting_dir, self.config, prefix=f"meeting_{safe_board_name}_{meeting_date}",
                    board_name=board_name, date_str=meeting_date, wait_selector=wait_selector
                )
                if png_path and pdf_path:
                    logger.info(f"Meeting screenshot saved: PNG={png_path}, PDF={pdf_path}")
                    details['screenshot_png'] = png_path
                    details['screenshot_pdf'] = pdf_path
                else:
                    logger.error("Failed to save meeting screenshot")

        except Exception as e:
            logger.error(f"Error scraping meeting details for {details_url}: {e}")
            debug_path = os.path.join(meeting_dir, f"meeting_error_{safe_board_name}_{meeting_date}_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML for meeting error: {debug_path}")
            self._log_page_state("after meeting details failure")
        finally:
            if iframe_exists:
                try:
                    self.driver.switch_to.default_content()
                    logger.debug("Switched back to default content after meeting details")
                except Exception as e:
                    logger.error(f"Error switching back to default content: {e}")

        return details, meeting_dir

    def scrape(self):
        if self.focus_mode and self.focus_board:
            board_dir = os.path.join(self.boards_data_dir, self.focus_board)
            meetings_csv = os.path.join(board_dir, "board_meeting_data.csv")
            if not os.path.exists(meetings_csv):
                error_msg = f"Source CSV file not found: {meetings_csv}. Please run the board scraper (mytowngov_board_scraper.py) first to generate the required meeting data."
                logger.error(error_msg)
                print(error_msg)
                sys.exit(1)
            boards_to_process = [(self.focus_board, meetings_csv)]
        else:
            error_msg = "Focus mode must be enabled with a specific board for this scraper."
            logger.error(error_msg)
            print(error_msg)
            sys.exit(1)

        meeting_details = []

        for board_name, meetings_csv in boards_to_process:
            logger.info(f"Processing meetings for board: {board_name}")
            df = pd.read_csv(meetings_csv)
            for _, row in df.iterrows():
                board_name = row['board_name']
                meeting_date = row['date']
                details_url = row['details_url']

                if not isinstance(board_name, str) or pd.isna(board_name) or not isinstance(details_url, str) or pd.isna(details_url):
                    logger.warning(f"Skipping invalid meeting: board_name={board_name}, details_url={details_url}")
                    continue

                try:
                    date_obj = datetime.datetime.strptime(meeting_date.split()[0], "%Y-%m-%d").date()
                    date_str = date_obj.strftime("%Y-%m-%d")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse meeting date '{meeting_date}': {e}")
                    continue

                if self.focus_mode and self.focus_date:
                    if date_str != self.focus_date or board_name != self.focus_board:
                        logger.debug(f"Skipping meeting for {board_name} on {date_str} (focus mode enabled for {self.focus_board} on {self.focus_date})")
                        continue

                logger.info(f"Scraping meeting for {board_name} on {date_str}")
                details, meeting_dir = self._scrape_meeting_details(board_name, date_str, details_url)
                meeting_details.append(details)

                if meeting_details:
                    df_details = pd.DataFrame([details])
                    safe_board_name = board_name.replace(" ", "_").replace("/", "_")
                    output_csv = os.path.join(meeting_dir, f"meeting_{safe_board_name}_{date_str}.csv")
                    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                    df_details.to_csv(output_csv, index=False)
                    logger.info(f"Saved meeting details to {output_csv}")
                else:
                    logger.warning(f"No meeting details to save for {board_name} on {date_str}")

        logger.info("Meeting scraping completed successfully")

    def close(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f"Error closing driver: {e}")

def main():
    scraper = MeetingScraper(headless=True, bypass_cache=True)
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()