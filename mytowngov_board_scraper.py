#!/usr/bin/env python3

import os
import logging
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
import time
import datetime
import sys
from mytowngov_common import load_config, setup_driver, fetch_page, capture_screenshot, Cache, has_iframe

# Configure logging
logger = logging.getLogger(__name__)

class BoardScraper:
    def __init__(self, config_path='config.yaml', headless=True, bypass_cache=True):
        self.config = load_config(config_path)
        self.base_url = self.config['homepage_url']
        self.data_dir = self.config['data_dir']
        self.use_cache = self.config.get('use_cache', True)
        self.cache = Cache(self.config)
        self.driver = setup_driver(headless=headless)
        self.wait = WebDriverWait(self.driver, 20)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.focus_mode = self.config.get('focus_mode_boards', False)
        self.focus_board = self.config.get('focus_board', None)
        self.bypass_cache = bypass_cache

    def _log_page_state(self, context="unknown"):
        try:
            logger.debug(f"Logging page state ({context})")
            logger.debug(f"Current URL: {self.driver.current_url}")
            logger.debug(f"Page title: {self.driver.title}")
            if has_iframe(self.driver, "content"):
                try:
                    self.driver.switch_to.frame("content")
                    iframe_content = self.driver.page_source[:1000]
                    logger.debug(f"Iframe content (first 1000 chars): {iframe_content}")
                except Exception as e:
                    logger.error(f"Error accessing content iframe: {e}")
                finally:
                    self.driver.switch_to.default_content()
            else:
                page_content = self.driver.page_source[:1000]
                logger.debug(f"Page content (first 1000 chars): {page_content}")
            logs = self.driver.get_log("browser")
            if logs:
                logger.debug(f"Browser console logs: {logs}")
        except Exception as e:
            logger.error(f"Error logging page state: {e}")

    def _scrape_board_details(self):
        details = {'name': '', 'chair': '', 'clerk': ''}
        try:
            self._log_page_state("before scraping board details")
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(2)

            selectors = [
                (By.TAG_NAME, 'h1'),
                (By.XPATH, "//h1"),
                (By.XPATH, "//*[contains(@class, 'board-title')]"),
                (By.XPATH, "//*[contains(text(), 'Planning Board')]"),
                (By.XPATH, "//*[contains(@class, 'board-name')]")
            ]
            name_element = None
            for by, value in selectors:
                try:
                    name_element = self.wait.until(EC.visibility_of_element_located((by, value)))
                    details['name'] = name_element.text.strip()
                    logger.debug(f"Found board name using {by}='{value}': {details['name']}")
                    break
                except TimeoutException:
                    logger.debug(f"Selector {by}='{value}' failed to find board name")

            if not details['name']:
                logger.error("Could not find board name with any selector")
                return details

            try:
                members_section = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Members:')]")
                members_text = members_section.text.strip()
                logger.debug(f"Found members section: {members_text}")

                members_lines = members_text.split('\n')
                for line in members_lines:
                    if "Chair" in line:
                        chair_name = line.split(', Chair')[0].strip()
                        details['chair'] = chair_name
                        logger.debug(f"Found chair: {details['chair']}")
                    if "Clerk" in line:
                        clerk_name = line.split(', Clerk')[0].strip()
                        details['clerk'] = clerk_name
                        logger.debug(f"Found clerk: {details['clerk']}")
                    if not details['clerk']:
                        try:
                            clerk_element = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Clerk:')]")
                            clerk_text = clerk_element.text.strip()
                            details['clerk'] = clerk_text.replace('Clerk:', '').strip()
                            logger.debug(f"Found clerk from Clerk field: {details['clerk']}")
                        except NoSuchElementException:
                            logger.debug("Clerk field not found")

            except NoSuchElementException:
                logger.warning("Members section not found, skipping chair and clerk extraction")

        except TimeoutException as e:
            logger.error(f"Error scraping board details: {e}")
            self._log_page_state("after board details failure")
        except Exception as e:
            logger.error(f"Unexpected error scraping board details: {e}")
            self._log_page_state("after unexpected board details failure")
        return details

    def _parse_date(self, date_str):
        try:
            date_str = date_str.replace('\n', ' ').strip()
            formats = [
                "%b %d, %Y %I:%M %p %Z",
                "%b %d, %Y %I:%M %p",
                "%B %d, %Y %I:%M %p",
                "%Y-%m-%d %H:%M:%S",
                "%b %d, %Y"
            ]
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            logger.error(f"Could not parse date '{date_str}' with any format")
            return None
        except Exception as e:
            logger.error(f"Error parsing date '{date_str}': {e}")
            return None

    def _scrape_meetings(self, board_name):
        meetings = []
        seen_meetings = set()
        try:
            self._log_page_state("before scraping meetings")

            # Ensure we're in the content iframe
            iframe_exists = has_iframe(self.driver, "content")
            if iframe_exists:
                try:
                    self.driver.switch_to.frame("content")
                    logger.debug("Switched to content iframe for meetings")
                except Exception as e:
                    logger.error(f"Failed to switch to content iframe for meetings: {e}")
                    iframe_exists = False
            else:
                logger.warning("No content iframe found for meetings")

            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(6)  # Increased sleep for dynamic content

            logger.debug("Scrolling to bottom of page to load all content")
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            for _ in range(5):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)  # Increased sleep
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            logger.debug(f"Final scroll height: {last_height}")

            debug_path = os.path.join(self.data_dir, board_name, "debug", f"meetings_page_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved meetings page HTML: {debug_path}")

            page_source_truncated = self.driver.page_source[:2000]
            logger.debug(f"Page source (first 2000 chars): {page_source_truncated}")

            selectors = [
                (By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'past meetings')]"),
                (By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'upcoming meetings')]"),
                (By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'planning board upcoming meetings')]"),
                (By.XPATH, "//h4[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'meetings')]"),
                (By.XPATH, "//*[contains(text(), 'Meetings')]"),
                (By.ID, "Upcoming"),
                (By.ID, "Past")
            ]
            headings = []
            for by, value in selectors:
                try:
                    elements = self.driver.find_elements(by, value)
                    logger.debug(f"Found {len(elements)} elements using {by}='{value}'")
                    headings.extend(elements)
                except Exception as e:
                    logger.debug(f"Selector {by}='{value}' failed: {e}")

            if not headings:
                logger.warning("No meetings sections found with any selector")
                self._log_page_state("after failing to find Meetings section")
                return meetings

            for heading in headings:
                heading_text = heading.text.strip()
                logger.info(f"Processing meetings section: {heading_text}")

                try:
                    table = heading.find_element(By.XPATH, "following::table[1]")
                    logger.debug(f"Found table for section '{heading_text}'")
                    
                    table_html = table.get_attribute('outerHTML')
                    logger.debug(f"Table HTML: {table_html}")

                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]
                    logger.debug(f"Found {len(rows)} meeting rows in section '{heading_text}'")

                    for idx, row in enumerate(rows):
                        try:
                            row_html = row.get_attribute('outerHTML')
                            logger.debug(f"Row {idx} HTML: {row_html}")

                            cells = row.find_elements(By.TAG_NAME, "td")
                            logger.debug(f"Row {idx} has {len(cells)} cells")

                            if len(cells) >= 5:  # Past Meetings (Date, Location, Minutes, Other Docs, Details)
                                date_str = cells[0].text.strip()
                                location = cells[1].text.strip()
                                minutes = cells[2].text.strip()
                                other_docs = cells[3].text.strip()
                                details_cell = cells[4]
                            elif len(cells) >= 4:  # Upcoming Meetings (Board Name, Time, Location, Details)
                                row_board_name = cells[0].text.strip()
                                date_str = cells[1].text.strip()
                                location = cells[2].text.strip()
                                minutes = ""
                                other_docs = ""
                                details_cell = cells[3]
                            else:
                                logger.warning(f"Row {idx} has insufficient cells ({len(cells)}): {row_html}")
                                continue

                            try:
                                details_link = details_cell.find_element(By.XPATH, ".//a[contains(text(), 'Details and Agenda')]")
                                details_url = details_link.get_attribute("href")
                            except NoSuchElementException:
                                details_url = ""
                                logger.debug(f"Row {idx}: No 'Details and Agenda' link found")

                            date = self._parse_date(date_str)
                            if not date:
                                logger.warning(f"Row {idx}: Failed to parse date '{date_str}'")
                                continue

                            logger.debug(f"Row {idx}: Parsed date '{date_str}' as {date}")

                            date_only = date.date()
                            target_date = datetime.datetime.strptime("Sep 10, 2024", "%b %d, %Y").date()
                            if date_only == target_date:
                                logger.info(f"Found meeting on Sep 10, 2024: {date_str}, {location}, {details_url}")

                            status = "Cancelled" if "Cancelled" in location else "Scheduled"
                            meeting_key = (board_name, date_str, details_url)
                            if meeting_key not in seen_meetings:
                                seen_meetings.add(meeting_key)
                                meeting_data = {
                                    "board_name": board_name,
                                    "date": date,
                                    "location": location,
                                    "status": status,
                                    "details_url": details_url,
                                    "minutes": minutes,
                                    "other_docs": other_docs
                                }
                                if len(cells) >= 4 and len(cells) < 5:
                                    meeting_data["row_board_name"] = row_board_name
                                meetings.append(meeting_data)
                                logger.info(f"Scraped meeting: {board_name}, {date_str}, {location}, {status}, {details_url}")
                        except Exception as e:
                            logger.error(f"Error processing row {idx} in section '{heading_text}': {e}")
                            continue

                except NoSuchElementException:
                    logger.debug(f"No table found for section '{heading_text}'")
                    continue
                except Exception as e:
                    logger.error(f"Error parsing section '{heading_text}': {e}")
                    continue

        except Exception as e:
            logger.error(f"Error scraping meetings: {e}")
            debug_path = os.path.join(self.data_dir, board_name, "debug", f"meetings_error_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML for meetings: {debug_path}")
            self._log_page_state("after meetings scrape failure")
        finally:
            if iframe_exists:
                try:
                    self.driver.switch_to.default_content()
                    logger.debug("Switched back to default content after meetings")
                except Exception as e:
                    logger.error(f"Error switching back to default content after meetings: {e}")

        logger.debug(f"Total meetings scraped: {len(meetings)}")
        return meetings

    def scrape_board(self, board_name, board_url):
        logger.info(f"Scraping board: {board_name}")
        board_dir = os.path.join(self.data_dir, board_name)
        os.makedirs(board_dir, exist_ok=True)

        content = fetch_page(self.driver, board_url, self.cache, bypass_cache=self.bypass_cache)

        current_url = self.driver.current_url
        if 'data:,' in current_url:
            logger.error(f"Page failed to load properly for {board_url}, current URL: {current_url}")
            raise Exception("Page load failed, invalid URL detected")

        iframe_exists = has_iframe(self.driver, "content")
        if iframe_exists:
            try:
                self.driver.switch_to.frame("content")
                logger.debug("Switched to content iframe")
            except Exception as e:
                logger.error(f"Failed to switch to content iframe: {e}")
                iframe_exists = False
        else:
            logger.warning("No content iframe found")

        self._log_page_state("after loading board page")

        details = self._scrape_board_details()
        if not details['name']:
            logger.error("Failed to scrape board details, aborting further scraping")
            raise Exception("Board name not found, cannot proceed with scraping")

        logger.info(f"Board details: name={details['name']}, chair={details['chair']}, clerk={details['clerk']}")

        debug_path = os.path.join(board_dir, "debug", f"board_{board_name}_{int(time.time())}.html")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(self.driver.page_source)
        logger.info(f"Saved board debug HTML: {debug_path}")

        if self.screenshots_enabled:
            wait_selector = (By.TAG_NAME, "h1")  # More reliable selector
            png_path, pdf_path = capture_screenshot(
                self.driver, board_dir, self.config, prefix="board", board_name=board_name, wait_selector=wait_selector
            )
            if png_path and pdf_path:
                logger.info(f"Board screenshot saved: PNG={png_path}, PDF={pdf_path}")
            else:
                logger.error("Failed to save board screenshot")

        meetings = self._scrape_meetings(board_name)
        logger.info(f"Scraped {len(meetings)} meetings for board {board_name}")

        if meetings:
            df_meetings = pd.DataFrame(meetings)
            meeting_csv = os.path.join(board_dir, "board_meeting_data.csv")
            os.makedirs(os.path.dirname(meeting_csv), exist_ok=True)
            df_meetings.to_csv(meeting_csv, index=False)
            logger.info(f"Saved meeting CSV for {board_name}: {meeting_csv}")
        else:
            logger.warning(f"No meetings to save for {board_name}")

        if iframe_exists:
            try:
                self.driver.switch_to.default_content()
                logger.debug("Switched back to default content")
            except Exception as e:
                logger.error(f"Error switching back to default content: {e}")

    def scrape(self):
        boards_csv = self.config['homepage_boards_csv']
        if not os.path.exists(boards_csv):
            error_msg = f"Source CSV file not found: {boards_csv}. Please run the homepage scraper (mytowngov_homepage_scraper.py) first to generate the required board data."
            logger.error(error_msg)
            print(error_msg)
            sys.exit(1)

        df = pd.read_csv(boards_csv)
        for _, row in df.iterrows():
            board_name = row['Name']
            board_url = row['URL']

            if not isinstance(board_name, str) or pd.isna(board_name):
                logger.warning(f"Skipping invalid board name: {board_name}")
                continue

            if self.focus_mode and board_name != self.focus_board:
                logger.debug(f"Skipping board {board_name} (focus mode enabled for {self.focus_board})")
                continue

            self.scrape_board(board_name, board_url)

        logger.info("Board scraping completed successfully")

    def close(self):
        try:
            self.driver.quit()
        except Exception as e:
            logger.error(f"Error closing driver: {e}")

def main():
    scraper = BoardScraper(headless=True, bypass_cache=True)
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()