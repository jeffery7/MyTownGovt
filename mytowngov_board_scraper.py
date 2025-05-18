#!/usr/bin/env python3

import os
import logging
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import datetime
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Configure logging
logger = logging.getLogger(__name__)

class BoardScraper:
    def __init__(self, config_path='config.yaml', headless=True, bypass_cache=False):
        self.config = load_config(config_path)
        self.base_url = self.config['homepage_url']
        self.data_dir = self.config['data_dir']
        self.cache = Cache(self.config)
        self.driver = setup_driver(headless=headless)
        self.wait = WebDriverWait(self.driver, 15)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.focus_mode = self.config.get('focus_mode_boards', False)
        self.focus_board = self.config.get('focus_board', None)
        self.bypass_cache = bypass_cache

    def _log_page_state(self, context="unknown"):
        """Log detailed page state for debugging."""
        try:
            logger.debug(f"Logging page state ({context})")
            logger.debug(f"Current URL: {self.driver.current_url}")
            logger.debug(f"Page title: {self.driver.title}")
            if has_iframe(self.driver, "content"):
                self.driver.switch_to.frame("content")
                iframe_content = self.driver.page_source[:1000]
                logger.debug(f"Iframe content (first 1000 chars): {iframe_content}")
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
                (By.XPATH, "//*[contains(text(), 'Planning Board')]")
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
                "%B %d, %Y"
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
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(3)

            # Aggressive scrolling to load all content
            logger.debug("Scrolling to load all meetings")
            max_scroll_attempts = 5
            for attempt in range(max_scroll_attempts):
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                logger.debug(f"Scroll attempt {attempt + 1}/{max_scroll_attempts}: height {last_height} -> {new_height}")
                if new_height == last_height:
                    break

            # Save pre-parse HTML for debugging
            debug_path = os.path.join(self.data_dir, board_name, "debug", f"meetings_page_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved meetings page HTML: {debug_path}")

            # Ensure iframe context
            iframe_exists = has_iframe(self.driver, "content")
            if iframe_exists:
                self.driver.switch_to.frame("content")
                logger.debug("Switched to content iframe for meetings")
            else:
                logger.warning("No content iframe found for meetings")

            # Log all h2 elements for debugging
            try:
                h2_elements = self.driver.find_elements(By.TAG_NAME, "h2")
                logger.debug(f"Found {len(h2_elements)} h2 elements")
                for idx, h2 in enumerate(h2_elements):
                    logger.debug(f"h2[{idx}]: {h2.text.strip()}")
            except Exception as e:
                logger.debug(f"Error logging h2 elements: {e}")

            # Target the Past Meetings table
            table_selectors = [
                (By.XPATH, "//table[preceding-sibling::h2[contains(text(), 'Past Meetings')]]"),
                (By.XPATH, "//table[contains(., 'Minutes Available')]"),
                (By.XPATH, "//table[preceding-sibling::h2[contains(text(), 'Past Meetings')]]/following::table[1]")
            ]
            past_meetings_table = None
            for by, value in table_selectors:
                try:
                    past_meetings_table = self.wait.until(EC.presence_of_element_located((by, value)))
                    logger.debug(f"Found Past Meetings table using {by}='{value}'")
                    break
                except TimeoutException:
                    logger.debug(f"Selector {by}='{value}' failed to find Past Meetings table")

            if past_meetings_table:
                rows = past_meetings_table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
                logger.debug(f"Found {len(rows)} rows in Past Meetings table")

                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:  # Time, Location, Minutes, Other Documents, Details
                        date_str = cells[0].text.strip()
                        location = cells[1].text.strip()
                        status = "Cancelled" if "Cancelled" in location else "Scheduled"
                        details_url = cells[4].find_element(By.TAG_NAME, "a").get_attribute("href")

                        logger.debug(f"Processing row: date='{date_str}', location='{location}', status='{status}', details_url='{details_url}'")

                        date = self._parse_date(date_str)
                        if date:
                            meeting_key = (board_name, date_str, details_url)
                            if meeting_key not in seen_meetings:
                                seen_meetings.add(meeting_key)
                                meetings.append({
                                    "board_name": board_name,
                                    "date": date,
                                    "location": location,
                                    "status": status,
                                    "details_url": details_url
                                })
                                logger.debug(f"Scraped past meeting: {board_name}, {date_str}, {location}, {status}, {details_url}")
                        else:
                            logger.warning(f"Failed to parse date: {date_str}")
                    else:
                        logger.warning(f"Skipping row with {len(cells)} cells: {[cell.text for cell in cells]}")
            else:
                logger.warning("Past Meetings table not found")
                self._log_page_state("after failing to find past meetings table")

            # Save HTML for upcoming meetings debugging
            debug_upcoming_path = os.path.join(self.data_dir, board_name, "debug", f"upcoming_meetings_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_upcoming_path), exist_ok=True)
            with open(debug_upcoming_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved upcoming meetings HTML: {debug_upcoming_path}")

            # Target the Upcoming Meetings section (non-table)
            upcoming_selectors = [
                (By.XPATH, "//h2[contains(., 'Upcoming')]"),
                (By.XPATH, "//h2[contains(text(), 'Upcoming Meetings')]"),
                (By.XPATH, "//div[contains(@class, 'upcoming')]"),
                (By.XPATH, "//*[contains(text(), 'Upcoming Meetings')]")
            ]
            upcoming_section = None
            for by, value in upcoming_selectors:
                try:
                    upcoming_section = self.wait.until(EC.presence_of_element_located((by, value)))
                    logger.debug(f"Found Upcoming Meetings section using {by}='{value}'")
                    break
                except TimeoutException:
                    logger.debug(f"Selector {by}='{value}' failed to find Upcoming Meetings section")

            if upcoming_section:
                parent = upcoming_section.find_element(By.XPATH, "..")
                elements = parent.find_elements(By.XPATH, ".//*[self::p or self::div or self::span or self::a]")
                current_meeting = {'board_name': board_name}
                logger.debug(f"Processing {len(elements)} non-table elements in Upcoming Meetings")

                for idx, elem in enumerate(elements):
                    elem_text = elem.text.strip()
                    if not elem_text:
                        continue

                    logger.debug(f"Element {idx}: tag={elem.tag_name}, text='{elem_text}', href={elem.get_attribute('href') or 'None'}")

                    if any(keyword in elem_text for keyword in [
                        "Past Meetings", "Regular Meetings", "Subcommittees"
                    ]):
                        if current_meeting.get('date') and current_meeting.get('details_url'):
                            date = self._parse_date(current_meeting['date'])
                            if date:
                                status = "Cancelled" if "Cancelled" in current_meeting.get('location', '') else "Scheduled"
                                meeting_key = (current_meeting['board_name'], current_meeting['date'], current_meeting['details_url'])
                                if meeting_key not in seen_meetings:
                                    seen_meetings.add(meeting_key)
                                    meetings.append({
                                        "board_name": current_meeting['board_name'],
                                        "date": date,
                                        "location": current_meeting.get('location', ''),
                                        "status": status,
                                        "details_url": current_meeting['details_url']
                                    })
                                    logger.debug(f"Scraped upcoming meeting: {current_meeting}")
                        else:
                            logger.debug(f"Discarded incomplete meeting: {current_meeting}")
                        current_meeting = {'board_name': board_name}
                        continue

                    if "Details and Agenda" in elem_text:
                        continue
                    elif any(month in elem_text for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]):
                        current_meeting['date'] = elem_text
                    elif any(loc in elem_text for loc in ["Myron E. Richardson", "Town House", "Cancelled", "Assessor's", "Conference room"]):
                        current_meeting['location'] = elem_text
                    elif elem.tag_name == "a" and elem.get_attribute("href"):
                        href = elem.get_attribute("href")
                        if "meeting" in href.lower():
                            current_meeting['details_url'] = href

                # Finalize last meeting
                if current_meeting.get('date') and current_meeting.get('details_url'):
                    date = self._parse_date(current_meeting['date'])
                    if date:
                        status = "Cancelled" if "Cancelled" in current_meeting.get('location', '') else "Scheduled"
                        meeting_key = (current_meeting['board_name'], current_meeting['date'], current_meeting['details_url'])
                        if meeting_key not in seen_meetings:
                            seen_meetings.add(meeting_key)
                            meetings.append({
                                "board_name": current_meeting['board_name'],
                                "date": date,
                                "location": current_meeting.get('location', ''),
                                "status": status,
                                "details_url": current_meeting['details_url']
                            })
                            logger.debug(f"Scraped final upcoming meeting: {current_meeting}")
            else:
                logger.warning("Upcoming Meetings section not found")
                self._log_page_state("after failing to find upcoming meetings section")

        except Exception as e:
            logger.error(f"Error scraping meetings: {e}")
            debug_path = os.path.join(self.data_dir, board_name, "debug", f"meetings_error_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML for meetings error: {debug_path}")
            self._log_page_state("after meetings scrape failure")

        finally:
            if iframe_exists:
                self.driver.switch_to.default_content()
                logger.debug("Switched back to default content after meetings")

        logger.info(f"Total meetings scraped for {board_name}: {len(meetings)}")
        return meetings

    def scrape_board(self, board_name, board_url):
        logger.info(f"Scraping board: {board_name} at {board_url}")
        board_dir = os.path.join(self.data_dir, board_name)
        os.makedirs(board_dir, exist_ok=True)

        content = fetch_page(self.driver, board_url, self.cache, bypass_cache=self.bypass_cache)

        iframe_exists = has_iframe(self.driver, "content")
        if iframe_exists:
            self.driver.switch_to.frame("content")
            logger.debug("Switched to content iframe")
        else:
            logger.warning("No content iframe found")

        self._log_page_state("after loading board page")

        details = self._scrape_board_details()
        logger.info(f"Board details: name={details['name']}, chair={details['chair']}, clerk={details['clerk']}")

        debug_path = os.path.join(board_dir, "debug", f"board_{board_name}_{int(time.time())}.html")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(self.driver.page_source)
        logger.info(f"Saved board debug HTML: {debug_path}")

        if self.screenshots_enabled:
            try:
                prefix = f"boardscraper_{board_name.replace(' ', '_')}"
                png_path, pdf_path = take_full_screenshot(self.driver, board_dir, self.config, prefix=prefix)
                if png_path and pdf_path:
                    logger.info(f"Board screenshot saved: PNG={png_path}, PDF={pdf_path}")
                else:
                    logger.error("Failed to save board screenshot")
            except Exception as e:
                logger.error(f"Error taking board screenshot: {e}")

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
            self.driver.switch_to.default_content()
            logger.debug("Switched back to default content")

    def scrape(self):
        boards_csv = self.config['homepage_boards_csv']
        if not os.path.exists(boards_csv):
            logger.error(f"Board CSV not found: {boards_csv}")
            return

        df = pd.read_csv(boards_csv)
        for _, row in df.iterrows():
            board_name = row['Name']
            board_url = row['URL']

            if not isinstance(board_name, str) or pd.isna(board_name):
                logger.warning(f"Skipping invalid board name: {board_name}")
                continue

            if self.focus_mode and board_name != self.focus_board:
                logger.info(f"Skipping board {board_name} (focus mode enabled for {self.focus_board})")
                continue

            self.scrape_board(board_name, board_url)

        logger.info("Board scraping completed successfully")

    def close(self):
        self.driver.quit()

def main():
    scraper = BoardScraper(headless=False, bypass_cache=True)
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()