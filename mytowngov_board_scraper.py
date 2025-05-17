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
            # Log iframe content if present
            if has_iframe(self.driver, "content"):
                self.driver.switch_to.frame("content")
                iframe_content = self.driver.page_source[:1000]  # Limit to avoid huge logs
                logger.debug(f"Iframe content (first 1000 chars): {iframe_content}")
                self.driver.switch_to.default_content()
            else:
                page_content = self.driver.page_source[:1000]
                logger.debug(f"Page content (first 1000 chars): {page_content}")
            # Check for JavaScript errors
            logs = self.driver.get_log("browser")
            if logs:
                logger.debug(f"Browser console logs: {logs}")
        except Exception as e:
            logger.error(f"Error logging page state: {e}")

    def _scrape_board_details(self):
        details = {'name': '', 'chair': '', 'clerk': ''}
        try:
            # Log page state before scraping
            self._log_page_state("before scraping board details")

            # Wait for the page to be fully loaded by checking for a key element
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(2)  # Additional wait for JavaScript rendering

            # Try multiple selectors for the board name
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

            # Extract chair and clerk from the members section
            try:
                # Look for the "Members" section directly in the text
                members_section = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Members:')]")
                members_text = members_section.text.strip()
                logger.debug(f"Found members section: {members_text}")

                # Parse chair and clerk from the members list
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
                    # If clerk is not in members, look for it in the "Clerk" field
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
            # Handle multi-line date strings (e.g., "May 22, 2025\n6:30 PM EDT")
            date_str = date_str.replace('\n', ' ')
            return datetime.datetime.strptime(date_str, "%b %d, %Y %I:%M %p %Z")
        except ValueError as e:
            logger.error(f"Error parsing date '{date_str}': {e}")
            return None

    def _scrape_meetings(self, board_name):
        meetings = []
        try:
            # Log page state before scraping meetings
            self._log_page_state("before scraping meetings")

            # Try multiple selectors for the Meetings sections
            selectors = [
                (By.XPATH, "//*[contains(text(), 'Regular Meetings')]"),
                (By.XPATH, "//*[contains(text(), 'Upcoming Meetings')]"),
                (By.XPATH, "//*[contains(text(), 'Past Meetings')]"),
                (By.XPATH, "//*[contains(text(), 'Planning Board Upcoming Meetings')]")
            ]
            headings = []
            for by, value in selectors:
                try:
                    elements = self.driver.find_elements(by, value)
                    headings.extend(elements)
                    logger.debug(f"Found {len(elements)} elements using {by}='{value}'")
                except Exception as e:
                    logger.debug(f"Selector {by}='{value}' failed: {e}")

            if not headings:
                logger.warning("No meetings sections found with any selector")
                self._log_page_state("after failing to find Meetings section")
                return meetings

            for heading in headings:
                heading_text = heading.text.strip()
                logger.debug(f"Found meetings section: {heading_text}")

                # Handle "Regular Meetings" section (table-based)
                if "Regular Meetings" in heading_text:
                    try:
                        table = heading.find_element(By.XPATH, "following-sibling::table")
                        rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header row
                        logger.debug(f"Found {len(rows)} meeting rows in section '{heading_text}'")

                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 4:
                                # First cell is the board name, second cell is the date
                                meeting_board = cells[0].text.strip()
                                date_str = cells[1].text.strip()
                                location = cells[2].text.strip()
                                details_link = cells[3].find_element(By.TAG_NAME, "a").get_attribute("href")

                                date = self._parse_date(date_str)
                                if date:
                                    meetings.append({
                                        "board_name": meeting_board,
                                        "date": date,
                                        "location": location,
                                        "status": "Scheduled",
                                        "details_url": details_link
                                    })
                                    logger.debug(f"Scraped meeting: {meeting_board}, {date_str}, {location}, Scheduled, {details_link}")
                    except NoSuchElementException:
                        logger.debug(f"No table found for section '{heading_text}'")
                        continue

                # Handle "Upcoming Meetings", "Past Meetings", and "Planning Board Upcoming Meetings" sections
                else:
                    try:
                        # Get all elements following the heading until the next section
                        next_elements = heading.find_elements(By.XPATH, "following-sibling::*")
                        meeting_data = []
                        current_meeting = {}
                        for elem in next_elements:
                            elem_text = elem.text.strip()
                            # Stop if we reach another section heading
                            if any(section in elem_text for section in ["Upcoming Meetings", "Past Meetings", "Planning Board Upcoming Meetings", "Subcommittees"]):
                                break
                            if not elem_text:
                                continue

                            # Look for a pattern: Board Name, Date, Location, Details
                            if "Details and Agenda" in elem_text:
                                if current_meeting:
                                    meeting_data.append(current_meeting)
                                    current_meeting = {}
                                continue
                            elif any(month in elem_text for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]):
                                current_meeting['date'] = elem_text
                            elif "Myron E. Richardson" in elem_text or "Town House" in elem_text or "Cancelled" in elem_text:
                                current_meeting['location'] = elem_text
                            elif elem.tag_name == "a" and "meeting" in elem.get_attribute("href"):
                                current_meeting['details_url'] = elem.get_attribute("href")
                            else:
                                current_meeting['board_name'] = elem_text

                        # Process collected meeting data
                        for meeting in meeting_data:
                            if not all(key in meeting for key in ['board_name', 'date', 'location', 'details_url']):
                                continue
                            date = self._parse_date(meeting['date'])
                            status = "Cancelled" if "Cancelled" in meeting['location'] else "Scheduled"
                            if date:
                                meetings.append({
                                    "board_name": meeting['board_name'],
                                    "date": date,
                                    "location": meeting['location'],
                                    "status": status,
                                    "details_url": meeting['details_url']
                                })
                                logger.debug(f"Scraped meeting: {meeting['board_name']}, {meeting['date']}, {meeting['location']}, {status}, {meeting['details_url']}")
                    except Exception as e:
                        logger.debug(f"Error parsing section '{heading_text}': {e}")
                        continue

        except Exception as e:
            logger.error(f"Error scraping meetings: {e}")
            debug_path = os.path.join(self.data_dir, board_name, "debug", f"meetings_error_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML for meetings: {debug_path}")
            self._log_page_state("after meetings scrape failure")

        return meetings

    def scrape_board(self, board_name, board_url):
        logger.info(f"Scraping board: {board_name}")
        board_dir = os.path.join(self.data_dir, board_name)
        os.makedirs(board_dir, exist_ok=True)

        # Fetch the board page
        content = fetch_page(self.driver, board_url, self.cache, bypass_cache=self.bypass_cache)

        # Switch to content iframe
        iframe_exists = has_iframe(self.driver, "content")
        if iframe_exists:
            self.driver.switch_to.frame("content")
            logger.debug("Switched to content iframe")
        else:
            logger.warning("No content iframe found")

        # Log initial page state
        self._log_page_state("after loading board page")

        # Scrape board details
        details = self._scrape_board_details()
        logger.info(f"Board details: name={details['name']}, chair={details['chair']}, clerk={details['clerk']}")

        # Save debug HTML of the board page
        debug_path = os.path.join(board_dir, "debug", f"board_{board_name}_{int(time.time())}.html")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(self.driver.page_source)
        logger.info(f"Saved board debug HTML: {debug_path}")

        # Take screenshot if enabled
        if self.screenshots_enabled:
            try:
                png_path, pdf_path = take_full_screenshot(self.driver, board_dir, self.config, prefix=f"board_{board_name}")
                if png_path and pdf_path:
                    logger.info(f"Board screenshot saved: PNG={png_path}, PDF={pdf_path}")
                else:
                    logger.error("Failed to save board screenshot")
            except Exception as e:
                logger.error(f"Error taking board screenshot: {e}")

        # Scrape meetings
        meetings = self._scrape_meetings(board_name)
        logger.info(f"Scraped {len(meetings)} meetings for board {board_name}")

        # Save meetings to CSV
        if meetings:
            df_meetings = pd.DataFrame(meetings)
            meeting_csv = os.path.join(board_dir, "board_meeting_data.csv")
            os.makedirs(os.path.dirname(meeting_csv), exist_ok=True)
            df_meetings.to_csv(meeting_csv, index=False)
            logger.info(f"Saved meeting CSV for {board_name}: {meeting_csv}")
        else:
            logger.warning(f"No meetings to save for {board_name}")

        # Switch back to default content
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

            if self.focus_mode and board_name != self.focus_board:
                logger.info(f"Skipping board {board_name} (focus mode enabled for {self.focus_board})")
                continue

            self.scrape_board(board_name, board_url)

        logger.info("Board scraping completed successfully")

    def close(self):
        self.driver.quit()

def main():
    # For debugging, disable headless mode and bypass cache
    scraper = BoardScraper(headless=False, bypass_cache=True)
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()