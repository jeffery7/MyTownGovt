#!/usr/bin/env python3
import os
import csv
import time
import logging
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as parse_date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Logging setup
logger = logging.getLogger(__name__)

class BoardScraper:
    def __init__(self, config):
        self.config = config
        self.data_dir = config['data_dir']
        self.base_url = config['base_url']
        self.cache = Cache(config)
        self.driver = setup_driver()
        self.screenshots_enabled = config.get('screenshots', {}).get('enabled', False)
        self.board_csv = os.path.join(self.data_dir, 'homepage_boards_and_committees.csv')
        self.target_board = 'Planning_Board'  # Consistent with config.yaml
        self.board_dir = os.path.join(self.data_dir, self.target_board)
        os.makedirs(self.board_dir, exist_ok=True)

    def scrape_boards(self):
        if not os.path.exists(self.board_csv):
            logger.error(f"Board CSV not found: {self.board_csv}")
            return None
        
        boards_df = pd.read_csv(self.board_csv)
        # Validate CSV columns
        expected_columns = ['Name', 'Chair', 'Clerk', 'URL']
        if not all(col in boards_df.columns for col in expected_columns):
            logger.error(f"CSV {self.board_csv} missing required columns: {expected_columns}, found: {list(boards_df.columns)}")
            return None
        
        # Log CSV contents
        logger.debug(f"Board CSV contents:\n{boards_df.to_string()}")
        
        board_data = []
        
        # Normalize board names by replacing underscores with spaces and doing case-insensitive comparison
        target_board_normalized = self.target_board.replace('_', ' ').lower()
        boards_df['Name_normalized'] = boards_df['Name'].str.replace('_', ' ').str.lower()
        board_row = boards_df[boards_df['Name_normalized'] == target_board_normalized]
        
        if board_row.empty:
            logger.error(f"Board {self.target_board} not found in CSV. Available boards: {boards_df['Name'].tolist()}")
            return None
        
        board = board_row.iloc[0]
        logger.info(f"Scraping board: {board['Name']}")
        board_info = self._scrape_board(board['URL'])
        if board_info:
            board_data.append(board_info)
            self._save_meeting_csv(board_info['meetings'], board['Name'])
        
        return board_data

    def _scrape_board(self, board_url):
        logger.info(f"Scraping board page: {board_url}")
        try:
            # Fetch page with caching
            content = fetch_page(self.driver, board_url, self.cache)
            
            # Take screenshot
            screenshot_data = {}
            if self.screenshots_enabled:
                png_path, pdf_path = take_full_screenshot(self.driver, self.board_dir, self.config, prefix='board_screenshot')
                if png_path and pdf_path:
                    screenshot_data = {'png_path': png_path, 'pdf_path': pdf_path}
                    logger.info(f"Board screenshot saved: PNG={png_path}, PDF={pdf_path}")
            
            # Check for content iframe
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                logger.debug("Switching to content iframe")
                self.driver.switch_to.frame('content')
            
            # Scrape board details
            board_info = {
                'url': board_url,
                'name': self._get_element_text('.board-name'),
                'chair': self._get_element_text('.board-chair'),
                'clerk': self._get_element_text('.board-clerk'),
                'meetings': []
            }
            
            # Scrape meeting tables
            for selector in ['.upcoming-meetings-table', '.past-meetings-table', 'table.meetings', 'div.meetings table']:
                table = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if table:
                    # Save table HTML for debugging
                    debug_path = os.path.join(self.board_dir, 'debug', f"meetings_table_{selector.replace('.', '_')}_{int(time.time())}.html")
                    os.makedirs(os.path.dirname(debug_path), exist_ok=True)
                    with open(debug_path, 'w', encoding='utf-8') as f:
                        f.write(table[0].get_attribute('outerHTML'))
                    logger.info(f"Saved meeting table HTML: {debug_path}")
                    meetings = self._scrape_meeting_table(table[0])
                    board_info['meetings'].extend(meetings)
                    logger.debug(f"Scraped meetings: {[m['date'] for m in meetings]}")
            
            if iframe_exists:
                self.driver.switch_to.default_content()
            board_info['screenshots'] = screenshot_data
            logger.info(f"Scraped {len(board_info['meetings'])} meetings for board")
            return board_info
        except Exception as e:
            logger.error(f"Error scraping board {board_url}: {str(e)}", exc_info=True)
            # Save raw HTML for debugging
            debug_path = os.path.join(self.board_dir, 'debug', f"board_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            return None

    def _scrape_meeting_table(self, table):
        meetings = []
        try:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 3:
                    try:
                        date_time = cells[0].text.strip()
                        # Skip non-date entries
                        if not date_time or any(keyword in date_time.lower() for keyword in ['time', 'committee', 'board']):
                            logger.debug(f"Skipping non-date entry: {date_time}")
                            continue
                        parsed_date = self._parse_date(date_time)
                        meeting = {
                            'date': parsed_date if parsed_date else date_time,
                            'location': cells[1].text.strip(),
                            'status': cells[2].text.strip() if len(cells) > 2 else '',
                            'details_url': cells[-1].find_element(By.TAG_NAME, 'a').get_attribute('href') if cells[-1].find_elements(By.TAG_NAME, 'a') else ''
                        }
                        meetings.append(meeting)
                    except Exception as e:
                        logger.warning(f"Error parsing meeting row: {str(e)}")
        except Exception as e:
            logger.error(f"Error scraping meeting table: {str(e)}", exc_info=True)
        return meetings

    def _parse_date(self, date_str):
        try:
            parsed = parse_date(date_str, fuzzy=True)
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"Failed to parse date: {date_str}, error: {str(e)}")
            return None

    def _get_element_text(self, selector):
        try:
            element = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return element.text.strip()
        except Exception:
            return ''

    def _save_meeting_csv(self, meetings, board_name):
        if not meetings:
            logger.warning(f"No meetings to save for {board_name}")
            return
        csv_path = os.path.join(self.board_dir, 'board_meeting_data.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'location', 'status', 'details_url'])
            writer.writeheader()
            writer.writerows(meetings)
        logger.info(f"Saved meeting CSV: {csv_path}")

    def close(self):
        self.driver.quit()

def main():
    config = load_config()
    scraper = BoardScraper(config)
    try:
        result = scraper.scrape_boards()
        if result:
            logger.info("Board scraping completed successfully")
        else:
            logger.error("Board scraping failed")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()