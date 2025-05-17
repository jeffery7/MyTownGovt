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
    def __init__(self, config_path='config.yaml'):
        self.config = load_config(config_path)
        self.data_dir = self.config['data_dir']
        self.cache = Cache(self.config)
        self.driver = setup_driver()
        self.wait = WebDriverWait(self.driver, 15)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.input_csv = self.config['boards_input_csv']
        self.output_csv = self.config['boards_output_csv']

    def scrape_boards(self):
        if not os.path.exists(self.input_csv):
            logger.error(f"Board CSV not found: {self.input_csv}")
            return None
        
        boards_df = pd.read_csv(self.input_csv)
        expected_columns = ['Name', 'URL']
        if not all(col in boards_df.columns for col in expected_columns):
            logger.error(f"CSV {self.input_csv} missing required columns: {expected_columns}, found: {list(boards_df.columns)}")
            return None
        
        logger.debug(f"Board CSV contents:\n{boards_df.to_string()}")
        board_data = []
        
        # Filter boards based on focus mode
        boards_to_scrape = []
        if self.config.get('focus_mode_boards', False):
            target_board = self.config.get('focus_board', '').replace(' ', '_')
            boards_df['Name_normalized'] = boards_df['Name'].str.replace(' ', '_').str.lower()
            target_board_normalized = target_board.lower()
            board_row = boards_df[boards_df['Name_normalized'] == target_board_normalized]
            if board_row.empty:
                logger.error(f"Board {target_board} not found in CSV. Available boards: {boards_df['Name'].tolist()}")
                return None
            boards_to_scrape = [board_row.iloc[0]]
            logger.info(f"Focus mode enabled, scraping only: {target_board}")
        else:
            boards_to_scrape = boards_df.to_dict('records')
            logger.info("Scraping all boards")

        for board in boards_to_scrape:
            board_name = board['Name'].replace(' ', '_')
            board_url = board['URL']
            logger.info(f"Scraping board: {board_name}")
            board_info = self._scrape_board(board_name, board_url)
            if board_info:
                board_data.append(board_info)
                self._save_meeting_csv(board_info['meetings'], board_name)
        
        return board_data

    def _scrape_board(self, board_name, board_url):
        logger.info(f"Scraping board page: {board_url}")
        try:
            # Create board-specific directory
            board_dir = os.path.join(self.data_dir, board_name)
            os.makedirs(board_dir, exist_ok=True)

            content = fetch_page(self.driver, board_url, self.cache)
            
            screenshot_data = {}
            if self.screenshots_enabled:
                safe_board_name = board_name.replace(' ', '_').replace('/', '_')
                png_path, pdf_path = take_full_screenshot(self.driver, board_dir, self.config, prefix=f"board_{safe_board_name}")
                if png_path and pdf_path:
                    screenshot_data = {'png_path': png_path, 'pdf_path': pdf_path}
                    logger.info(f"Board screenshot saved: PNG={png_path}, PDF={pdf_path}")
            
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                logger.debug("Switching to content iframe")
                self.driver.switch_to.frame('content')
            
            board_info = {
                'url': board_url,
                'name': self._get_element_text('.board-name'),
                'chair': self._get_element_text('.board-chair'),
                'clerk': self._get_element_text('.board-clerk'),
                'meetings': []
            }
            
            for selector in ['.upcoming-meetings-table', '.past-meetings-table', 'table.meetings', 'div.meetings table']:
                table = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if table:
                    debug_path = os.path.join(board_dir, 'debug', f"meetings_table_{selector.replace('.', '_')}_{int(time.time())}.html")
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
            logger.info(f"Scraped {len(board_info['meetings'])} meetings for board {board_name}")
            return board_info
        except Exception as e:
            logger.error(f"Error scraping board {board_url}: {str(e)}", exc_info=True)
            board_dir = os.path.join(self.data_dir, board_name)
            debug_path = os.path.join(board_dir, 'debug', f"board_{board_name}_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            return None

    def _scrape_meeting_table(self, table):
        meetings = []
        try:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows[1:]:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 3:
                    try:
                        date_time = cells[0].text.strip()
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
            element = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return element.text.strip()
        except Exception:
            return ''

    def _save_meeting_csv(self, meetings, board_name):
        if not meetings:
            logger.warning(f"No meetings to save for {board_name}")
            return
        board_dir = os.path.join(self.data_dir, board_name)
        os.makedirs(board_dir, exist_ok=True)
        csv_path = os.path.join(board_dir, 'board_meeting_data.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'location', 'status', 'details_url'])
            writer.writeheader()
            writer.writerows(meetings)
        logger.info(f"Saved meeting CSV for {board_name}: {csv_path}")

    def close(self):
        self.driver.quit()

def main():
    scraper = BoardScraper()
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