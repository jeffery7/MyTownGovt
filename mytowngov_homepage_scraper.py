#!/usr/bin/env python3
import os
import csv
import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Logging setup
logger = logging.getLogger(__name__)

class HomepageScraper:
    def __init__(self, config):
        self.config = config
        self.data_dir = config['data_dir']
        self.base_url = config['base_url']
        self.zip = config['zip']
        self.homepage_url = f"{self.base_url}/{self.zip}"
        self.cache = Cache(config)
        self.driver = setup_driver()
        self.screenshots_enabled = config.get('screenshots', {}).get('enabled', False)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, 'screenshots'), exist_ok=True)

    def scrape_homepage(self):
        logger.info(f"Scraping homepage: {self.homepage_url}")
        try:
            # Fetch page with caching (this will switch to iframe if present)
            content = fetch_page(self.driver, self.homepage_url, self.cache)
            
            # Take screenshot
            screenshot_data = {}
            if self.screenshots_enabled:
                png_path, pdf_path = take_full_screenshot(self.driver, os.path.join(self.data_dir, 'screenshots'), self.config, prefix='homepage_screenshot')
                if png_path and pdf_path:
                    screenshot_data = {'png_path': png_path, 'pdf_path': pdf_path}
                    logger.info(f"Homepage screenshot saved: PNG={png_path}, PDF={pdf_path}")
            
            # Scrape Boards and Committees from the content iframe
            boards = self._scrape_boards()
            self._save_csv(boards, 'homepage_boards_and_committees.csv')
            
            # Scrape Outside Agencies (if present)
            agencies = self._scrape_outside_agencies()
            self._save_csv(agencies, 'homepage_outside_agencies.csv')
            
            return {'boards': boards, 'agencies': agencies, 'screenshots': screenshot_data}
        except Exception as e:
            logger.error(f"Error scraping homepage: {str(e)}", exc_info=True)
            # Save raw HTML for debugging
            debug_path = os.path.join(self.data_dir, 'debug', f"homepage_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
            return None

    def _scrape_boards(self):
        data = []
        try:
            # Ensure we're in the content iframe if it exists
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                self.driver.switch_to.frame('content')
                logger.debug("Switched to content iframe for scraping boards")
            else:
                logger.debug("No content iframe found, scraping main content")

            # Try to find a table of boards
            selectors = ['.boards-and-committees-table', 'table.boards', 'div.boards table', 'table[data-type="boards"]']
            for selector in selectors:
                try:
                    table = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    # Save table HTML for debugging
                    debug_path = os.path.join(self.config['data_dir'], 'debug', f"boards_table_{selector.replace('.', '_')}_{int(time.time())}.html")
                    os.makedirs(os.path.dirname(debug_path), exist_ok=True)
                    with open(debug_path, 'w', encoding='utf-8') as f:
                        f.write(table.get_attribute('outerHTML'))
                    logger.info(f"Saved boards table HTML: {debug_path}")

                    rows = table.find_elements(By.TAG_NAME, 'tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if len(cells) >= 3:
                            name = cells[0].text.strip()
                            chair = cells[1].text.strip()
                            clerk = cells[2].text.strip()
                            url = ''
                            link = cells[0].find_elements(By.TAG_NAME, 'a')
                            if link:
                                url = link[0].get_attribute('href')
                            data.append({
                                'Name': name,
                                'Chair': chair,
                                'Clerk': clerk,
                                'URL': url
                            })
                    logger.info(f"Scraped {len(data)} boards from table with selector {selector}")
                    if data:
                        break
                except Exception as e:
                    logger.warning(f"Failed to scrape boards table with selector {selector}: {str(e)}")

            # Fallback to dropdown if table not found
            if not data:
                logger.warning("No boards table found, falling back to dropdown")
                self.driver.switch_to.default_content()
                data = self._scrape_dropdown("Boards and Committees:")

            if iframe_exists:
                self.driver.switch_to.default_content()
            logger.debug(f"Scraped board names: {[item['Name'] for item in data]}")
            return data
        except Exception as e:
            logger.error(f"Error scraping boards: {str(e)}", exc_info=True)
            if iframe_exists:
                self.driver.switch_to.default_content()
            return data

    def _scrape_outside_agencies(self):
        data = []
        try:
            # Ensure we're in the content iframe if it exists
            iframe_exists = has_iframe(self.driver, 'content')
            if iframe_exists:
                self.driver.switch_to.frame('content')
                logger.debug("Switched to content iframe for scraping outside agencies")
            else:
                logger.debug("No content iframe found, scraping main content")

            # Try to find a table of outside agencies
            selectors = ['.outside-agencies-table', 'table.agencies', 'div.agencies table', 'table[data-type="agencies"]']
            for selector in selectors:
                try:
                    table = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    # Save table HTML for debugging
                    debug_path = os.path.join(self.config['data_dir'], 'debug', f"agencies_table_{selector.replace('.', '_')}_{int(time.time())}.html")
                    os.makedirs(os.path.dirname(debug_path), exist_ok=True)
                    with open(debug_path, 'w', encoding='utf-8') as f:
                        f.write(table.get_attribute('outerHTML'))
                    logger.info(f"Saved outside agencies table HTML: {debug_path}")

                    rows = table.find_elements(By.TAG_NAME, 'tr')
                    for row in rows[1:]:  # Skip header
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if len(cells) >= 3:
                            name = cells[0].text.strip()
                            chair = cells[1].text.strip()
                            clerk = cells[2].text.strip()
                            url = ''
                            link = cells[0].find_elements(By.TAG_NAME, 'a')
                            if link:
                                url = link[0].get_attribute('href')
                            data.append({
                                'Name': name,
                                'Chair': chair,
                                'Clerk': clerk,
                                'URL': url
                            })
                    logger.info(f"Scraped {len(data)} agencies from table with selector {selector}")
                    if data:
                        break
                except Exception as e:
                    logger.warning(f"Failed to scrape agencies table with selector {selector}: {str(e)}")

            # Fallback to dropdown if table not found
            if not data:
                logger.warning("No outside agencies table found, falling back to dropdown")
                self.driver.switch_to.default_content()
                data = self._scrape_dropdown("Outside Agencies:")

            if iframe_exists:
                self.driver.switch_to.default_content()
            logger.debug(f"Scraped agency names: {[item['Name'] for item in data]}")
            return data
        except Exception as e:
            logger.error(f"Error scraping outside agencies: {str(e)}", exc_info=True)
            if iframe_exists:
                self.driver.switch_to.default_content()
            return data

    def _scrape_dropdown(self, heading_text):
        data = []
        try:
            logger.debug(f"Looking for dropdown heading: {heading_text}")
            heading = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, f"//span[@class='heading' and contains(text(), '{heading_text}')]"))
            )
            select = heading.find_element(By.XPATH, "following-sibling::select")
            
            # Save select HTML for debugging
            debug_path = os.path.join(self.config['data_dir'], 'debug', f"dropdown_{heading_text.replace(' ', '_').replace(':', '')}_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(select.get_attribute('outerHTML'))
            logger.info(f"Saved dropdown HTML: {debug_path}")
            
            options = select.find_elements(By.TAG_NAME, 'option')
            for option in options:
                name = option.text.strip()
                value = option.get_attribute('value')
                if not value or name in ["Select from list..."]:
                    continue
                url = f"{self.base_url}/board?board={value}" if value else ''
                data.append({
                    'Name': name,
                    'Chair': '',
                    'Clerk': '',
                    'URL': url
                })
            logger.info(f"Scraped {len(data)} items from {heading_text} dropdown")
        except Exception as e:
            logger.error(f"Error scraping {heading_text} dropdown: {str(e)}", exc_info=True)
        return data

    def _save_csv(self, data, filename):
        if not data:
            logger.warning(f"No data to save for {filename}")
            return
        csv_path = os.path.join(self.data_dir, filename)
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['Name', 'Chair', 'Clerk', 'URL'])
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Saved CSV: {csv_path}")
        with open(csv_path, 'r', encoding='utf-8') as f:
            logger.debug(f"CSV contents for {filename}:\n{f.read()}")

    def close(self):
        self.driver.quit()

def main():
    config = load_config()
    scraper = HomepageScraper(config)
    try:
        result = scraper.scrape_homepage()
        if result:
            logger.info("Homepage scraping completed successfully")
        else:
            logger.error("Homepage scraping failed")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()