#!/usr/bin/env python3

import os
import logging
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from mytowngov_common import load_config, setup_driver, fetch_page, take_full_screenshot, Cache, has_iframe

# Configure logging
logger = logging.getLogger(__name__)

class HomepageScraper:
    def __init__(self, config_path='config.yaml'):
        self.config = load_config(config_path)
        self.base_url = self.config['homepage_url']
        self.data_dir = self.config['data_dir']
        self.cache = Cache(self.config)
        self.driver = setup_driver()
        self.wait = WebDriverWait(self.driver, 20)
        self.screenshots_enabled = self.config.get('screenshots', {}).get('enabled', True)
        self.board_dir = os.path.join(self.data_dir, 'homepage')
        os.makedirs(self.board_dir, exist_ok=True)

    def _scrape_dropdown(self, heading_text):
        logger.info(f"Scraping section: {heading_text}")
        boards = []

        try:
            # Check for content iframe and switch to it
            logger.debug("Waiting for content iframe to be present")
            iframe = self.wait.until(EC.presence_of_element_located((By.NAME, 'content')))
            logger.debug("Switching to content iframe")
            self.driver.switch_to.frame(iframe)

            # Wait for the iframe content to load by checking for the heading
            logger.debug(f"Waiting for heading '{heading_text}' in iframe")
            heading = self.wait.until(EC.presence_of_element_located((By.XPATH, f"//h1[contains(text(), '{heading_text}')]")))
            logger.debug(f"Found heading: {heading_text}")

            # Find the table following the heading
            table = heading.find_element(By.XPATH, "following-sibling::table")
            logger.debug("Located table")

            # Find all rows in the table (skip the header row)
            rows = table.find_elements(By.TAG_NAME, "tr")[1:]
            logger.debug(f"Found {len(rows)} rows in {heading_text}")

            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 1:
                        link = cells[0].find_element(By.TAG_NAME, "a")
                        name = link.text.strip()
                        # Remove "(inactive)" from the name if present
                        name = name.replace(" (inactive)", "")
                        url = link.get_attribute('href')
                        if name and url:
                            boards.append({'Name': name, 'URL': url})
                            logger.debug(f"Scraped board: {name} - {url}")
                except Exception as e:
                    logger.error(f"Error processing row in {heading_text}: {e}")

        except TimeoutException as e:
            logger.error(f"Error scraping {heading_text}: {e}")
            debug_path = os.path.join(self.board_dir, 'debug', f"iframe_content_{int(time.time())}.html")
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.info(f"Saved debug HTML: {debug_path}")
        except Exception as e:
            logger.error(f"Unexpected error in _scrape_dropdown for {heading_text}: {e}")
        finally:
            self.driver.switch_to.default_content()
            logger.debug("Switched back to default content")

        return boards

    def scrape(self):
        logger.info(f"Scraping homepage: {self.base_url}")
        boards_data = []
        agencies_data = []

        # Load the page using fetch_page for caching
        content = fetch_page(self.driver, self.base_url, self.cache)

        if self.screenshots_enabled:
            try:
                # Ensure we're in the correct context for the screenshot
                logger.debug("Waiting for content iframe for screenshot")
                iframe = self.wait.until(EC.presence_of_element_located((By.NAME, 'content')))
                self.driver.switch_to.frame(iframe)
                logger.debug("Switched to iframe for screenshot")

                # Wait for some content to ensure the screenshot is meaningful
                self.wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(text(), 'Boards and Committees')]")))
                png_path, pdf_path = take_full_screenshot(self.driver, self.board_dir, self.config, prefix="homepage")
                if png_path and pdf_path:
                    logger.info(f"Saved screenshot: PNG={png_path}, PDF={pdf_path}")
                else:
                    logger.error("Failed to save screenshot")
            except Exception as e:
                logger.error(f"Error taking screenshot: {e}")
            finally:
                self.driver.switch_to.default_content()
                logger.debug("Switched back to default content after screenshot")

        # Scrape Boards and Committees
        boards = self._scrape_dropdown("Boards and Committees")
        boards_data.extend(boards)

        # Scrape Outside Agencies
        agencies = self._scrape_dropdown("Outside Agencies and Organizations")
        agencies_data.extend(agencies)

        if boards_data:
            df_boards = pd.DataFrame(boards_data)
            boards_csv = self.config['homepage_boards_csv']
            os.makedirs(os.path.dirname(boards_csv), exist_ok=True)
            df_boards.to_csv(boards_csv, index=False)
            logger.info(f"Saved boards to {boards_csv}")
        else:
            logger.warning("No data to save for homepage_boards_and_committees.csv")

        if agencies_data:
            df_agencies = pd.DataFrame(agencies_data)
            agencies_csv = self.config['homepage_agencies_csv']
            os.makedirs(os.path.dirname(agencies_csv), exist_ok=True)
            df_agencies.to_csv(agencies_csv, index=False)
            logger.info(f"Saved agencies to {agencies_csv}")
        else:
            logger.warning("No data to save for homepage_outside_agencies.csv")

        logger.info("Homepage scraping completed")

    def close(self):
        self.driver.quit()

def main():
    scraper = HomepageScraper()
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()