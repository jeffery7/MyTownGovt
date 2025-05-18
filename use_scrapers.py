#!/usr/bin/env python3

import logging
import yaml
import os
from mytowngov_homepage_scraper import HomepageScraper
from mytowngov_board_scraper import BoardScraper
from mytowngov_meeting_scraper import MeetingScraper

# Ensure the logging directory exists
log_dir = 'Hardwick_Data'
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('Hardwick_Data/scraper.log'),
        logging.StreamHandler()
    ]
)

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    config_path = 'config.yaml'
    config = load_config(config_path)

    # Step 1: Scrape the homepage
    logging.info("Starting homepage scraping")
    homepage_scraper = None
    try:
        homepage_scraper = HomepageScraper(config_path)
        homepage_scraper.scrape()
    except Exception as e:
        logging.error(f"Homepage scraping failed: {e}")
        raise
    finally:
        if homepage_scraper is not None:
            homepage_scraper.close()

    # Step 2: Scrape the boards
    logging.info("Starting board scraping")
    board_scraper = None
    try:
        board_scraper = BoardScraper(config_path)
        board_scraper.scrape()
    except Exception as e:
        logging.error(f"Board scraping failed: {e}")
        raise
    finally:
        if board_scraper is not None:
            board_scraper.close()

    # Step 3: Scrape the meetings
    logging.info("Starting meeting scraping")
    meeting_scraper = None
    try:
        meeting_scraper = MeetingScraper(config_path)
        meeting_scraper.scrape()
    except Exception as e:
        logging.error(f"Meeting scraping failed: {e}")
        raise
    finally:
        if meeting_scraper is not None:
            meeting_scraper.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Scraping process failed: {e}")
        exit(1)