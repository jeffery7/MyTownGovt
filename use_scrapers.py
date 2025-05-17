#!/usr/bin/env python3
import os
import logging
import csv
from mytowngov_common import load_config, Cache
from mytowngov_homepage_scraper import HomepageScraper
from mytowngov_board_scraper import BoardScraper
from mytowngov_meeting_scraper import MeetingScraper

# Logging setup
logger = logging.getLogger(__name__)

def main():
    config = load_config()
    cache = Cache(config)
    
    # Step 1: Scrape homepage
    logger.info("Starting homepage scraping")
    homepage_scraper = HomepageScraper(config)
    try:
        homepage_result = homepage_scraper.scrape_homepage()
        if not homepage_result:
            logger.error("Homepage scraping failed, exiting")
            return
        logger.info("Homepage scraping completed")
    finally:
        homepage_scraper.close()
    
    # Step 2: Scrape boards (focusing on Planning Board)
    logger.info("Starting board scraping")
    board_scraper = BoardScraper(config)
    try:
        board_result = board_scraper.scrape_boards()
        if not board_result:
            logger.error("Board scraping failed, exiting")
            return
        logger.info("Board scraping completed")
    finally:
        board_scraper.close()
    
    # Step 3: Scrape meetings (focusing on Planning Board, Sep 10, 2024)
    logger.info("Starting meeting scraping")
    meeting_scraper = MeetingScraper(config)
    try:
        # Read board_meeting_data.csv for meeting URLs
        board_dir = os.path.join(config['data_dir'], 'Planning_Board')
        meeting_csv = os.path.join(board_dir, 'board_meeting_data.csv')
        if not os.path.exists(meeting_csv):
            logger.error(f"Meeting CSV not found: {meeting_csv}, exiting")
            return
        
        meetings = []
        with open(meeting_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filter for Sep 10, 2024
                if '2024-09-10' in row['date']:
                    meeting_data = meeting_scraper.scrape_meeting(row['details_url'], row['date'])
                    if meeting_data:
                        meetings.append(meeting_data)
        
        if meetings:
            meeting_scraper.save_meeting_data(meetings)
            logger.info("Meeting scraping completed")
        else:
            logger.warning("No meetings scraped")
    finally:
        meeting_scraper.close()

if __name__ == "__main__":
    main()