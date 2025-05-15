#!/usr/bin/env python3

import yaml
import os
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
from mytowngov_common import fetch_page_content, setup_logging, save_to_csv, generate_pdf_screenshot
from error_tracker import ErrorTracker

def scrape_homepage(url, logger, driver, error_tracker):
    boards_data = []
    agencies_data = []
    meetings_data = []
    extracted_text = []

    # Step 1: Scrape main homepage
    max_retries = 3
    for attempt in range(max_retries):
        html = fetch_page_content(url, driver, logger, full_page=True, error_tracker=error_tracker)
        if not html:
            logger.error(f"Failed to fetch homepage content: {url} (attempt {attempt + 1}/{max_retries})")
            time.sleep(2)
            continue

        try:
            WebDriverWait(driver, 20).until(
                EC.frame_to_be_available_and_switch_to_it((By.NAME, 'content'))
            )
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Allow dynamic content to load
            html = driver.page_source
            driver.switch_to.default_content()
            break
        except Exception as e:
            logger.debug(f"Timeout or error waiting for content: {e} (attempt {attempt + 1}/{max_retries})")
            driver.switch_to.default_content()
            time.sleep(2)
    else:
        logger.error(f"Failed to load homepage content after {max_retries} attempts: {url}")
        return boards_data, agencies_data, extracted_text

    # Save debug HTML
    debug_html_path = "Hardwick_Data/debug_homepage.html"
    os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
    with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Saved debug HTML to {debug_html_path}")

    soup = BeautifulSoup(html, 'html.parser')
    logger.info("Extracting boards, agencies, and meetings data...")

    # Extract text for searchability
    header = soup.find('table', class_='header')
    if header:
        header_town = header.find('td', class_='headerTown')
        if header_town:
            for line in header_town.stripped_strings:
                extracted_text.append(line)
        print_button = header.find('button', attrs={'onclick': re.compile(r'print')})
        if print_button:
            extracted_text.append(print_button.get_text(strip=True))
        sign_in = header.find('a', href=re.compile(r'/login'))
        if sign_in:
            extracted_text.append(sign_in.get_text(strip=True))

    side_nav = soup.find('td', class_='sideNav')
    if side_nav:
        nav_links = side_nav.find_all('a')
        for link in nav_links:
            extracted_text.append(link.get_text(strip=True) if link else "")

    time_elements = soup.find_all('p', text=re.compile(r'The current local time|All meetings before'))
    for elem in time_elements:
        extracted_text.append(elem.get_text(strip=True) if elem else "")

    # Extract Boards and Committees
    boards_section = None
    for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        boards_section = soup.find(tag, text=re.compile(r'boards.*committees', re.IGNORECASE))
        if boards_section:
            logger.debug(f"Found boards section: {boards_section.get_text(strip=True)}")
            extracted_text.append(boards_section.get_text(strip=True))
            break

    if boards_section:
        boards_table = boards_section.find_next('table')
        if boards_table:
            rows = boards_table.find_all('tr')
            logger.debug(f"Found {len(rows)} rows in boards table")
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    board_name = cells[0].get_text(strip=True) if cells[0] else ""
                    chair = cells[1].get_text(strip=True) if cells[1] else ""
                    clerk = cells[2].get_text(strip=True) if cells[2] else ""
                    board_link = cells[0].find('a', href=re.compile(r'board\?board='))
                    board_url = f"https://www.mytowngovernment.org{board_link['href']}" if board_link else ""
                    active = "(inactive)" not in board_name.lower()
                    if board_name and board_name.strip():
                        board_entry = {
                            "Board": board_name.replace("(inactive)", "").strip(),
                            "Chair": chair,
                            "Clerk": clerk,
                            "Active": active,
                            "URL": board_url
                        }
                        boards_data.append(board_entry)
                        logger.debug(f"Appended board: {board_name} | URL: {board_url}")
                        row_text = f"{board_name} | {chair} | {clerk}"
                        extracted_text.append(row_text)
            if not boards_data:
                logger.warning("Parsed boards table but no valid boards were added")
                error_tracker.add_error('ParsingError', "Parsed boards table but no valid boards were added", url)
        else:
            logger.warning("No boards table found")
            error_tracker.add_error('ParsingError', "No boards table found", url)
    else:
        logger.warning("No boards section found")
        error_tracker.add_error('ParsingError', "No boards section found", url)

    # Extract Outside Agencies
    agencies_section = None
    for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        agencies_section = soup.find(tag, text=re.compile(r'outside\s*agencies', re.IGNORECASE))
        if boards_section:
            logger.debug(f"Found agencies section: {agencies_section.get_text(strip=True)}")
            extracted_text.append(agencies_section.get_text(strip=True))
            break

    if agencies_section:
        agencies_table = agencies_section.find_next('table')
        if agencies_table:
            rows = agencies_table.find_all('tr')
            logger.debug(f"Found {len(rows)} rows in agencies table")
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    agency_name = cells[0].get_text(strip=True) if cells[0] else ""
                    chair = cells[1].get_text(strip=True) if cells[1] else ""
                    clerk = cells[2].get_text(strip=True) if cells[2] else ""
                    agency_link = cells[0].find('a', href=re.compile(r'board\?board='))
                    agency_url = f"https://www.mytowngovernment.org{agency_link['href']}" if agency_link else ""
                    active = "(inactive)" not in agency_name.lower()
                    if agency_name and agency_name.strip():
                        agency_entry = {
                            "Agency": agency_name.replace("(inactive)", "").strip(),
                            "Chair": chair,
                            "Clerk": clerk,
                            "Active": active,
                            "URL": agency_url
                        }
                        agencies_data.append(agency_entry)
                        logger.debug(f"Appended agency: {agency_name} | URL: {agency_url}")
                        row_text = f"{agency_name} | {chair} | {clerk}"
                        extracted_text.append(row_text)
            if not agencies_data:
                logger.warning("Parsed agencies table but no valid agencies were added")
                error_tracker.add_error('ParsingError', "Parsed agencies table but no valid agencies were added", url)
        else:
            logger.warning("No agencies table found")
            error_tracker.add_error('ParsingError', "No agencies table found", url)
    else:
        logger.warning("No agencies section found")
        error_tracker.add_error('ParsingError', "No agencies section found", url)

    # Step 2: Navigate to "All Meetings" view for past meetings
    all_meetings_url = "https://www.mytowngovernment.org/01031/calendar"
    html = None
    for attempt in range(max_retries):
        try:
            logger.info(f"Navigating to All Meetings view: {all_meetings_url} (attempt {attempt + 1}/{max_retries})")
            driver.get(all_meetings_url)
            WebDriverWait(driver, 20).until(
                EC.frame_to_be_available_and_switch_to_it((By.NAME, 'content'))
            )
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(3)
            html = driver.page_source
            driver.switch_to.default_content()
            break
        except Exception as e:
            logger.warning(f"Failed to load All Meetings view: {e} (attempt {attempt + 1}/{max_retries})")
            error_tracker.add_error('FetchError', f"Failed to load All Meetings view: {e}", all_meetings_url)
            driver.switch_to.default_content()
            time.sleep(2)

    if html:
        soup = BeautifulSoup(html, 'html.parser')
        meetings_table = soup.find('table', recursive=True, string=re.compile(r'Details and Agenda', re.IGNORECASE))
        if meetings_table:
            rows = meetings_table.find_all('tr')
            logger.debug(f"Found {len(rows)} rows in All Meetings table")
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 4:
                    board_name = cells[0].get_text(strip=True) if cells[0] else ""
                    time_text = cells[1].get_text(strip=True).replace('\n', ' ') if cells[1] else ""
                    location = cells[2].get_text(strip=True).replace('\n', ' ') if cells[2] else ""
                    meeting_link = cells[3].find('a', href=re.compile(r'meeting\?meeting='))
                    meeting_url = f"https://www.mytowngovernment.org{meeting_link['href']}" if meeting_link else ""
                    timestamp = parse_time_to_timestamp(time_text, logger, error_tracker)
                    if timestamp:
                        meeting_entry = {
                            "Board": board_name,
                            "Timestamp": timestamp,
                            "Time": time_text,
                            "Location": location,
                            "URL": meeting_url
                        }
                        meetings_data.append(meeting_entry)
                        logger.debug(f"Added meeting: {board_name} | {timestamp} | URL: {meeting_url}")
                        extracted_text.append(f"Meeting: {board_name} | {time_text} | {location}")
            if not meetings_data:
                logger.warning("Parsed All Meetings table but no valid meetings were added")
                error_tracker.add_error('ParsingError', "Parsed All Meetings table but no valid meetings were added", all_meetings_url)
        else:
            logger.warning("No meetings table found in All Meetings view")
            error_tracker.add_error('ParsingError', "No meetings table found in All Meetings view", all_meetings_url)

    # Save CSVs even if data is empty to ensure files are created
    board_csv = "Hardwick_Data/homepage_boards_and_committees.csv"
    board_fieldnames = ["Board", "Chair", "Clerk", "Active", "URL"]
    save_to_csv(boards_data, board_csv, board_fieldnames, logger, error_tracker)
    logger.info(f"Saved boards data to {board_csv} ({len(boards_data)} entries)")

    agency_csv = "Hardwick_Data/homepage_outside_agencies.csv"
    agency_fieldnames = ["Agency", "Chair", "Clerk", "Active", "URL"]
    save_to_csv(agencies_data, agency_csv, agency_fieldnames, logger, error_tracker)
    logger.info(f"Saved agencies data to {agency_csv} ({len(agencies_data)} entries)")

    if meetings_data:
        meetings_csv = "Hardwick_Data/homepage_meetings.csv"
        meeting_fieldnames = ["Board", "Timestamp", "Time", "Location", "URL"]
        save_to_csv(meetings_data, meetings_csv, meeting_fieldnames, logger, error_tracker)
        logger.info(f"Saved meetings data to {meetings_csv} ({len(meetings_data)} entries)")

    logger.debug(f"Total boards collected: {len(boards_data)}")
    logger.debug(f"Total agencies collected: {len(agencies_data)}")
    logger.debug(f"Total meetings collected: {len(meetings_data)}")
    return boards_data, agencies_data, extracted_text

def main():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger = setup_logging(config.get("debug", False))
    error_tracker = ErrorTracker()
    url = config.get("homepage_url", "https://www.mytowngovernment.org/01031")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,4000")
    driver = webdriver.Chrome(options=options)

    try:
        logger.info(f"Scraping homepage: {url}...")
        boards_data, agencies_data, extracted_text = scrape_homepage(url, logger, driver, error_tracker)

        # Generate PDF screenshot
        pdf_path = "Hardwick_Data/homepage_screenshot.pdf"
        png_path = "Hardwick_Data/homepage_screenshot.png"
        generate_pdf_screenshot(url, pdf_path, driver, logger, error_tracker, extracted_text, save_png_path=png_path)
        logger.info(f"Generated homepage screenshot: PNG={png_path}, PDF={pdf_path}")

    finally:
        logger.info(f"Error Summary: Total Errors = {error_tracker.total_errors}, Total Warnings = {error_tracker.total_warnings}")
        with open("Hardwick_Data/errors.log", "w", encoding="utf-8") as f:
            f.write("Scraper Error Log\n=================\n")
            if error_tracker.errors:
                for error in error_tracker.errors:
                    f.write(f"Timestamp: {error['timestamp']}\n")
                    f.write(f"Type: {error['type']}\n")
                    f.write(f"Message: {error['message']}\n")
                    f.write(f"URL: {error['url']}\n")
                    f.write(f"Warning: {error['is_warning']}\n")
                    if 'retry_count' in error:
                        f.write(f"Retry Count: {error['retry_count']}\n")
                    f.write("-----------------\n")
            else:
                f.write("No errors occurred during the run.\n")
        logger.info("Saved error log to Hardwick_Data/errors.log")
        driver.quit()

if __name__ == "__main__":
    main()