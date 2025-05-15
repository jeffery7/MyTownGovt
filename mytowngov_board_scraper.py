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
from mytowngov_common import fetch_page_content, setup_logging, save_to_csv, parse_time_to_timestamp
from error_tracker import ErrorTracker
from dateutil.parser import parse as parse_date
from datetime import datetime

def scrape_board(url, board_name, board_dir, logger, driver, force_redownload=False, error_tracker=None, year=None, meeting_date=None):
    """
    Scrape a board page for meetings and board information.
    Args:
        url (str): URL of the board page, from config.yaml or homepage_boards_and_committees.csv.
        board_name (str): Name of the board (e.g., "Planning Board").
        board_dir (str): Directory to save board data.
        logger: Logger instance.
        driver: Selenium WebDriver instance.
        force_redownload (bool): Whether to bypass cache.
        error_tracker: ErrorTracker instance for logging errors.
        year (str): Optional year filter for meetings.
        meeting_date (str): Optional specific meeting date filter (e.g., "Sep 10, 2024").
    Returns:
        tuple: (board_data, meetings, extracted_text)
    """
    board_data = {
        "Board": board_name,
        "URL": url,
        "Members": [],
        "Email Subscribe URL": "",
        "Calendar Feed URL": ""
    }
    meetings = []
    extracted_text = []

    # Fetch board page content with retries
    max_retries = 3
    for attempt in range(max_retries):
        html = fetch_page_content(url, driver, logger, force_redownload=force_redownload, error_tracker=error_tracker, full_page=True)
        if not html:
            logger.error(f"Failed to fetch board content: {url} (attempt {attempt + 1}/{max_retries})")
            time.sleep(2)
            continue

        try:
            # Switch to content frame
            WebDriverWait(driver, 15).until(
                EC.frame_to_be_available_and_switch_to_it((By.NAME, 'content'))
            )
            # Wait for any table or meeting-related content
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Allow dynamic content to load
            html = driver.page_source
            driver.switch_to.default_content()
            break
        except Exception as e:
            logger.debug(f"Error accessing content frame or table: {e} (attempt {attempt + 1}/{max_retries})")
            driver.switch_to.default_content()
            time.sleep(2)
    else:
        logger.error(f"Failed to load board page content after {max_retries} attempts: {url}")
        return board_data, meetings, extracted_text

    # Save debug HTML
    debug_html_path = os.path.join(board_dir, "debug_board.html")
    os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
    with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Saved debug HTML to {debug_html_path}")

    soup = BeautifulSoup(html, 'html.parser')
    logger.info(f"Extracting data for board: {board_name}")

    # Extract header information
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

    # Extract navigation links
    side_nav = soup.find('td', class_='sideNav')
    if side_nav:
        nav_links = side_nav.find_all('a')
        for link in nav_links:
            text = link.get_text(strip=True) if link else ""
            href = link.get('href', '') if link else ""
            if 'groups.google.com' in href:
                board_data["Email Subscribe URL"] = f"https://www.mytowngovernment.org{href}" if href.startswith('/') else href
                extracted_text.append(f"Email Subscribe: {text}")
            elif 'calfeed' in href or 'calendar.ics' in href:
                board_data["Calendar Feed URL"] = f"https://www.mytowngovernment.org{href}" if href.startswith('/') else href
                extracted_text.append(f"Calendar Feed: {text}")
            else:
                extracted_text.append(text)

    if not board_data["Email Subscribe URL"]:
        logger.warning("No Email Subscribe URL found")
        error_tracker.add_error('ParsingError', "No Email Subscribe URL found", url, is_warning=True)
    if not board_data["Calendar Feed URL"]:
        logger.warning("No Calendar Feed URL found")
        error_tracker.add_error('ParsingError', "No Calendar Feed URL found", url, is_warning=True)

    # Extract members
    members_section = None
    for tag in ['h1', 'h2', 'h3']:
        members_section = soup.find(tag, text=re.compile(r'members', re.IGNORECASE))
        if members_section:
            logger.debug(f"Found members section: {members_section.get_text(strip=True)}")
            extracted_text.append(members_section.get_text(strip=True))
            break

    if members_section:
        members_table = members_section.find_next('table')
        if members_table:
            rows = members_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    member_name = cells[0].get_text(strip=True) if cells[0] else ""
                    role = cells[1].get_text(strip=True) if cells[1] else ""
                    if member_name:
                        board_data["Members"].append({"Name": member_name, "Role": role})
                        extracted_text.append(f"Member: {member_name} | Role: {role}")
        else:
            logger.warning("No members table found")
            error_tracker.add_error('ParsingError', "No members table found", url, is_warning=True)

    # Extract meetings from all meeting-related sections
    meeting_sections = []
    for tag in ['h1', 'h2', 'h3']:
        sections = soup.find_all(tag, text=re.compile(r'meetings', re.IGNORECASE))
        meeting_sections.extend(sections)

    for section in meeting_sections:
        section_text = section.get_text(strip=True) if section else ""
        logger.debug(f"Processing meeting section: {section_text}")
        if section_text:
            extracted_text.append(section_text)

        # Skip "Regular Meetings" if it’s not followed by a table with meeting links
        if "Regular Meetings" in section_text:
            next_table = section.find_next('table')
            if next_table and not next_table.find('a', href=re.compile(r'meeting\?meeting=')):
                logger.debug(f"Skipping Regular Meetings section: no meeting links in table")
                continue

        meetings_table = section.find_next('table')
        if not meetings_table:
            logger.debug(f"No table found after section: {section_text}")
            # Fallback: Look for any table containing meeting links
            meetings_table = soup.find('table', recursive=True, string=re.compile(r'Details and Agenda', re.IGNORECASE))
            if meetings_table:
                logger.debug(f"Found fallback meetings table for section: {section_text}")

        if meetings_table:
            rows = meetings_table.find_all('tr')
            logger.debug(f"Found {len(rows)} rows in meetings table for section: {section_text}")
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 4:  # Expect Time, Location, Minutes, Details
                    time_text = cells[0].get_text(strip=True).replace('\n', ' ') if cells[0] else ""
                    location = cells[1].get_text(strip=True).replace('\n', ' ') if cells[1] else ""
                    minutes = cells[2].get_text(strip=True) if cells[2] else ""
                    meeting_link = cells[-1].find('a', href=re.compile(r'meeting\?meeting=')) if cells[-1] else None
                    meeting_url = f"https://www.mytowngovernment.org{meeting_link['href']}" if meeting_link and meeting_link.get('href') else ""
                    timestamp = parse_time_to_timestamp(time_text, logger, error_tracker)
                    if timestamp:
                        meeting_entry = {
                            "Timestamp": timestamp,
                            "Time": time_text,
                            "Location": location,
                            "Minutes": minutes,
                            "URL": meeting_url,
                            "Is Past Meeting": "Past Meetings" in section_text
                        }
                        meetings.append(meeting_entry)
                        logger.debug(f"Added meeting: {timestamp} | URL: {meeting_url}")
                        extracted_text.append(f"Meeting: {time_text} | Location: {location} | Minutes: {minutes}")
                    else:
                        logger.warning(f"Skipping meeting with unparsable timestamp: {time_text}")
                else:
                    logger.debug(f"Skipping row with insufficient cells: {len(cells)} in section: {section_text}")
            if not any(m.get("Timestamp") for m in meetings):
                logger.warning(f"Parsed meetings table but no valid meetings added for section: {section_text}")
                error_tracker.add_error('ParsingError', f"Parsed meetings table but no valid meetings added for section: {section_text}", url)
        else:
            logger.warning(f"No meetings table found for section: {section_text}")
            error_tracker.add_error('ParsingError', f"No meetings table found for section: {section_text}", url)

    # Filter meetings by year and date if specified
    if year or meeting_date:
        filtered_meetings = []
        target_date = None
        if meeting_date:
            try:
                target_date = parse_date(meeting_date).date()
                logger.debug(f"Filtering for meeting date: {target_date}")
            except ValueError as e:
                logger.error(f"Invalid meeting_date format: {e}")
                error_tracker.add_error('ConfigError', f"Invalid meeting_date format: {e}", "config.yaml")
                return board_data, [], extracted_text

        for meeting in meetings:
            try:
                meeting_datetime = parse_date(meeting["Timestamp"])
                meeting_date_only = meeting_datetime.date()
                if year and meeting_datetime.year != int(year):
                    logger.debug(f"Skipping meeting {meeting['Timestamp']}: year {meeting_datetime.year} does not match {year}")
                    continue
                if target_date and meeting_date_only != target_date:
                    logger.debug(f"Skipping meeting {meeting['Timestamp']}: date {meeting_date_only} does not match {target_date}")
                    continue
                filtered_meetings.append(meeting)
            except ValueError as e:
                logger.warning(f"Could not parse meeting timestamp {meeting['Timestamp']}: {e}")
                error_tracker.add_error('TimeParseError', f"Could not parse meeting timestamp: {e}", meeting["URL"])
        
        meetings = filtered_meetings
        logger.debug(f"After filtering, {len(meetings)} meetings remain")

    # Save board data to CSV
    board_csv = os.path.join(board_dir, "board_data.csv")
    board_fieldnames = ["Board", "URL", "Members", "Email Subscribe URL", "Calendar Feed URL"]
    save_to_csv([board_data], board_csv, board_fieldnames, logger, error_tracker)

    # Save meetings data to CSV
    if meetings:
        meeting_csv = os.path.join(board_dir, "board_meeting_data.csv")
        meeting_fieldnames = ["Timestamp", "Time", "Location", "Minutes", "URL", "Is Past Meeting"]
        save_to_csv(meetings, meeting_csv, meeting_fieldnames, logger, error_tracker)

    logger.debug(f"Total meetings extracted: {len(meetings)}")
    return board_data, meetings, extracted_text

def main():
    """
    Run the board scraper standalone for debugging purposes, using config.yaml for board URL and settings.
    """
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger = setup_logging(config.get("debug", False))
    error_tracker = ErrorTracker()
    board_name = config.get("target_boards", ["Planning Board"])[0]
    # Use board_url from config.yaml, with a fallback
    board_url = config.get("board_url", "")
    if not board_url:
        logger.error("No board_url specified in config.yaml")
        return
    board_dir = os.path.join("Hardwick_Data", board_name.replace(' ', '_'))
    year = config.get("year")
    meeting_date = config.get("meeting_date")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,4000")
    driver = webdriver.Chrome(options=options)

    try:
        logger.info(f"Scraping board: {board_name} at {board_url}...")
        board_data, meetings, extracted_text = scrape_board(
            board_url, board_name, board_dir, logger, driver,
            force_redownload=config.get("force_redownload", False),
            error_tracker=error_tracker,
            year=year,
            meeting_date=meeting_date
        )
    finally:
        driver.quit()

    logger.info(f"Error Summary: Total Errors = {error_tracker.total_errors}, Total Warnings = {error_tracker.total_warnings}")
    with open(os.path.join("Hardwick_Data", "errors.log"), "w", encoding="utf-8") as f:
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

if __name__ == "__main__":
    main()