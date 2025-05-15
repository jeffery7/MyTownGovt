#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import logging
import os
import subprocess
import re
import time
import csv
from urllib.parse import urljoin
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from mytowngov_common import fetch_page_content, sanitize_directory_name, sanitize_filename, parse_time_to_timestamp, setup_logging, save_to_csv, generate_pdf_screenshot, load_config
from error_tracker import ErrorTracker

def get_meeting_urls(board_dir, target_date, logger, error_tracker):
    """
    Read meeting URLs from board_meeting_data.csv, optionally filtering by date.
    
    Args:
        board_dir (str): Directory containing board_meeting_data.csv (e.g., Hardwick_Data/Planning_Board).
        target_date (str): Date to match (e.g., "Mar 11, 2025"), or None for all meetings.
        logger (logging.Logger): Logger instance.
        error_tracker (ErrorTracker): Error tracking instance.
    
    Returns:
        list: List of (URL, Time) tuples for matching meetings, or empty list if none found.
    """
    csv_path = os.path.join(board_dir, "board_meeting_data.csv")
    
    if not os.path.exists(csv_path):
        logger.error(f"Meeting CSV file not found: {csv_path}")
        error_tracker.add_error('FileNotFoundError', f"Meeting CSV file not found: {csv_path}", csv_path)
        return []
    
    meetings = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                meeting_time = row.get('Time', '')
                url = row.get('URL', '')
                if not url:
                    logger.warning(f"Meeting at {meeting_time} found in CSV but URL is empty")
                    error_tracker.add_error('DataError', f"Meeting URL is empty for {meeting_time} in CSV", csv_path, is_warning=True)
                    continue
                if target_date and target_date.lower() not in meeting_time.lower():
                    continue
                meetings.append((url, meeting_time))
                logger.debug(f"Found meeting for {meeting_time} with URL: {url}")
        if not meetings:
            logger.error(f"No meetings found in {csv_path}" + (f" for date {target_date}" if target_date else ""))
            error_tracker.add_error('DataError', f"No meetings found in CSV" + (f" for date {target_date}" if target_date else ""), csv_path)
        else:
            logger.info(f"Found {len(meetings)} meetings in {csv_path}" + (f" for date {target_date}" if target_date else ""))
        return meetings
    except Exception as e:
        logger.error(f"Error reading CSV {csv_path}: {e}")
        error_tracker.add_error('CSVReadError', f"Error reading CSV: {e}", csv_path)
        return []

def scrape_meeting(meeting_url, logger=None, driver=None, main_board_dir="Hardwick_Data", force_redownload=False, error_tracker=None, board_name=None, meeting_time=None):
    if not logger:
        logger = logging.getLogger('scraper')
    
    html = fetch_page_content(meeting_url, driver, logger, force_redownload, error_tracker, full_page=True)
    if not html:
        logger.error(f"Failed to fetch page content for {meeting_url}")
        return None, [], [], []
    
    # Save HTML for debugging
    debug_html_path = os.path.join(main_board_dir, f"meeting_debug_{sanitize_filename(meeting_url.split('=')[-1])}.html")
    with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.debug(f"Saved meeting page HTML for debugging: {debug_html_path}")
    
    soup = BeautifulSoup(html, 'html.parser')
    
    meeting_data = {
        "Timestamp": "",
        "Town": "Hardwick",
        "Board": board_name or "Unknown",
        "Time": meeting_time or "Unknown",
        "Location": "Not specified",
        "Agenda": "No agenda available",
        "Document Count": 0,
        "URL": meeting_url
    }
    
    documents = []
    agenda_pdf_paths = []
    extracted_text = []
    
    try:
        logger.info("Searching for fields...")
        
        # Extract fields from the iframe's table
        iframe_soup = soup
        iframe = soup.find('iframe', {'name': 'content'})
        iframe_html = html
        if iframe and iframe.get('src'):
            # Fetch iframe content separately
            iframe_url = urljoin("https://www.mytowngovernment.org", iframe['src'])
            attempts = 0
            max_attempts = 3
            while attempts < max_attempts:
                try:
                    driver.get(iframe_url)
                    time.sleep(3)  # Allow dynamic content to load
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)  # Ensure scroll completes
                    iframe_html = driver.page_source
                    if iframe_html:
                        iframe_soup = BeautifulSoup(iframe_html, 'html.parser')
                        logger.debug("Successfully fetched iframe content")
                        break
                    logger.warning(f"Failed to fetch iframe content for {iframe_url}, attempt {attempts + 1}/{max_attempts}")
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"Failed to fetch iframe content after {max_attempts} attempts, using main page content")
                        error_tracker.add_error('IframeFetchError', f"Failed to fetch iframe content after {max_attempts} attempts", iframe_url)
                except Exception as e:
                    logger.warning(f"Error fetching iframe content: {e}, attempt {attempts + 1}/{max_attempts}")
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"Failed to fetch iframe content after {max_attempts} attempts: {e}")
                        error_tracker.add_error('IframeFetchError', f"Failed to fetch iframe content: {e}", iframe_url)
        else:
            logger.debug("No iframe found, using main page content for field extraction")
        
        # Debug iframe content
        logger.debug(f"Iframe HTML snippet: {str(iframe_soup)[:1000]}")
        
        # Extract fields from table
        info_table = iframe_soup.find('table')
        if info_table:
            logger.debug("Found info table, extracting fields...")
            rows = info_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    logger.debug(f"Table row: {label} = {value}")
                    extracted_text.append(f"{label}: {value}")
                    
                    if 'town:' in label:
                        meeting_data["Town"] = "Hardwick" if "Hardwick" in value else "Hardwick"
                        logger.debug(f"Found Town: {meeting_data['Town']}")
                    elif 'board:' in label:
                        if board_name and board_name.lower() in value.lower():
                            meeting_data["Board"] = board_name
                            logger.debug(f"Confirmed Board: {board_name}")
                        else:
                            meeting_data["Board"] = value or board_name or "Unknown"
                            logger.debug(f"Found Board: {meeting_data['Board']}")
                    elif 'time:' in label:
                        meeting_data["Time"] = value or meeting_time or "Unknown"
                        logger.debug(f"Found Time: {meeting_data['Time']}")
                    elif 'location:' in label:
                        meeting_data["Location"] = value or "Not specified"
                        logger.debug(f"Found Location: {meeting_data['Location']}")
                    elif 'agenda:' in label:
                        agenda_content = cells[1].find('pre') or cells[1].find('div') or cells[1]
                        agenda_text = agenda_content.get_text(strip=True) or value
                        meeting_data["Agenda"] = agenda_text if agenda_text else "No agenda available"
                        logger.debug(f"Found Agenda: {meeting_data['Agenda'][:200]}...")
                        extracted_text.append(meeting_data["Agenda"])
        else:
            logger.warning("No info table found, using defaults")
        
        # Parse timestamp using meeting_time first
        timestamp = parse_time_to_timestamp(meeting_time or meeting_data["Time"], logger, error_tracker)
        if timestamp:
            meeting_data["Timestamp"] = timestamp
            logger.debug(f"Parsed Timestamp: {timestamp}")
        else:
            meeting_data["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(f"Failed to parse time '{meeting_time or meeting_data['Time']}', using current time: {meeting_data['Timestamp']}")
        
        # Generate PDF from iframe HTML
        agenda_dir = os.path.join(main_board_dir, "Agendas")
        os.makedirs(agenda_dir, exist_ok=True)
        agenda_filename = f"Meeting_Agenda_{sanitize_directory_name(meeting_data['Board'])}_{sanitize_filename(meeting_data['Timestamp'])}.pdf"
        merged_agenda_filename = f"Meeting_Agenda_{sanitize_directory_name(meeting_data['Board'])}_{sanitize_filename(meeting_data['Timestamp'])}_Merged.pdf"
        agenda_path = os.path.join(agenda_dir, agenda_filename)
        merged_agenda_path = os.path.join(agenda_dir, merged_agenda_filename)
        temp_html_path = os.path.join(agenda_dir, f"temp_agenda_{sanitize_filename(meeting_data['Timestamp'])}.html")
        
        # Preprocess iframe HTML for PDF generation
        logger.debug("Preprocessing iframe HTML for PDF generation")
        modified_soup = BeautifulSoup(iframe_html, 'html.parser')
        for nav in modified_soup.find_all(['div', 'table'], class_=re.compile(r'nav|menu|header|sidebar', re.IGNORECASE)):
            nav.decompose()
        for tag in modified_soup.find_all(['link', 'script', 'img']):
            if tag.get('href'):
                tag['href'] = urljoin("https://www.mytowngovernment.org", tag['href'])
            if tag.get('src'):
                tag['src'] = urljoin("https://www.mytowngovernment.org", tag['src'])
        for script in modified_soup.find_all('script'):
            if script.string and "window.location.href=\"/01031?content=\"" in script.string:
                script.decompose()
                logger.debug("Removed redirect script from HTML")
        
        logger.info(f"Generating PDF from iframe content: {agenda_path}...")
        with open(temp_html_path, "w", encoding="utf-8") as f:
            f.write(str(modified_soup))
        
        try:
            subprocess.run(
                [
                    "wkhtmltopdf",
                    "--enable-local-file-access",
                    "--load-error-handling", "ignore",
                    "--enable-javascript",
                    "--javascript-delay", "15000",
                    "--no-stop-slow-scripts",
                    "--no-outline",
                    "--disable-smart-shrinking",
                    "--page-size", "Letter",
                    "--margin-top", "5mm",
                    "--margin-bottom", "5mm",
                    "--margin-left", "5mm",
                    "--margin-right", "5mm",
                    "--zoom", "0.75",
                    temp_html_path,
                    agenda_path
                ],
                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60
            )
            if os.path.exists(agenda_path) and os.path.getsize(agenda_path) > 0:
                agenda_pdf_paths.append(agenda_path)
                logger.info(f"PDF generated successfully: {agenda_path}")
            else:
                logger.error(f"Failed to generate PDF or PDF is empty: {agenda_path}")
                error_tracker.add_error('PDFGenerationError', f"Failed to generate PDF or PDF is empty: {agenda_path}", meeting_url)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating PDF with wkhtmltopdf: {e.stderr}")
            error_tracker.add_error('PDFGenerationError', f"Error generating PDF with wkhtmltopdf: {e.stderr}", meeting_url)
        except FileNotFoundError:
            logger.error("wkhtmltopdf not found in PATH")
            error_tracker.add_error('PDFGenerationError', "wkhtmltopdf not found in PATH", meeting_url)
        except subprocess.TimeoutExpired as e:
            logger.error(f"wkhtmltopdf timed out: {e.stderr}")
            error_tracker.add_error('PDFGenerationError', f"wkhtmltopdf timed out: {e.stderr}", meeting_url)
        finally:
            if os.path.exists(temp_html_path):
                os.remove(temp_html_path)
                logger.debug(f"Removed temporary HTML file: {temp_html_path}")
        
        # Merge PDFs (copy single PDF to merged path)
        if agenda_pdf_paths:
            logger.info(f"Copying PDF to merged path: {merged_agenda_path}")
            try:
                subprocess.run(
                    ["cp", agenda_path, merged_agenda_path],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30
                )
                if os.path.exists(merged_agenda_path) and os.path.getsize(merged_agenda_path) > 0:
                    logger.info(f"Merged PDF created: {merged_agenda_path}")
                else:
                    logger.error(f"Failed to create merged PDF: {merged_agenda_path}")
                    error_tracker.add_error('PDFMergeError', f"Failed to create merged PDF: {merged_agenda_path}", meeting_url)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error copying PDF to merged path: {e.stderr}")
                error_tracker.add_error('PDFMergeError', f"Error copying PDF to merged path: {e.stderr}", meeting_url)
        
        # Extract documents
        logger.info("Searching for documents section...")
        documents_section = (
            iframe_soup.find('h2', text=re.compile(r'Minutes and Associated Documents|Documents|Attachments|Files|Docs', re.IGNORECASE)) or
            iframe_soup.find(['h2', 'h3'], text=re.compile(r'Minutes and Associated Documents|Documents|Attachments|Files|Docs', re.IGNORECASE)) or
            iframe_soup.find(['div', 'section'], class_=re.compile(r'documents|attachments|files|docs', re.IGNORECASE))
        )
        if not documents_section:
            logger.debug(f"No documents section found in iframe, trying main page")
            documents_section = (
                soup.find('h2', text=re.compile(r'Minutes and Associated Documents|Documents|Attachments|Files|Docs', re.IGNORECASE)) or
                soup.find(['h2', 'h3'], text=re.compile(r'Minutes and Associated Documents|Documents|Attachments|Files|Docs', re.IGNORECASE)) or
                soup.find(['div', 'section'], class_=re.compile(r'documents|attachments|files|docs', re.IGNORECASE))
            )
        if documents_section:
            documents_table = documents_section.find_next('table')
            if documents_table:
                rows = documents_table.find_all('tr')[1:]  # Skip header
                logger.debug(f"Found {len(rows)} document rows")
                for row in rows:
                    cells = row.find_all('td')
                    logger.debug(f"Document row cells: {[cell.get_text(strip=True) for cell in cells]}")
                    if len(cells) >= 1:  # Relaxed to handle varying table structures
                        doc_name = cells[0].get_text(strip=True).replace('(download)', '').strip()
                        doc_link = cells[0].find('a', href=True)
                        if doc_link:
                            doc_url = urljoin("https://www.mytowngovernment.org", doc_link['href'])
                            doc_filename = sanitize_filename(doc_name)
                            doc_path = os.path.join(main_board_dir, "Attachments", doc_filename)
                            os.makedirs(os.path.dirname(doc_path), exist_ok=True)
                            
                            logger.debug(f"Attempting to download document: {doc_name} from {doc_url}")
                            attempts = 0
                            max_attempts = 3
                            headers = {
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                            }
                            while attempts < max_attempts:
                                try:
                                    response = requests.get(doc_url, stream=True, timeout=15, headers=headers, allow_redirects=True)
                                    if response.status_code == 200:
                                        with open(doc_path, 'wb') as f:
                                            for chunk in response.iter_content(chunk_size=8192):
                                                if chunk:
                                                    f.write(chunk)
                                        if os.path.getsize(doc_path) > 0:
                                            logger.info(f"Downloaded {doc_name} to {doc_path}")
                                            break
                                        else:
                                            logger.warning(f"Downloaded file is empty: {doc_name}")
                                            attempts += 1
                                    else:
                                        logger.warning(f"Failed to download {doc_name} (Status: {response.status_code})")
                                        attempts += 1
                                        if attempts == max_attempts:
                                            error_tracker.add_error('FileDownloadError', f"Failed to download document after {max_attempts} attempts: {doc_name}", doc_url, retry_count=attempts)
                                except Exception as e:
                                    logger.error(f"Error downloading {doc_name}: {e}")
                                    attempts += 1
                                    if attempts == max_attempts:
                                        error_tracker.add_error('FileDownloadError', f"Error downloading document after {max_attempts} attempts: {e}", doc_url, retry_count=attempts)
                                    time.sleep(1)
                            
                            pdf_path = None
                            if os.path.exists(doc_path) and os.path.getsize(doc_path) > 0:
                                if doc_path.lower().endswith(('.doc', '.docx')):
                                    pdf_filename = doc_filename.rsplit('.', 1)[0] + '.pdf'
                                    pdf_path = os.path.join(main_board_dir, "Attachments", pdf_filename)
                                    if not os.path.exists(pdf_path) or force_redownload:
                                        try:
                                            result = subprocess.run(
                                                ["libreoffice", "--headless", "--convert-to", "pdf", "--outdir", os.path.dirname(pdf_path), doc_path],
                                                check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60
                                            )
                                            generated_pdf = os.path.join(os.path.dirname(pdf_path), os.path.basename(pdf_filename))
                                            if os.path.exists(generated_pdf) and os.path.getsize(generated_pdf) > 0:
                                                os.rename(generated_pdf, pdf_path)
                                                logger.info(f"Converted {doc_filename} to PDF at {pdf_path}")
                                            else:
                                                logger.warning(f"LibreOffice conversion produced empty or missing PDF for {doc_filename}")
                                                error_tracker.add_error('DocumentConversionError', f"LibreOffice conversion produced empty or missing PDF: {doc_filename}", doc_url, is_warning=True)
                                                pdf_path = None
                                        except subprocess.CalledProcessError as e:
                                            logger.warning(f"Error converting {doc_filename} to PDF with LibreOffice: {e.stderr}")
                                            error_tracker.add_error('DocumentConversionError', f"Error converting {doc_filename} to PDF with LibreOffice: {e.stderr}", doc_url, is_warning=True)
                                            pdf_path = None
                                        except FileNotFoundError:
                                            logger.error("LibreOffice not found in PATH, skipping conversion")
                                            error_tracker.add_error('DocumentConversionError', "LibreOffice not found in PATH", doc_url, is_warning=True)
                                            pdf_path = None
                                        except subprocess.TimeoutExpired as e:
                                            logger.warning(f"LibreOffice conversion timed out for {doc_filename}: {e.stderr}")
                                            error_tracker.add_error('DocumentConversionError', f"LibreOffice conversion timed out: {e.stderr}", doc_url, is_warning=True)
                                            pdf_path = None
                                    else:
                                        logger.info(f"Using existing PDF {pdf_path} for {doc_filename}")
                                else:
                                    pdf_path = doc_path if doc_path.lower().endswith('.pdf') else None
                            
                            documents.append({
                                "Board": meeting_data["Board"],
                                "Timestamp": meeting_data["Timestamp"],
                                "File Name": doc_filename,
                                "Download URL": doc_url,
                                "File Path": doc_path,
                                "PDF File Path": pdf_path
                            })
                            meeting_data["Document Count"] += 1
                            logger.debug(f"Added document: {doc_filename}, URL: {doc_url}")
                            extracted_text.append(f"Document: {doc_filename}")
            else:
                logger.debug("No documents table found")
        else:
            logger.debug(f"No documents section found in iframe or main page")
        
        # Extract related meetings
        logger.info("Searching for related meetings section...")
        related_section = (
            iframe_soup.find('h2', text=re.compile(r'Possibly Related or Conflicting Meetings|Related|Associated|Other Meetings|Linked', re.IGNORECASE)) or
            iframe_soup.find(['h2', 'h3'], text=re.compile(r'Possibly Related or Conflicting Meetings|Related|Associated|Other Meetings|Linked', re.IGNORECASE)) or
            iframe_soup.find(['div', 'section'], class_=re.compile(r'related|meetings|associated|other|linked', re.IGNORECASE))
        )
        if not related_section:
            logger.debug(f"No related meetings section found in iframe")
            related_section = (
                soup.find('h2', text=re.compile(r'Possibly Related or Conflicting Meetings|Related|Associated|Other Meetings|Linked', re.IGNORECASE)) or
                soup.find(['h2', 'h3'], text=re.compile(r'Possibly Related or Conflicting Meetings|Related|Associated|Other Meetings|Linked', re.IGNORECASE)) or
                soup.find(['div', 'section'], class_=re.compile(r'related|meetings|associated|other|linked', re.IGNORECASE))
            )
        if related_section:
            related_table = related_section.find_next('table')
            if related_table:
                rows = related_table.find_all('tr')[1:]  # Skip header
                logger.debug(f"Found related meetings table with {len(rows)} rows")
                for row in rows:
                    cells = row.find_all('td')
                    logger.debug(f"Related meeting row cells: {[cell.get_text(strip=True) for cell in cells]}")
                    if len(cells) >= 3:  # Adjusted for varying structures
                        related_board = cells[0].get_text(strip=True)
                        related_time = cells[1].get_text(strip=True)
                        details_link = cells[-1].find('a', href=re.compile(r'meeting', re.IGNORECASE))
                        if details_link:
                            related_url = urljoin("https://www.mytowngovernment.org", details_link['href'])
                            logger.info(f"Processing related meeting: {related_url} for {related_board} at {related_time}")
                            try:
                                related_data, related_docs, related_pdfs, related_text = scrape_meeting(
                                    related_url, logger=logger, driver=driver, main_board_dir=main_board_dir,
                                    force_redownload=force_redownload, error_tracker=error_tracker,
                                    board_name=related_board, meeting_time=related_time
                                )
                                if related_data and related_pdfs:
                                    agenda_pdf_paths.extend(related_pdfs)
                                    documents.extend(related_docs)
                                    extracted_text.extend(related_text)
                                    meeting_data["Document Count"] += len(related_docs)
                                    logger.debug(f"Added {len(related_pdfs)} PDFs and {len(related_docs)} documents from related meeting {related_board} at {related_time}")
                                    extracted_text.append(f"Related Meeting: {related_board} at {related_time}")
                            except Exception as e:
                                logger.error(f"Error processing related meeting {related_url}: {e}")
                                error_tracker.add_error('RelatedMeetingError', f"Error processing related meeting: {e}", related_url)
            else:
                logger.debug("No related meetings table found")
        else:
            logger.debug(f"No related meetings section found in iframe or main page")
        
        logger.info(f"Collected agenda PDFs: {agenda_pdf_paths}")
        return meeting_data, documents, agenda_pdf_paths, extracted_text
    
    except Exception as e:
        if error_tracker:
            error_tracker.add_error('MeetingScrapeError', f"Error extracting meeting data: {e}", meeting_url)
        logger.error(f"Error extracting meeting data for {meeting_url}: {e}")
        logger.debug(f"HTML snippet: {str(soup)[:1000]}")
        return None, [], [], []

def main():
    # Load configuration
    config = load_config("config.yaml")
    
    # Setup logging
    logger = setup_logging(config.get("debug", False))
    error_tracker = ErrorTracker()
    
    # Get configuration values
    target_board = config.get("target_boards", ["Planning Board"])[0] if config.get("target_boards") else "Planning Board"
    meeting_date = config.get("meeting_date")
    
    # Set up board directory
    board_dir = os.path.join("Hardwick_Data", sanitize_directory_name(target_board))
    
    # Get meeting URLs from board_meeting_data.csv
    meeting_urls = get_meeting_urls(board_dir, meeting_date, logger, error_tracker)
    if not meeting_urls:
        logger.error(f"No valid meeting URLs found for {target_board}" + (f" on {meeting_date}" if meeting_date else ""))
        return
    
    # Initialize WebDriver
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")  # Reduced height to limit PDF size
    driver = webdriver.Chrome(options=options)
    
    try:
        meeting_data_list = []
        all_documents = []
        for meeting_url, meeting_time in meeting_urls:
            logger.info(f"Scraping meeting: {meeting_url} for {target_board} at {meeting_time}")
            meeting_data, documents, agenda_pdf_paths, extracted_text = scrape_meeting(
                meeting_url=meeting_url,
                logger=logger,
                driver=driver,
                main_board_dir=board_dir,
                force_redownload=config.get("force_redownload", False),
                error_tracker=error_tracker,
                board_name=target_board,
                meeting_time=meeting_time
            )
            
            # Collect meeting data
            if meeting_data:
                meeting_data_list.append(meeting_data)
            
            # Collect documents
            if documents:
                all_documents.extend(documents)
            
            # Generate PDF screenshot
            pdf_path = os.path.join(board_dir, f"meeting_screenshot_{sanitize_filename(meeting_data['Timestamp'] if meeting_data else meeting_time)}.pdf")
            generate_pdf_screenshot(meeting_url, pdf_path, driver, logger, error_tracker, extracted_text)
        
        # Save meeting data
        if meeting_data_list:
            meeting_fieldnames = [
                "Timestamp", "Town", "Board", "Time", "Location", "Agenda",
                "Document Count", "URL"
            ]
            meeting_csv = os.path.join(board_dir, "meeting_data.csv")
            save_to_csv(meeting_data_list, meeting_csv, meeting_fieldnames, logger, error_tracker)
        
        # Save document data
        if all_documents:
            document_fieldnames = [
                "Board", "Timestamp", "File Name", "Download URL", "File Path", "PDF File Path"
            ]
            document_csv = os.path.join(board_dir, "meeting_documents.csv")
            save_to_csv(all_documents, document_csv, document_fieldnames, logger, error_tracker)
        
        # Log error summary
        logger.info(f"Error Summary: Total Errors = {error_tracker.total_errors}, Total Warnings = {error_tracker.total_warnings}")
        error_log_path = os.path.join(board_dir, "errors.log")
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write("Meeting Scraper Error Log\n=================\n")
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
        logger.info(f"Saved error log to {error_log_path}")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()