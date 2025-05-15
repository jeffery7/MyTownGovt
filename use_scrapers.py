#!/usr/bin/env python3

import yaml
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import subprocess
import time
from datetime import datetime
from dateutil.parser import parse as parse_date
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from mytowngov_common import sanitize_directory_name, sanitize_filename, generate_pdf_screenshot
import mytowngov_board_scraper
import mytowngov_meeting_scraper
import mytowngov_homepage_scraper
import csv
from error_tracker import ErrorTracker

def setup_logging(config):
    log_level = logging.DEBUG if config.get("debug", False) else logging.INFO
    os.makedirs("Hardwick_Data", exist_ok=True)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(os.path.join("Hardwick_Data", "scraper.log")),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('scraper')

def compress_pdf(input_pdf, output_pdf, logger, error_tracker):
    try:
        original_size = os.path.getsize(input_pdf)
        subprocess.run(
            ["gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4", "-dPDFSETTINGS=/screen", "-dNOPAUSE", "-dQUIET", "-dBATCH", f"-sOutputFile={output_pdf}", input_pdf],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if os.path.exists(output_pdf) and os.path.getsize(output_pdf) > 0:
            compressed_size = os.path.getsize(output_pdf)
            logger.info(f"Compressed PDF from {original_size} to {compressed_size} bytes: {output_pdf}")
            return True
        else:
            logger.warning(f"Compression produced empty or missing PDF: {output_pdf}")
            error_tracker.add_error('PDFCompressionError', f"Compression produced empty or missing PDF: {output_pdf}", input_pdf)
            return False
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error compressing PDF {input_pdf}: {e.stderr}")
        error_tracker.add_error('PDFCompressionError', f"Error compressing PDF: {e.stderr}", input_pdf)
        return False
    except FileNotFoundError:
        logger.error("Ghostscript not found in PATH, skipping compression")
        error_tracker.add_error('PDFCompressionError', "Ghostscript not found in PATH", input_pdf)
        return False

def check_meeting_exists(board_dir, timestamp):
    agenda_dir = os.path.join(board_dir, "Agendas")
    agenda_filename = f"Meeting_Agenda_Planning_Board_{timestamp.replace(':', '_').replace(' ', '_')}_Merged.pdf"
    agenda_path = os.path.join(agenda_dir, agenda_filename)
    return os.path.exists(agenda_path) and os.path.getsize(agenda_path) > 0

def load_meetings_from_homepage_csv(meetings_csv, target_board, target_date, logger):
    meetings = []
    if not os.path.exists(meetings_csv):
        logger.warning(f"Homepage meetings CSV not found: {meetings_csv}")
        return meetings
    try:
        with open(meetings_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['Board'] == target_board:
                    try:
                        meeting_date = parse_date(row['Timestamp']).date()
                        if target_date and meeting_date == target_date:
                            meetings.append({
                                "URL": row['URL'],
                                "Timestamp": row['Timestamp'],
                                "Time": row['Time'],
                                "Location": row['Location']
                            })
                    except ValueError as e:
                        logger.warning(f"Could not parse timestamp {row['Timestamp']} in {meetings_csv}: {e}")
    except Exception as e:
        logger.error(f"Error reading {meetings_csv}: {e}")
    logger.debug(f"Loaded {len(meetings)} meetings from {meetings_csv} for {target_board}")
    return meetings

def process_board(board, config, error_tracker):
    processed_meetings = 0
    skipped_meetings = 0
    board_name = board["Board"]
    board_url = board["URL"]
    logger = logging.getLogger('scraper')
    
    board_dir = os.path.join("Hardwick_Data", sanitize_directory_name(board_name))
    os.makedirs(board_dir, exist_ok=True)
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,4000")
    driver = webdriver.Chrome(options=options)
    
    local_meeting_data = []
    local_documents = []
    local_temp_files = []
    
    try:
        logger.info(f"Starting board processing: {board_url} ({board_name})...")
        board_data, meetings, extracted_text = mytowngov_board_scraper.scrape_board(
            board_url, board_name, board_dir, logger, driver,
            force_redownload=config["force_redownload"], error_tracker=error_tracker,
            year=config.get("year"), meeting_date=config.get("meeting_date")
        )
        
        # Save board screenshot
        pdf_path = os.path.join(board_dir, "board_screenshot.pdf")
        png_path = os.path.join(board_dir, "board_screenshot.png")
        generate_pdf_screenshot(board_url, pdf_path, driver, logger, error_tracker, extracted_text, save_png_path=png_path)
        logger.info(f"Generated board screenshot: PNG={png_path}, PDF={pdf_path}")
        
        # If no meetings found, try homepage meetings CSV
        if not meetings and config.get("meeting_date"):
            logger.info(f"No meetings found on board page, checking homepage_meetings.csv for {board_name}")
            target_date = parse_date(config["meeting_date"]).date()
            meetings = load_meetings_from_homepage_csv(
                "Hardwick_Data/homepage_meetings.csv", board_name, target_date, logger
            )

        if meetings:
            target_date = None
            if config.get("meeting_date"):
                try:
                    target_date = parse_date(config["meeting_date"]).date()
                except ValueError as e:
                    logger.error(f"Invalid meeting_date format in config: {e}")
                    error_tracker.add_error('ConfigError', f"Invalid meeting_date format: {e}", "config.yaml")
                    return processed_meetings, skipped_meetings, [], [], []
            
            filtered_meetings = []
            for meeting in meetings:
                try:
                    meeting_date = parse_date(meeting["Timestamp"]).date()
                    if target_date and meeting_date != target_date:
                        logger.debug(f"Skipping meeting {meeting['Timestamp']}: does not match {target_date}")
                        continue
                    filtered_meetings.append(meeting)
                except ValueError as e:
                    logger.warning(f"Could not parse meeting timestamp {meeting['Timestamp']}: {e}")
                    error_tracker.add_error('TimeParseError', f"Could not parse meeting timestamp: {e}", meeting["URL"])
            
            upcoming_meetings = [m for m in filtered_meetings if not m.get("Is Past Meeting", False)]
            past_meetings = [m for m in filtered_meetings if m.get("Is Past Meeting", False)]
            logger.info(f"Found {len(upcoming_meetings)} upcoming and {len(past_meetings)} past meetings for {board_name} after filtering")
            
            all_meetings = upcoming_meetings + past_meetings[:config.get("past_meeting_limit", len(past_meetings))]
            
            with ThreadPoolExecutor(max_workers=config.get("max_meeting_workers", 3)) as executor:
                future_to_meeting = {
                    executor.submit(
                        process_meeting, meeting["URL"], meeting["Timestamp"], board_dir, board_name, logger, config,
                        local_meeting_data, local_documents, local_temp_files, error_tracker
                    ): meeting for meeting in all_meetings
                }
                for future in as_completed(future_to_meeting):
                    try:
                        proc, skip = future.result()
                        processed_meetings += proc
                        skipped_meetings += skip
                    except Exception as e:
                        meeting = future_to_meeting[future]
                        error_tracker.add_error('MeetingProcessError', f"Error processing meeting {meeting['URL']}: {e}", meeting["URL"])
                        logger.error(f"Error processing meeting {meeting['URL']}: {e}")
        
        logger.info(f"Completed board processing: {board_name}")
    
    except Exception as e:
        error_tracker.add_error('BoardProcessError', f"Failed to process board {board_name}: {e}", board_url)
        logger.error(f"Failed to process board {board_name}: {e}")
    
    finally:
        driver.quit()
    
    return processed_meetings, skipped_meetings, local_meeting_data, local_documents, local_temp_files

def process_meeting(url, timestamp, board_dir, board_name, logger, config, local_meeting_data, local_documents, local_temp_files, error_tracker):
    processed_meetings = 0
    skipped_meetings = 0
    
    time.sleep(1)
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,4000")
    driver = webdriver.Chrome(options=options)
    
    try:
        if not config["force_redownload"] and check_meeting_exists(board_dir, timestamp):
            logger.info(f"Skipping meeting {url} (Merged PDF already exists)")
            skipped_meetings += 1
            return processed_meetings, skipped_meetings

        logger.info(f"Scraping meeting: {url}...")
        meeting_data, documents, agenda_pdf_paths, extracted_text = mytowngov_meeting_scraper.scrape_meeting(
            url, logger=logger, driver=driver, main_board_dir=board_dir, 
            force_redownload=config["force_redownload"], error_tracker=error_tracker,
            board_name=board_name, meeting_time=timestamp
        )

        if meeting_data:
            local_meeting_data.append(meeting_data)
            processed_meetings += 1

            # Generate meeting screenshot
            pdf_path = os.path.join(board_dir, f"meeting_screenshot_{sanitize_filename(timestamp)}.pdf")
            png_path = os.path.join(board_dir, f"meeting_screenshot_{sanitize_filename(timestamp)}.png")
            generate_pdf_screenshot(url, pdf_path, driver, logger, error_tracker, extracted_text, save_png_path=png_path)
            logger.info(f"Generated meeting screenshot for {timestamp}: PNG={png_path}, PDF={pdf_path}")

            final_agenda_pdf = None
            if agenda_pdf_paths:
                logger.info(f"Collected agenda PDFs: {agenda_pdf_paths}")
                agendas_dir = os.path.join(board_dir, "Agendas")
                os.makedirs(agendas_dir, exist_ok=True)
                final_agenda_pdf = os.path.join(agendas_dir, f"Meeting_Agenda_{sanitize_directory_name(board_name)}_{timestamp.replace(':', '_').replace(' ', '_')}_Merged.pdf")
                temp_agenda_pdf = os.path.join(agendas_dir, f"Temp_Meeting_Agenda_{sanitize_directory_name(board_name)}_{timestamp.replace(':', '_').replace(' ', '_')}_Merged.pdf")
                compressed_agenda_pdf = os.path.join(agendas_dir, f"Temp_Meeting_Agenda_{sanitize_directory_name(board_name)}_{timestamp.replace(':', '_').replace(' ', '_')}_Compressed.pdf")
                try:
                    valid_pdf_paths = [path for path in agenda_pdf_paths if os.path.exists(path) and os.path.getsize(path) > 0]
                    if not valid_pdf_paths:
                        error_msg = f"No valid PDFs to merge for meeting {timestamp}"
                        error_tracker.add_error('PDFMergeError', error_msg, url)
                        logger.warning(error_msg)
                        final_agenda_pdf = None
                    else:
                        subprocess.run(
                            ["pdftk"] + valid_pdf_paths + ["cat", "output", temp_agenda_pdf],
                            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
                        )
                        if compress_pdf(temp_agenda_pdf, compressed_agenda_pdf, logger, error_tracker):
                            shutil.move(compressed_agenda_pdf, final_agenda_pdf)
                        else:
                            shutil.move(temp_agenda_pdf, final_agenda_pdf)
                        logger.info(f"Merged {len(valid_pdf_paths)} agenda PDFs into {final_agenda_pdf}")
                        for pdf_path in valid_pdf_paths:
                            if pdf_path != final_agenda_pdf and os.path.exists(pdf_path):
                                try:
                                    os.remove(pdf_path)
                                    logger.info(f"Removed temporary PDF {pdf_path}")
                                    local_temp_files.append(pdf_path)
                                except Exception as e:
                                    error_tracker.add_error('FileCleanupError', f"Error removing temporary PDF {pdf_path}: {e}", url)
                                    logger.error(f"Error removing temporary PDF {pdf_path}: {e}")
                except subprocess.CalledProcessError as e:
                    error_tracker.add_error('PDFMergeError', f"Error merging agenda PDFs with pdftk: {e.stderr}", url)
                    logger.error(f"Error merging agenda PDFs with pdftk: {e.stderr}")
                    final_agenda_pdf = valid_pdf_paths[0] if valid_pdf_paths else None
                except Exception as e:
                    error_tracker.add_error('PDFMergeError', f"Error during agenda PDF merging: {e}", url)
                    logger.error(f"Error during agenda PDF merging: {e}")
                    final_agenda_pdf = valid_pdf_paths[0] if valid_pdf_paths else None
                finally:
                    for temp_file in [temp_agenda_pdf, compressed_agenda_pdf]:
                        if os.path.exists(temp_file):
                            try:
                                os.remove(temp_file)
                                local_temp_files.append(temp_file)
                            except Exception as e:
                                error_tracker.add_error('FileCleanupError', f"Error removing temporary file {temp_file}: {e}", url)
                                logger.error(f"Error removing temporary file {temp_file}: {e}")

            if documents:
                original_attachments_dir = os.path.join(board_dir, "Attachments")
                os.makedirs(original_attachments_dir, exist_ok=True)
                for doc in documents:
                    original_file_path = doc["File Path"]
                    if os.path.exists(original_file_path) and os.path.getsize(original_file_path) > 0:
                        new_file_path = os.path.join(original_attachments_dir, os.path.basename(original_file_path))
                        if original_file_path == new_file_path:
                            logger.info(f"Skipping copy of {os.path.basename(original_file_path)} (already in {original_attachments_dir})")
                            doc["File Path"] = new_file_path
                            continue
                        try:
                            shutil.copy2(original_file_path, new_file_path)
                            logger.info(f"Copied {os.path.basename(original_file_path)} to {original_attachments_dir}")
                            doc["File Path"] = new_file_path
                        except Exception as e:
                            error_tracker.add_error('FileCopyError', f"Error copying document {original_file_path} to {new_file_path}: {e}", url)
                            logger.error(f"Error copying document {original_file_path} to {new_file_path}: {e}")
                    else:
                        error_tracker.add_error('FileNotFoundError', f"Document file not found or empty at {original_file_path}", url)
                        logger.warning(f"Document file not found or empty at {original_file_path}")
                    if doc.get("PDF File Path"):
                        logger.info(f"Document {doc['File Name']} has PDF at {doc['PDF File Path']}")
                    else:
                        logger.warning(f"Document {doc['File Name']} has no PDF conversion")
                local_documents.extend(documents)
                logger.info(f"Added {len(documents)} documents for meeting at {timestamp}")

    except Exception as e:
        error_tracker.add_error('MeetingScrapeError', f"Failed to process meeting: {e}", url)
        logger.error(f"Failed to process meeting {url}: {e}")
    
    finally:
        driver.quit()
    
    return processed_meetings, skipped_meetings

def main():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    logger = setup_logging(config)
    error_tracker = ErrorTracker()
    
    logger.info(f"Scraping homepage: {config['homepage_url']}...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,4000")
    driver = webdriver.Chrome(options=options)
    
    try:
        boards_data, agencies_data, extracted_text = mytowngov_homepage_scraper.scrape_homepage(
            config["homepage_url"], logger=logger, driver=driver,
            error_tracker=error_tracker
        )
        
        # Save homepage screenshot
        pdf_path = os.path.join("Hardwick_Data", "homepage_screenshot.pdf")
        png_path = os.path.join("Hardwick_Data", "homepage_screenshot.png")
        generate_pdf_screenshot(config["homepage_url"], pdf_path, driver, logger, error_tracker, extracted_text, save_png_path=png_path)
        logger.info(f"Generated homepage screenshot: PNG={png_path}, PDF={pdf_path}")
        
        if not boards_data:
            logger.error("No boards found, exiting")
            return
        
        logger.info(f"Found {len(boards_data)} boards")
        target_boards = config.get("target_boards", [])
        if target_boards:
            boards_data = [b for b in boards_data if b["Board"] in target_boards]
            logger.debug(f"Filtered to {len(boards_data)} target boards: {target_boards}")
        
        if config.get("board_limit"):
            boards_data = boards_data[:config["board_limit"]]
        
        total_processed = 0
        total_skipped = 0
        all_meeting_data = []
        all_documents = []
        all_temp_files = []
        
        with ThreadPoolExecutor(max_workers=config.get("max_board_workers", 3)) as executor:
            future_to_board = {
                executor.submit(
                    process_board, board, config, error_tracker
                ): board for board in boards_data
            }
            for future in as_completed(future_to_board):
                try:
                    proc, skip, meetings, docs, temps = future.result()
                    total_processed += proc
                    total_skipped += skip
                    all_meeting_data.extend(meetings)
                    all_documents.extend(docs)
                    all_temp_files.extend(temps)
                except Exception as e:
                    board = future_to_board[future]
                    error_tracker.add_error('BoardProcessError', f"Error processing board {board['Board']}: {e}", board["URL"])
                    logger.error(f"Error processing board {board['Board']}: {e}")
        
        logger.info(f"Processed {total_processed} meetings, skipped {total_skipped}")
        
        if all_meeting_data:
            for meeting in all_meeting_data:
                board_dir = os.path.join("Hardwick_Data", sanitize_directory_name(meeting["Board"]))
                meeting_csv = os.path.join(board_dir, "meeting_data.csv")
                with open(meeting_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["Timestamp", "Town", "Board", "Time", "Location", "Agenda", "Document Count", "URL"])
                    if os.path.getsize(meeting_csv) == 0:
                        writer.writeheader()
                    writer.writerow(meeting)
                logger.info(f"Saved meeting to {meeting_csv}")
        
        if all_documents:
            for doc in all_documents:
                board_dir = os.path.join("Hardwick_Data", sanitize_directory_name(doc["Board"]))
                doc_csv = os.path.join(board_dir, "meeting_documents.csv")
                with open(doc_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=["Board", "Timestamp", "File Name", "Download URL", "File Path", "PDF File Path"])
                    if os.path.getsize(doc_csv) == 0:
                        writer.writeheader()
                    writer.writerow(doc)
                logger.info(f"Saved document to {doc_csv}")
        
        validation_issues = []
        for meeting in all_meeting_data:
            board_dir = os.path.join("Hardwick_Data", sanitize_directory_name(meeting["Board"]))
            agenda_path = os.path.join(board_dir, "Agendas", f"Meeting_Agenda_{sanitize_directory_name(meeting['Board'])}_{sanitize_filename(meeting['Timestamp'])}_Merged.pdf")
            if meeting["Agenda"] != "No agenda available" and (not os.path.exists(agenda_path) or os.path.getsize(agenda_path) == 0):
                error_msg = f"Missing or empty agenda PDF for meeting {meeting['Timestamp']} at {agenda_path}"
                validation_issues.append(error_msg)
                error_tracker.add_error('ValidationError', error_msg, meeting["URL"])
                logger.warning(error_msg)
        
        for doc in all_documents:
            if not doc.get("PDF File Path") and doc["File Path"].lower().endswith(('.doc', '.docx')):
                error_msg = f"Missing PDF conversion for document: {doc['File Name']} at {doc['File Path']}"
                validation_issues.append(error_msg)
                error_tracker.add_error('ValidationError', error_msg, doc["Download URL"], is_warning=True)
                logger.warning(error_msg)
        
        validation_report_path = os.path.join("Hardwick_Data", "Planning_Board_validation_report.txt")
        with open(validation_report_path, "w", encoding="utf-8") as f:
            f.write("Data Validation Report\n=====================\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Board: Planning Board\n")
            f.write(f"Total Meetings: {len(all_meeting_data)}\n")
            f.write(f"Total Documents: {len(all_documents)}\n")
            f.write(f"Validation Issues: {len(validation_issues)}\n")
            if validation_issues:
                for issue in validation_issues:
                    f.write(f"- {issue}\n")
            else:
                f.write("No validation issues found.\n")
            f.write("=====================\n")
        logger.info(f"Saved validation report to {validation_report_path}")
        logger.info(f"Validation Summary: Total Meetings = {len(all_meeting_data)}, Total Documents = {len(all_documents)}, Validation Issues = {len(validation_issues)}")
        
        metadata = {"last_scrape": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        with open(os.path.join("Hardwick_Data", "scrape_metadata.yaml"), "w") as f:
            yaml.dump(metadata, f)
        logger.info(f"Saved scrape metadata to Hardwick_Data/scrape_metadata.yaml")
    
    finally:
        driver.quit()
    
    logger.info(f"Error Summary: Total Errors = {error_tracker.total_errors}, Total Warnings = {error_tracker.total_warnings}")
    logger.info("Errors/Warnings by Type:")
    for error_type, count in error_tracker.error_counts.items():
        logger.info(f"  {error_type}: {count}")
    
    error_log_path = os.path.join("Hardwick_Data", "errors.log")
    with open(error_log_path, "w", encoding="utf-8") as f:
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
    logger.info(f"Saved error log to {error_log_path}")

if __name__ == "__main__":
    main()