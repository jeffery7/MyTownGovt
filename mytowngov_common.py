import requests
from bs4 import BeautifulSoup
import logging
import os
import subprocess
import re
import time
from urllib.parse import urljoin
from datetime import datetime
from PIL import Image
import img2pdf
import PyPDF2
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import tempfile
import yaml
import dateutil.parser
import hashlib
import io
import csv
import shutil

def fetch_page_content(url, driver, logger, force_redownload=False, error_tracker=None, full_page=False):
    """
    Fetch page content using Selenium WebDriver.
    
    Args:
        url (str): URL to fetch.
        driver: Selenium WebDriver instance.
        logger: Logger instance.
        force_redownload (bool): Force fetching even if cached.
        error_tracker: ErrorTracker instance.
        full_page (bool): Capture full page content.
    
    Returns:
        str: HTML content or None if failed.
    """
    logger.info(f"Fetching page: {url}")
    cache_dir = "Hardwick_Data/cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_filename = os.path.join(cache_dir, f"{hashlib.md5(url.encode()).hexdigest()}.html")
    
    if not force_redownload and os.path.exists(cache_filename):
        try:
            with open(cache_filename, 'r', encoding='utf-8') as f:
                logger.debug(f"Using cached content for {url}")
                return f.read()
        except Exception as e:
            logger.warning(f"Error reading cache for {url}: {e}")
    
    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        try:
            driver.get(url)
            time.sleep(3)  # Allow page to load
            if full_page:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Ensure scroll completes
            html = driver.page_source
            if html:
                try:
                    with open(cache_filename, 'w', encoding='utf-8') as f:
                        f.write(html)
                    logger.debug(f"Cached content for {url}")
                except Exception as e:
                    logger.warning(f"Error caching content for {url}: {e}")
                return html
            logger.warning(f"Empty content fetched for {url}, attempt {attempts + 1}/{max_attempts}")
            attempts += 1
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}, attempt {attempts + 1}/{max_attempts}")
            attempts += 1
            time.sleep(1)
    
    logger.error(f"Failed to fetch {url} after {max_attempts} attempts")
    if error_tracker:
        error_tracker.add_error('FetchError', f"Failed to fetch page after {max_attempts} attempts", url, retry_count=attempts)
    return None

def sanitize_directory_name(name):
    """
    Sanitize a string to be safe for use as a directory name.
    
    Args:
        name (str): Input string.
    
    Returns:
        str: Sanitized string.
    """
    if not name:
        return "Unknown"
    name = re.sub(r'[^\w\s-]', '', name.strip())
    name = re.sub(r'\s+', '_', name)
    return name or "Unknown"

def sanitize_filename(name):
    """
    Sanitize a string to be safe for use as a filename.
    
    Args:
        name (str): Input string.
    
    Returns:
        str: Sanitized string.
    """
    if not name:
        return "unknown"
    name = re.sub(r'[^\w\s-]', '', name.strip())
    name = re.sub(r'\s+', '_', name)
    return name or "unknown"

def parse_time_to_timestamp(time_str, logger, error_tracker=None):
    """
    Parse a time string to a timestamp.
    
    Args:
        time_str (str): Time string to parse.
        logger: Logger instance.
        error_tracker: ErrorTracker instance.
    
    Returns:
        str: Formatted timestamp (YYYY-MM-DD HH:MM:SS) or None if parsing fails.
    """
    if not time_str or time_str.strip() == '':
        logger.warning("Empty time string provided")
        if error_tracker:
            error_tracker.add_error('TimeParseError', "Empty time string", time_str)
        return None
    try:
        time_str = re.sub(r'\s+', ' ', time_str.strip())
        match = re.match(r'(\w+\s+\d{1,2},\s+\d{4})(?:,?\s*(\d{1,2}:\d{2}\s*[AP]M\s*[A-Z]+)?)?', time_str)
        if match:
            date_part, time_part = match.groups()
            time_str = f"{date_part} {time_part}" if time_part else f"{date_part} 00:00:00"
        parsed_time = dateutil.parser.parse(time_str, fuzzy=True)
        timestamp = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
        logger.debug(f"Parsed '{time_str}' to {timestamp}")
        return timestamp
    except ValueError as e:
        logger.error(f"Error parsing time '{time_str}': {e}")
        if error_tracker:
            error_tracker.add_error('TimeParseError', f"Error parsing time: {e}", time_str)
        return None

def setup_logging(debug=False):
    """
    Set up logging configuration.
    
    Args:
        debug (bool): Enable debug logging.
    
    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger('scraper')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)
    
    os.makedirs("Hardwick_Data", exist_ok=True)
    file_handler = logging.FileHandler("Hardwick_Data/scraper.log", encoding='utf-8')
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(file_formatter)
    
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger

def save_to_csv(data, output_file, fieldnames, logger, error_tracker):
    """
    Save data to a CSV file.
    
    Args:
        data (list): List of dictionaries to save.
        output_file (str): Output CSV file path.
        fieldnames (list): List of field names for CSV headers.
        logger: Logger instance.
        error_tracker: ErrorTracker instance.
    """
    try:
        mode = 'a' if os.path.exists(output_file) else 'w'
        with open(output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            if mode == 'w':
                writer.writeheader()
            writer.writerows(data)
        logger.info(f"Saved {len(data)} rows to {output_file}")
    except Exception as e:
        logger.error(f"Error saving CSV {output_file}: {e}")
        if error_tracker:
            error_tracker.add_error('CSVWriteError', f"Error saving CSV: {e}", output_file)

def generate_pdf_screenshot(url, output_path, driver, logger, error_tracker, extracted_text, save_png_path=None):
    """
    Generate a PDF screenshot of a webpage with a searchable text layer and optional PNG output.
    
    Args:
        url (str): URL to capture.
        output_path (str): Output PDF path.
        driver: Selenium WebDriver instance.
        logger: Logger instance.
        error_tracker: ErrorTracker instance.
        extracted_text (list): List of text strings for searchable layer.
        save_png_path (str, optional): Path to save PNG screenshot.
    """
    logger.info(f"Generating PDF screenshot for {url} at {output_path}")
    try:
        driver.get(url)
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, 0);")
        
        viewport_height = 1080
        driver.set_window_size(1920, viewport_height)
        
        screenshot_path = output_path.replace('.pdf', '.png')
        driver.save_screenshot(screenshot_path)
        logger.debug(f"Saved screenshot: {screenshot_path}")
        
        img = Image.open(screenshot_path)
        width, height = img.size
        cropped_img = img.crop((0, 100, width, min(height, viewport_height)))
        cropped_path = screenshot_path.replace('.pdf', '_cropped.png')
        cropped_img.save(cropped_path)
        logger.debug(f"Cropped screenshot to {cropped_path}")
        
        if save_png_path and save_png_path != cropped_path:
            shutil.copy2(cropped_path, save_png_path)
            logger.info(f"Saved PNG screenshot to {save_png_path}")
        elif save_png_path:
            logger.debug(f"Skipped copying PNG as cropped_path and save_png_path are the same: {save_png_path}")
        
        pdf_path = output_path.replace('.pdf', '.image_temp.pdf')
        with open(pdf_path, 'wb') as f:
            f.write(img2pdf.convert(cropped_path))
        logger.debug(f"Converted screenshot to PDF: {pdf_path}")
        
        text_pdf_path = output_path.replace('.pdf', '.text_temp.pdf')
        c = canvas.Canvas(text_pdf_path, pagesize=letter)
        y_position = 800
        for text in extracted_text:
            if y_position < 50:
                c.showPage()
                y_position = 800
            c.drawString(10, y_position, text[:100])
            y_position -= 12
        c.save()
        logger.debug(f"Created text layer PDF: {text_pdf_path}")
        
        merger = PyPDF2.PdfMerger()
        merger.append(pdf_path)
        merger.append(text_pdf_path)
        merger.write(output_path)
        merger.close()
        logger.info(f"Generated PDF: {output_path} ({os.path.getsize(output_path)} bytes)")
        
        for path in [screenshot_path, cropped_path, pdf_path, text_pdf_path]:
            if os.path.exists(path):
                os.remove(path)
                logger.debug(f"Removed temporary file: {path}")
                
    except Exception as e:
        logger.error(f"Error generating PDF screenshot for {url}: {e}")
        error_tracker.add_error('PDFGenerationError', f"Error generating PDF screenshot: {e}", url)

def load_config(config_file):
    """
    Load configuration from a YAML file and log its content.
    
    Args:
        config_file (str): Path to the YAML configuration file.
    
    Returns:
        dict: Configuration dictionary, or empty dict if loading fails.
    """
    logger = logging.getLogger('scraper')
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded config.yaml: {config}")
            return config
    except Exception as e:
        logger.error(f"Error loading config file {config_file}: {e}")
        return {}