import os
import time
import csv
import logging
import boto3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import get_browser_version_from_os

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ================== AWS CONFIG ==================
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# ================== SELENIUM SETUP ==================
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Auto-detect correct Chromium version
    browser_version = (
        get_browser_version_from_os("google-chrome")
        or get_browser_version_from_os("chromium")
    )
    logger.info(f"Detected browser version: {browser_version}")

    driver_path = ChromeDriverManager(version=browser_version).install()
    logger.info(f"Using ChromeDriver: {driver_path}")

    return webdriver.Chrome(service=Service(driver_path), options=chrome_options)


# ================== SCRAPER FUNCTION ==================
def scrape_lot(lot_number):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    logger.info(f"Scraping lot number: {lot_number} from {url}")
    driver.get(url)

    # Example scraping (you should adjust selectors as per actual site)
    try:
        element = driver.find_element("id", "ctl00_ContentPlaceHolder1_lblPassportNo")
        passport_no = element.text.strip()
    except Exception as e:
        logger.warning(f"No data found for lot {lot_number}: {e}")
        passport_no = None

    return {"lot_number": lot_number, "passport_no": passport_no}


# ================== SAVE TO CSV ==================
def save_to_csv(data, filename):
    keys = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


# ================== UPLOAD TO S3 ==================
def upload_to_s3(file_path, bucket_name, object_name):
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{object_name}")
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")


# ================== MAIN ==================
if __name__ == "__main__":
    driver = create_driver()

    start_lot = 48156965
    end_lot = 48156975  # Example range
    results = []

    for lot in range(start_lot, end_lot + 1):
        try:
            result = scrape_lot(lot)
            if result["passport_no"]:
                results.append(result)
        except Exception as e:
            logger.error(f"Error scraping lot {lot}: {e}")

        # Wait 20 seconds between requests
        logger.info("Sleeping for 20 seconds before next request...")
        time.sleep(20)

        # Save & upload every 5 records
        if len(results) >= 5:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lot_data_{timestamp}.csv"
            save_to_csv(results, filename)
            upload_to_s3(filename, S3_BUCKET_NAME, filename)
            results.clear()

    # Save remaining results if any
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lot_data_{timestamp}.csv"
        save_to_csv(results, filename)
        upload_to_s3(filename, S3_BUCKET_NAME, filename)

    driver.quit()
    logger.info("Scraping completed.")
