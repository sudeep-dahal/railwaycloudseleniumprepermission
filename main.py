import time
import csv
import os
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import boto3

# ===== Setup logging =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ===== AWS S3 Setup =====
S3_BUCKET = "finalapproval-csv-uploads"
S3_PATH = "scraped-data/final_permission_selenium_scraped.csv"

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# ===== Scraper settings =====
BASE_URL = "https://dofe.gov.np/PassportDetail.aspx?lot={lot}"
WAIT_BETWEEN_REQUESTS = 20  # seconds
MAX_RETRIES = 3

# ===== CSV file =====
CSV_FILE = "final_permission_selenium_scraped.csv"
CSV_FIELDS = ["lot_number", "passport_number"]

# ===== Create Chrome driver =====
def create_driver():
    chromedriver_autoinstaller.install()  # auto install chromedriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    return driver

# ===== Dynamic element detection =====
def get_passport_number(driver):
    """
    Tries multiple selectors to find the passport number dynamically.
    Returns text if found, None otherwise.
    """
    selectors = [
        "#ctl00_ContentPlaceHolder1_lblPassportNo",
        "#ctl00_ContentPlaceHolder1_lblAlternatePassportNo",
        "//span[contains(@id,'lblPassport')]",
        "//span[contains(text(),'Passport')]/following-sibling::span"
    ]
    element = None
    for sel in selectors:
        try:
            if sel.startswith("//"):
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, sel))
                )
            else:
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                )
            if element:
                return element.text.strip()
        except:
            continue
    return None

# ===== Scrape a single lot =====
def scrape_lot(driver, lot):
    url = BASE_URL.format(lot=lot)
    logging.info(f"🔍 Scraping lot {lot} -> {url}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            passport_number = get_passport_number(driver)
            if passport_number:
                logging.info(f"✅ Data scraped for lot {lot}: {passport_number}")
                return {"lot_number": lot, "passport_number": passport_number}
            else:
                logging.warning(f"⚠️ Element not found for lot {lot}")
                return {"lot_number": lot, "passport_number": ""}
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt}/{MAX_RETRIES} failed for lot {lot}: {e}")
            time.sleep(5)
    logging.error(f"❌ Failed to scrape lot {lot} after {MAX_RETRIES} attempts")
    return {"lot_number": lot, "passport_number": ""}

# ===== Save CSV =====
def save_csv(data):
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for row in data:
            writer.writerow(row)
    logging.info(f"💾 CSV saved locally: {CSV_FILE}")

# ===== Upload to S3 =====
def upload_to_s3():
    s3_client.upload_file(CSV_FILE, S3_BUCKET, S3_PATH)
    logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET}/{S3_PATH}")

# ===== Main scraper loop =====
def main(start_lot=None, end_lot=None):
    if start_lot is None:
        start_lot = int(os.getenv("START_LOT", "48156967"))  # fallback if no input
    if end_lot is None:
        end_lot = int(os.getenv("END_LOT", str(start_lot + 10)))  # scrape 10 lots by default

    driver = create_driver()
    scraped_data = []

    try:
        for lot in range(start_lot, end_lot + 1):
            data = scrape_lot(driver, lot)
            scraped_data.append(data)
            logging.info(f"⏱ Sleeping {WAIT_BETWEEN_REQUESTS} seconds before next request...")
            time.sleep(WAIT_BETWEEN_REQUESTS)
    finally:
        driver.quit()

    save_csv(scraped_data)
    upload_to_s3()

if __name__ == "__main__":
    main()
