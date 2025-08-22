import time
import csv
import logging
import os
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Ensure chromedriver is installed and matches current Chrome
chromedriver_autoinstaller.install()

# AWS S3 config (replace with your bucket details)
S3_BUCKET = "finalapproval-csv-uploads"
S3_KEY_PREFIX = "scraped-data/final_permission_selenium_scraped.csv"

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_passport_number(driver):
    """
    Dynamically attempts multiple selectors to find passport number.
    """
    selectors = [
        "#ctl00_ContentPlaceHolder1_lblPassportNo",
        "#ctl00_ContentPlaceHolder1_lblAlternatePassportNo",
        "//span[contains(@id,'lblPassport')]",
        "//span[contains(text(),'Passport')]/following-sibling::span",
        "//td[contains(text(),'Passport Number')]/following-sibling::td"
    ]
    for sel in selectors:
        try:
            if sel.startswith("//"):
                element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, sel))
                )
            else:
                element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                )
            if element:
                return element.text.strip()
        except:
            continue
    return None

def scrape_lot(driver, lot):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot}"
    logging.info(f"🔍 Scraping lot {lot} -> {url}")
    driver.get(url)

    # Ensure page loaded
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except:
        logging.warning(f"⚠️ Page did not load properly for lot {lot}")
        return None

    passport_no = get_passport_number(driver)
    if passport_no:
        logging.info(f"✅ Found passport number for lot {lot}: {passport_no}")
    else:
        logging.warning(f"⚠️ Element not found for lot {lot}")
    return {"lot": lot, "passport_no": passport_no or ""}

def save_to_csv(data, filename="final_permission_selenium_scraped.csv"):
    keys = data[0].keys() if data else ["lot", "passport_no"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"💾 CSV saved locally: {filename}")
    return filename

def upload_to_s3(file_path):
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, S3_BUCKET, S3_KEY_PREFIX)
    logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET}/{S3_KEY_PREFIX}")

def main():
    driver = create_driver()

    # Configurable lot range
    start_lot = int(os.environ.get("START_LOT", 48156967))
    end_lot = int(os.environ.get("END_LOT", 48156970))

    scraped_data = []
    for lot in range(start_lot, end_lot + 1):
        try:
            data = scrape_lot(driver, lot)
            if data:
                scraped_data.append(data)
        except Exception as e:
            logging.warning(f"⚠️ Error scraping lot {lot}: {e}")
        logging.info("⏱ Sleeping 20 seconds before next request...")
        time.sleep(20)

    driver.quit()

    if scraped_data:
        csv_file = save_to_csv(scraped_data)
        upload_to_s3(csv_file)
    else:
        logging.warning("⚠️ No data scraped.")

if __name__ == "__main__":
    main()
