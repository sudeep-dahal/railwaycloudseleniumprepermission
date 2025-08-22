import time
import csv
import logging
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- Logging ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Selenium Setup ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# ---------------- Scraper ---------------- #
def scrape_lot(lot, retries=2):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot}"
    logging.info(f"🔍 Scraping lot {lot}...")
    driver.get(url)

    attempt = 0
    while attempt <= retries:
        try:
            # Wait for an element to appear (adjust selector for your case!)
            element = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_lblLot"))
            )

            # Example: scrape multiple fields
            lot_no = element.text.strip()
            passport = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblPassportNo").text.strip()
            name = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblName").text.strip()

            logging.info(f"✅ Data scraped for lot {lot}")
            return {
                "lot": lot_no,
                "passport": passport,
                "name": name
            }

        except Exception as e:
            attempt += 1
            if attempt > retries:
                logging.warning(f"⚠️ Failed to scrape lot {lot} after {retries} retries: {e}")
                return None
            logging.info(f"🔁 Retry {attempt} for lot {lot} after 10s...")
            time.sleep(10)

# ---------------- Save to CSV ---------------- #
def save_to_csv(data, filename="final_permission_selenium_scraped.csv"):
    keys = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"💾 CSV saved locally: {filename}")
    return filename

# ---------------- Upload to S3 ---------------- #
def upload_to_s3(filename, bucket_name="finalapproval-csv-uploads", key_prefix="scraped-data/"):
    s3 = boto3.client("s3")
    key = f"{key_prefix}{filename}"
    s3.upload_file(filename, bucket_name, key)
    logging.info(f"✅ File uploaded to S3: s3://{bucket_name}/{key}")

# ---------------- Main ---------------- #
def main():
    start_lot = 48363817
    end_lot = 48363825
    scraped_data = []

    for lot in range(start_lot, end_lot + 1):
        data = scrape_lot(str(lot))
        if data:
            scraped_data.append(data)
        time.sleep(20)  # polite delay between requests

    if scraped_data:
        filename = save_to_csv(scraped_data)
        upload_to_s3(filename)

    driver.quit()

if __name__ == "__main__":
    main()
