import os
import time
import logging
import pandas as pd
import chromedriver_autoinstaller
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =============================
# Logging setup
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# =============================
# S3 setup
# =============================
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    logging.error("Environment variable S3_BUCKET_NAME not set!")
    exit(1)

s3_client = boto3.client("s3")

# =============================
# Lot range setup (from env or default)
# =============================
START_LOT = int(os.getenv("START_LOT", 48156967))  # default start lot
END_LOT = int(os.getenv("END_LOT", 48156970))      # default end lot

# =============================
# ChromeDriver setup
# =============================
chromedriver_autoinstaller.install()  # automatically installs matching chromedriver

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # run headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    return driver

# =============================
# Scrape function
# =============================
def scrape_lot(lot_number, driver, timeout=15, retries=3):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    logging.info(f"🔍 Scraping lot {lot_number} -> {url}")

    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            passport_element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_lblPassportNo"))
            )
            passport_no = passport_element.text.strip()
            logging.info(f"✅ Data scraped for lot {lot_number}")
            return {"lot": lot_number, "passport_no": passport_no}
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt}/{retries} failed for lot {lot_number}: {e}")
            if attempt == retries:
                logging.error(f"❌ Failed to scrape lot {lot_number} after {retries} attempts.")
                return {"lot": lot_number, "passport_no": "N/A"}
            time.sleep(5)  # short wait before retry

# =============================
# Save to CSV & S3
# =============================
def save_to_csv_and_s3(data, filename="final_permission_selenium_scraped.csv"):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    logging.info(f"💾 CSV saved locally: {filename}")

    s3_key = f"scraped-data/{filename}"
    s3_client.upload_file(filename, S3_BUCKET, s3_key)
    logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

# =============================
# Main function
# =============================
def main():
    driver = create_driver()
    scraped_data = []

    for lot in range(START_LOT, END_LOT + 1):
        data = scrape_lot(lot, driver)
        scraped_data.append(data)

        logging.info("⏱ Sleeping 20 seconds before next request...")
        time.sleep(20)

    save_to_csv_and_s3(scraped_data)
    driver.quit()

if __name__ == "__main__":
    main()
