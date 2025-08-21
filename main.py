import os
import time
import logging
import boto3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# AWS S3 setup
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
BUCKET = os.getenv("S3_BUCKET_NAME")

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service("/usr/bin/chromedriver")  # adjust path if needed
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def scrape_lot(lot_number):
    url = f"https://dofe.gov.np/PassportDetail.aspx?LotNo={lot_number}"
    driver = init_driver()
    driver.get(url)

    try:
        name_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "lblName"))
        )
        passport_elem = driver.find_element(By.ID, "lblPassportNo")
        country_elem = driver.find_element(By.ID, "lblCountry")

        data = {
            "LotNo": lot_number,
            "Name": name_elem.text.strip(),
            "Passport": passport_elem.text.strip(),
            "Country": country_elem.text.strip()
        }
        logger.info(f"✅ Scraped lot {lot_number} successfully")
        return data

    except Exception as e:
        logger.warning(f"⚠️ Error scraping lot {lot_number}: {e}")
        return {
            "LotNo": lot_number,
            "Name": "N/A",
            "Passport": "N/A",
            "Country": "N/A"
        }

    finally:
        driver.quit()

def save_to_s3(df, filename):
    df.to_csv(filename, index=False)
    try:
        s3.upload_file(filename, BUCKET, filename)
        logger.info(f"✅ Uploaded {filename} to S3 bucket {BUCKET}")
    except Exception as e:
        logger.error(f"❌ Failed to upload {filename} to S3: {e}")

def main():
    start_lot = int(os.getenv("START_LOT", "48156969"))
    end_lot = int(os.getenv("END_LOT", "48156975"))  # change as needed
    results = []

    for lot in range(start_lot, end_lot + 1):
        logger.info(f"🔍 Scraping lot {lot}...")
        data = scrape_lot(lot)
        results.append(data)
        time.sleep(10)  # 10 seconds between requests

    if results:
        df = pd.DataFrame(results)
        filename = f"scraped_{start_lot}_{end_lot}.csv"
        save_to_s3(df, filename)
    else:
        logger.warning("⚠️ No data scraped.")

if __name__ == "__main__":
    main()
