import os
import time
import pandas as pd
import boto3
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME]):
    raise ValueError("❌ Missing one or more required AWS environment variables.")

def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    return driver

def scrape_lot(lot_number):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    driver = init_driver()
    driver.get(url)

    try:
        data = {
            "lot_number": lot_number,
            "name": driver.find_element(By.ID, "lblName").text,
            "passport_number": driver.find_element(By.ID, "lblPassportNo").text,
            "status": driver.find_element(By.ID, "lblStatus").text,
        }
        logger.info(f"Scraped lot {lot_number} successfully")
    except Exception as e:
        logger.warning(f"Error scraping lot {lot_number}: {e}")
        data = None
    finally:
        driver.quit()

    return data

def save_to_s3(df, filename):
    if not S3_BUCKET_NAME:
        raise ValueError("❌ Environment variable S3_BUCKET_NAME is not set!")

    df.to_csv(filename, index=False)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3.upload_file(filename, S3_BUCKET_NAME, filename)

    logger.info(f"✅ Uploaded {filename} to s3://{S3_BUCKET_NAME}/{filename}")

def main():
    start_lot = 48156965
    end_lot = 48156970  # change as needed

    results = []
    for lot in range(start_lot, end_lot + 1):
        logger.info(f"🔍 Scraping lot {lot}...")
        data = scrape_lot(lot)
        if data:
            results.append(data)
        time.sleep(10)  # respect rate limits

    if results:
        df = pd.DataFrame(results)
        filename = f"lots_{start_lot}_{end_lot}.csv"
        save_to_s3(df, filename)
    else:
        logger.warning("⚠️ No data scraped.")

if __name__ == "__main__":
    main()
