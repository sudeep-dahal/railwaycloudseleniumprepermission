import os
import time
import logging
import boto3
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET_NAME")
START_LOT = int(os.getenv("START_LOT", 48156969))
END_LOT = int(os.getenv("END_LOT", 48156975))

# Check required environment variables
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET]):
    raise ValueError("❌ Missing required AWS S3 environment variables!")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Initialize boto3 S3 client
# -------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# -------------------------
# Selenium driver
# -------------------------
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

# -------------------------
# Scrape single lot
# -------------------------
def scrape_lot(lot_number):
    driver = init_driver()
    driver.get("https://dofe.gov.np/PassportDetail.aspx")
    data = {
        "LotNo": lot_number,
        "Name": "N/A",
        "Passport": "N/A",
        "Country": "N/A"
    }

    try:
        # Input Lot number
        lot_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "lytA_ctl23_Stickertext"))
        )
        lot_input.clear()
        lot_input.send_keys(f"{lot_number:09d}")

        # Click search
        driver.find_element(By.ID, "lytA_ctl23_passportSearch").click()
        time.sleep(5)

        # Scrape data
        name_elem = driver.find_element(By.ID, "lblName")
        passport_elem = driver.find_element(By.ID, "lblPassportNo")
        country_elem = driver.find_element(By.ID, "lblCountry")

        data["Name"] = name_elem.text.strip()
        data["Passport"] = passport_elem.text.strip()
        data["Country"] = country_elem.text.strip()

        logger.info(f"✅ Scraped lot {lot_number}: {data}")

    except (TimeoutException, NoSuchElementException) as e:
        logger.warning(f"⚠️ Error scraping lot {lot_number}: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error for lot {lot_number}: {e}")
    finally:
        driver.quit()

    return data

# -------------------------
# Save CSV and upload to S3
# -------------------------
def save_to_s3(df, filename):
    df.to_csv(filename, index=False)
    logger.info(f"💾 CSV saved locally as {filename}")

    try:
        s3.upload_file(filename, BUCKET, filename)
        logger.info(f"✅ Uploaded {filename} to S3 bucket {BUCKET}")
    except Exception as e:
        logger.error(f"❌ Failed to upload CSV to S3: {e}")

# -------------------------
# Main
# -------------------------
def main():
    results = []
    for lot in range(START_LOT, END_LOT + 1):
        logger.info(f"🔍 Scraping lot {lot}...")
        data = scrape_lot(lot)
        results.append(data)
        time.sleep(10)  # 10-second delay between requests

    if results:
        df = pd.DataFrame(results)
        filename = f"scraped_{START_LOT}_{END_LOT}.csv"
        save_to_s3(df, filename)
    else:
        logger.warning("⚠️ No data scraped.")

if __name__ == "__main__":
    main()
