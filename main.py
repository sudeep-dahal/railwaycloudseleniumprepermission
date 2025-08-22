import os
import time
import logging
import csv
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3

# ====================== CONFIG ======================
START_LOT = int(os.environ.get("START_LOT", 48156967))
END_LOT = int(os.environ.get("END_LOT", 48156970))  # example end lot
DELAY_SECONDS = int(os.environ.get("DELAY_SECONDS", 20))
MAX_RETRIES = 3
CSV_FILE = "final_permission_selenium_scraped.csv"

# S3 config
S3_BUCKET = os.environ.get("S3_BUCKET", "finalapproval-csv-uploads")
S3_KEY = f"scraped-data/{CSV_FILE}"

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ====================== DRIVER SETUP ======================
def create_driver():
    # auto-install chromedriver matching installed chrome
    chromedriver_autoinstaller.install()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=options)
    return driver

# ====================== SCRAPER ======================
def scrape_lot(driver, lot_number):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    logging.info(f"🔍 Scraping lot {lot_number} -> {url}")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            
            # wait for page to fully load
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # try multiple possible selectors in case ID changes
            selectors = [
                "#ctl00_ContentPlaceHolder1_lblPassportNo",
                "#ctl00_ContentPlaceHolder1_lblAlternatePassportNo"
            ]
            element = None
            for sel in selectors:
                try:
                    element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    if element:
                        break
                except:
                    continue
            
            if element:
                passport_no = element.text.strip()
                logging.info(f"✅ Found passport number: {passport_no}")
                return {"lot": lot_number, "passport_no": passport_no, "url": url}
            else:
                logging.warning(f"⚠️ Element not found for lot {lot_number}")
                return {"lot": lot_number, "passport_no": None, "url": url}
        
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt}/{MAX_RETRIES} failed for lot {lot_number}: {e}")
            time.sleep(5)
    
    logging.error(f"❌ Failed to scrape lot {lot_number} after {MAX_RETRIES} attempts")
    return {"lot": lot_number, "passport_no": None, "url": url}

# ====================== CSV UTILS ======================
def save_to_csv(data_list, filename):
    keys = data_list[0].keys() if data_list else ["lot", "passport_no", "url"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data_list)
    logging.info(f"💾 CSV saved locally: {filename}")

# ====================== S3 UPLOAD ======================
def upload_to_s3(local_file, bucket, key):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    )
    s3.upload_file(local_file, bucket, key)
    logging.info(f"✅ File uploaded to S3: s3://{bucket}/{key}")

# ====================== MAIN ======================
def main():
    driver = create_driver()
    all_data = []
    
    for lot_number in range(START_LOT, END_LOT + 1):
        data = scrape_lot(driver, lot_number)
        all_data.append(data)
        
        logging.info(f"⏱ Sleeping {DELAY_SECONDS} seconds before next request...")
        time.sleep(DELAY_SECONDS)
    
    driver.quit()
    save_to_csv(all_data, CSV_FILE)
    upload_to_s3(CSV_FILE, S3_BUCKET, S3_KEY)

if __name__ == "__main__":
    main()
