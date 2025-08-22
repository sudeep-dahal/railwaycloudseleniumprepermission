import time
import csv
import logging
import os
import boto3
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- Logging ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ---------------- AWS S3 ---------------- #
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_REGION', 'us-east-1')
)
S3_BUCKET = os.environ.get('S3_BUCKET', 'finalapproval-csv-uploads')

# ---------------- WebDriver ---------------- #
def create_driver():
    # Install chromedriver matching local Chrome
    chromedriver_autoinstaller.install()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # remove if you want to see browser
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    return driver

# ---------------- Scraper ---------------- #
def scrape_lot(driver, lot):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot}"
    logging.info(f"🔍 Scraping lot {lot} -> {url}")
    driver.get(url)
    
    try:
        # Wait up to 15 seconds for the element to appear
        elem = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_lblPassportNo"))
        )
        passport_no = elem.text.strip()
        logging.info(f"✅ Found passport number for lot {lot}: {passport_no}")
        return {"lot": lot, "passport_no": passport_no}
    except:
        logging.warning(f"⚠️ Element not found for lot {lot}")
        return {"lot": lot, "passport_no": ""}

def scrape_lot_with_retries(driver, lot, retries=3):
    for attempt in range(1, retries + 1):
        data = scrape_lot(driver, lot)
        if data["passport_no"]:
            return data
        logging.info(f"🔁 Retry {attempt}/{retries} for lot {lot}")
        time.sleep(5)
    return {"lot": lot, "passport_no": ""}

# ---------------- CSV ---------------- #
def save_to_csv(data_list, filename="final_permission_selenium_scraped.csv"):
    keys = data_list[0].keys() if data_list else ["lot", "passport_no"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data_list)
    logging.info(f"💾 CSV saved locally: {filename}")
    return filename

def upload_to_s3(file_path):
    s3_key = f"scraped-data/{os.path.basename(file_path)}"
    s3_client.upload_file(file_path, S3_BUCKET, s3_key)
    logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

# ---------------- Main ---------------- #
def main():
    driver = create_driver()
    
    # Define start and end lot numbers here
    start_lot = 48156967
    end_lot = 48156970
    
    all_data = []
    
    for lot in range(start_lot, end_lot + 1):
        data = scrape_lot_with_retries(driver, lot)
        all_data.append(data)
        logging.info("⏱ Sleeping 20 seconds before next request...")
        time.sleep(20)
    
    driver.quit()
    
    csv_file = save_to_csv(all_data)
    upload_to_s3(csv_file)

if __name__ == "__main__":
    main()
