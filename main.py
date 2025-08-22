import os
import time
import csv
import logging
import boto3
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# ----------------- Logging -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------- Create Selenium Driver -----------------
def create_driver():
    logging.info("Checking ChromeDriver...")
    chromedriver_autoinstaller.install()  # install driver matching installed Chrome

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    logging.info("Chrome driver started successfully.")
    return driver

# ----------------- Scrape Function -----------------
def scrape_lot(lot_number, driver):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    logging.info(f"🔍 Scraping lot {lot_number} -> {url}")
    driver.get(url)
    time.sleep(3)  # wait for page to load

    try:
        data_element = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblPassportNo")
        passport_no = data_element.text.strip()
        logging.info(f"✅ Data scraped for lot {lot_number}")
    except Exception as e:
        passport_no = "N/A"
        logging.warning(f"⚠️ Error scraping lot {lot_number}: {e}")

    return {"lot": lot_number, "passport_no": passport_no}

# ----------------- Save CSV -----------------
def save_to_csv(data, filename="final_permission_selenium_scraped.csv"):
    logging.info(f"💾 Saving {len(data)} records to {filename}...")
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["lot", "passport_no"])
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(data)
    logging.info("CSV saved locally.")

# ----------------- Upload to S3 -----------------
def save_to_s3(filename):
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket:
        logging.error("S3_BUCKET_NAME environment variable not set!")
        return

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION")
    )

    s3_key = f"scraped-data/{filename}"
    try:
        s3.upload_file(filename, bucket, s3_key)
        logging.info(f"✅ File uploaded to S3: s3://{bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Failed to upload to S3: {e}")

# ----------------- Main Function -----------------
def main():
    driver = create_driver()
    start_lot = 48156965
    end_lot = 48156975
    batch_size = 5
    results = []

    for lot in range(start_lot, end_lot + 1):
        data = scrape_lot(lot, driver)
        results.append(data)

        if len(results) >= batch_size:
            save_to_csv(results)
            save_to_s3("final_permission_selenium_scraped.csv")
            results = []

        logging.info("⏱ Sleeping 20 seconds before next request...")
        time.sleep(20)

    if results:
        save_to_csv(results)
        save_to_s3("final_permission_selenium_scraped.csv")

    driver.quit()
    logging.info("🏁 Scraping completed successfully!")

# ----------------- Entry Point -----------------
if __name__ == "__main__":
    main()
