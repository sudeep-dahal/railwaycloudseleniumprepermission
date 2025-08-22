import os
import time
import logging
import pandas as pd
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------------------------------------------------------
# Create Chrome driver
# -----------------------------------------------------------------------------
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = "/usr/bin/chromium"  # Railway chromium path

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager(version="139.0.0").install()),
        options=chrome_options
    )
    return driver

# -----------------------------------------------------------------------------
# Scrape single passport detail
# -----------------------------------------------------------------------------
def scrape_passport(driver, lot_number):
    url = "https://dofe.gov.np/PassportDetail.aspx"
    driver.get(url)

    try:
        # Fill lot number
        input_box = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtLot"))
        )
        input_box.clear()
        input_box.send_keys(str(lot_number))

        # Click search button
        search_btn = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnSearch")
        search_btn.click()

        # Wait for table to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_gvPassport"))
        )

        rows = driver.find_elements(By.XPATH, "//table[@id='ctl00_ContentPlaceHolder1_gvPassport']/tbody/tr")

        results = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) > 1:
                results.append({
                    "lot_number": lot_number,
                    "passport_no": cols[0].text.strip(),
                    "name": cols[1].text.strip(),
                    "status": cols[2].text.strip()
                })

        logging.info(f"Scraped lot {lot_number}, found {len(results)} records")
        return results

    except Exception as e:
        logging.error(f"Error scraping lot {lot_number}: {e}")
        return []

# -----------------------------------------------------------------------------
# Save dataframe to S3
# -----------------------------------------------------------------------------
def save_to_s3(df, filename):
    bucket = os.getenv("AWS_BUCKET_NAME")
    if not bucket:
        raise ValueError("AWS_BUCKET_NAME environment variable is not set!")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

    df.to_csv(filename, index=False, encoding="utf-8-sig")
    logging.info(f"Uploading {filename} to S3 bucket {bucket}")
    s3.upload_file(filename, bucket, filename)
    logging.info("Upload complete")

# -----------------------------------------------------------------------------
# Main logic
# -----------------------------------------------------------------------------
def main():
    driver = create_driver()
    start_lot = int(os.getenv("START_LOT", "48156965"))
    end_lot = int(os.getenv("END_LOT", "48156995"))
    batch_size = 5

    all_results = []
    file_index = 1

    try:
        for lot in range(start_lot, end_lot + 1):
            results = scrape_passport(driver, lot)
            all_results.extend(results)

            # Wait 20 seconds between requests
            time.sleep(20)

            # Save in batches of 5
            if len(all_results) >= batch_size:
                df = pd.DataFrame(all_results)
                filename = f"passport_batch_{file_index}.csv"
                save_to_s3(df, filename)
                all_results = []
                file_index += 1

        # Save any remaining records
        if all_results:
            df = pd.DataFrame(all_results)
            filename = f"passport_batch_{file_index}.csv"
            save_to_s3(df, filename)

    finally:
        driver.quit()
        logging.info("Closed browser")

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
