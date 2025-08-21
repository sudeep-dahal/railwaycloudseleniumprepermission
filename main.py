import csv
import os
import time
import logging
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from dotenv import load_dotenv

# -------------------------
# Load environment variables from .env
# -------------------------
load_dotenv()

BASE_URL = 'https://dofe.gov.np/'
START_LOT_NUMBER = 48363817
END_LOT_NUMBER = 48363820

# AWS S3 config
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = os.getenv("S3_FILE_KEY", "scraped-data/final_permission_selenium_scraped.csv")
AWS_REGION = os.getenv("AWS_REGION")

# -------------------------
# Logging config
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

class ForeignJobs:
    def __init__(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--window-size=1920,1080")

        self.driver = webdriver.Chrome(options=options)

    def land_first_page(self):
        self.driver.get(BASE_URL)
        time.sleep(2)

    def select_lot_number(self):
        csv_headers = [
            "Going Through", "Name", "Gender", "PassportNo", "Company",
            "Country", "ApprovedDate", "StickerNo", "Skill", "Contract Period (in years)",
            "Salary", "Insurance", "Policy No.", "Policy Expiry Date", "Medical", "SSFId", "SubmissionNo"
        ]
        table_data = []

        try:
            for i in range(START_LOT_NUMBER, END_LOT_NUMBER):
                lot_value = f"{i:09d}"
                logging.info(f"🔍 Scraping lot {lot_value}...")
                details = {}

                try:
                    # Wait up to 20 seconds for the input box
                    lot_input = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.ID, "lytA_ctl23_Stickertext"))
                    )
                    lot_input.clear()
                    lot_input.send_keys(lot_value)

                    search_button = self.driver.find_element(By.ID, "lytA_ctl23_passportSearch")
                    search_button.click()

                    # Wait up to 20 seconds for the results table
                    details_table = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="PassportMainshowTable"]/table'))
                    )

                    # Scrape the table
                    for tr in details_table.find_elements(By.XPATH, './/tr'):
                        row = [td.text for td in tr.find_elements(By.XPATH, './/td')]
                        if len(row) >= 2:
                            details[row[0]] = row[1]

                    if details:
                        table_data.append(details)
                        logging.info(f"✅ Data scraped for lot {lot_value}")
                    else:
                        logging.warning(f"⚠️ No data found for lot {lot_value}")

                except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
                    logging.warning(f"⚠️ Error scraping lot {lot_value}: {e}")

                except Exception as e:
                    logging.warning(f"⚠️ Unexpected error for lot {lot_value}: {e}")

                finally:
                    time.sleep(10)  # 10-second delay between lots

        finally:
            csv_file = "final_permission_selenium_scraped.csv"
            file_exists = os.path.isfile(csv_file)
            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                    if not file_exists:
                        writer.writeheader()
                    for row in table_data:
                        writer.writerow({key: row.get(key, "") for key in csv_headers})

                logging.info(f"💾 CSV saved locally: {csv_file}")
                self.upload_to_s3(csv_file)

            finally:
                self.driver.quit()

    def upload_to_s3(self, file_path):
        """Uploads the CSV to AWS S3"""
        if not S3_BUCKET_NAME:
            logging.error("❌ S3_BUCKET_NAME is not set in environment variables.")
            return

        try:
            s3 = boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            s3.upload_file(file_path, S3_BUCKET_NAME, S3_FILE_KEY)
            logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")
        except Exception as e:
            logging.error(f"❌ Failed to upload file to S3: {e}")


if __name__ == '__main__':
    fj = ForeignJobs()
    fj.land_first_page()
    fj.select_lot_number()
