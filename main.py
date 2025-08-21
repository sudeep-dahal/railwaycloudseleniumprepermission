import os
import time
import csv
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
import boto3
from dotenv import load_dotenv

# -------------------------
# Load environment variables
# -------------------------
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = os.getenv("S3_FILE_KEY", "scraped-data/final_permission_selenium_scraped.csv")

# -------------------------
# Logging configuration
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -------------------------
# Scraper configuration
# -------------------------
BASE_URL = "https://dofe.gov.np/"
START_LOT_NUMBER = 48363817
END_LOT_NUMBER = 48363820  # adjust as needed
CSV_FILE = "final_permission_selenium_scraped.csv"
CSV_HEADERS = [
    "Going Through", "Name", "Gender", "PassportNo", "Company",
    "Country", "ApprovedDate", "StickerNo", "Skill", "Contract Period (in years)",
    "Salary", "Insurance", "Policy No.", "Policy Expiry Date", "Medical", "SSFId", "SubmissionNo"
]

class ForeignJobsScraper:
    def __init__(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # Initialize Chrome driver
        self.driver = webdriver.Chrome(options=options)
        self.driver.get(BASE_URL)

    def scrape_lots(self):
        table_data = []

        for lot in range(START_LOT_NUMBER, END_LOT_NUMBER):
            lot_value = f"{lot:09d}"
            logging.info(f"🔍 Scraping lot {lot_value}...")
            details = {}

            try:
                # Wait for lot input field each iteration
                lot_input = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.ID, "lytA_ctl23_Stickertext"))
                )
                lot_input.clear()
                lot_input.send_keys(lot_value)

                # Click search
                search_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "lytA_ctl23_passportSearch"))
                )
                search_btn.click()

                # Wait for table to load
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="PassportMainshowTable"]/table'))
                )
                details_table = self.driver.find_element(By.XPATH, '//*[@id="PassportMainshowTable"]/table')

                for tr in details_table.find_elements(By.XPATH, ".//tr"):
                    row = [td.text for td in tr.find_elements(By.XPATH, ".//td")]
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
                logging.error(f"❌ Unexpected error for lot {lot_value}: {e}")

            # Delay 10 seconds between requests
            time.sleep(10)

        # Save CSV locally
        self.save_csv(table_data)

    def save_csv(self, table_data):
        if not table_data:
            logging.warning("⚠️ No data scraped. CSV will not be created.")
            self.driver.quit()
            return

        file_exists = os.path.isfile(CSV_FILE)
        try:
            with open(CSV_FILE, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
                if not file_exists:
                    writer.writeheader()
                for row in table_data:
                    writer.writerow({key: row.get(key, "") for key in CSV_HEADERS})
            logging.info(f"💾 CSV saved locally: {CSV_FILE}")
        except Exception as e:
            logging.error(f"❌ Error writing CSV: {e}")

        # Upload to S3
        self.upload_to_s3(CSV_FILE)
        self.driver.quit()

    def upload_to_s3(self, file_path):
        try:
            s3 = boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            s3.upload_file(file_path, S3_BUCKET_NAME, S3_FILE_KEY)
            logging.info(f"✅ File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")
        except Exception as e:
            logging.error(f"❌ Failed to upload file to S3: {e}")


if __name__ == "__main__":
    scraper = ForeignJobsScraper()
    scraper.scrape_lots()
