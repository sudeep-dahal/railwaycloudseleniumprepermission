import csv
import os
import time
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

# -------------------------
# CONFIG
# -------------------------
BASE_URL = 'https://dofe.gov.np/'
START_LOT_NUMER = 48363817
END_LOT_NUMBER = 48363820

# AWS S3 config (set in Railway environment variables)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "your-bucket-name")
S3_FILE_KEY = os.getenv("S3_FILE_KEY", "scraped-data/final_permission_selenium_scraped.csv")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")


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

    def select_Lot_Number(self):
        csv_headers = [
            "Going Through", "Name", "Gender", "PassportNo", "Company",
            "Country", "ApprovedDate", "StickerNo", "Skill", "Contract Period (in years)",
            "Salary", "Insurance", "Policy No.", "Policy Expiry Date", "Medical", "SSFId", "SubmissionNo"
        ]
        table_data = []

        try:
            for i in range(START_LOT_NUMER, END_LOT_NUMBER):
                details = {}
                value = f"{i:09d}"
                try:
                    lot_input = self.driver.find_element(By.ID, "lytA_ctl23_Stickertext")
                    lot_input.clear()
                    lot_input.send_keys(value)

                    self.driver.find_element(By.ID, "lytA_ctl23_passportSearch").click()
                    time.sleep(5)

                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="PassportMainshowTable"]/table'))
                    )

                    details_table = self.driver.find_element(By.XPATH, '//*[@id="PassportMainshowTable"]/table')
                    for tr in details_table.find_elements(By.XPATH, './/tr'):
                        row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
                        if len(row) >= 2:
                            details[row[0]] = row[1]

                    if details:
                        table_data.append(details)
                        print(f"✅ Scraped details for lot {value}: {details}")
                        self.driver.find_element(By.ID, "passportSearch").click()
                        time.sleep(10)
                    else:
                        print(f"⚠️ No details found for lot {value}")

                except (StaleElementReferenceException, TimeoutException) as e:
                    print(f"⚠️ Error scraping lot {value}: {e}")
                    continue
                except Exception as e:
                    print(f"⚠️ Unexpected error for lot {value}: {e}")
                    continue

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

                print("💾 CSV saved locally")
                self.upload_to_s3(csv_file)

            except Exception as e:
                print(f"❌ Error writing to CSV: {e}")
            finally:
                self.driver.quit()

    def upload_to_s3(self, file_path):
        """Uploads the CSV to AWS S3"""
        try:
            s3 = boto3.client("s3", region_name=AWS_REGION)
            s3.upload_file(file_path, S3_BUCKET_NAME, S3_FILE_KEY)
            print(f"✅ File uploaded to S3 bucket '{S3_BUCKET_NAME}' at '{S3_FILE_KEY}'")
        except Exception as e:
            print(f"❌ Failed to upload file to S3: {e}")


if __name__ == '__main__':
    fj = ForeignJobs()
    fj.land_first_page()
    fj.select_Lot_Number()
