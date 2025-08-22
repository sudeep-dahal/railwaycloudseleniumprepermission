import csv
import os
import time
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

# Constants
BASE_URL = 'https://dofe.gov.np/PassportDetail.aspx'
START_LOT_NUMBER = 48363817
END_LOT_NUMBER = 48363820
CSV_FILE = "final_permission_selenium_scraped.csv"
HOME_PAGE_SLEEP = 5
REQUEST_SLEEP = 20
RETRY_COUNT = 3

# Ensure ChromeDriver is installed
chromedriver_autoinstaller.install()

class ForeignJobsScraper:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Optional: run headless
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=options)

    def land_first_page(self):
        print(f"[INFO] Loading homepage: {BASE_URL}")
        self.driver.get(BASE_URL)
        time.sleep(HOME_PAGE_SLEEP)

    def scrape_lots(self):
        # Define CSV headers based on expected fields
        csv_headers = [
            "Going Through", "Name", "Gender", "PassportNo", "Company",
            "Country", "ApprovedDate", "StickerNo", "Skill", "Contract Period (in years)",
            "Salary", "Insurance", "Policy No.", "Policy Expiry Date", "Medical", "SSFId", "SubmissionNo"
        ]
        table_data = []

        for lot_number in range(START_LOT_NUMBER, END_LOT_NUMBER):
            value = f"{lot_number:09d}"
            success = False

            for attempt in range(1, RETRY_COUNT + 1):
                try:
                    # Clear and enter lot number
                    lot_input = self.driver.find_element(By.ID, "lytA_ctl23_Stickertext")
                    lot_input.clear()
                    lot_input.send_keys(value)

                    self.driver.find_element(By.ID, "lytA_ctl23_passportSearch").click()

                    # Wait for the details table
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="PassportMainshowTable"]/table'))
                    )

                    # Extract the table data
                    details_table = self.driver.find_element(By.XPATH, '//*[@id="PassportMainshowTable"]/table')
                    details = {}
                    for tr in details_table.find_elements(By.XPATH, './/tr'):
                        row = [td.text for td in tr.find_elements(By.XPATH, './/td')]
                        if len(row) >= 2:
                            details[row[0]] = row[1]

                    if details:
                        table_data.append(details)
                        print(f"[INFO] Scraped lot {value}: {details}")
                    else:
                        print(f"[WARNING] Element not found for lot {value}")

                    success = True
                    break  # Break retry loop if successful

                except (StaleElementReferenceException, TimeoutException) as e:
                    print(f"[WARNING] Attempt {attempt}/{RETRY_COUNT} failed for lot {value}: {e}")
                    time.sleep(REQUEST_SLEEP)
                except Exception as e:
                    print(f"[ERROR] Unexpected error for lot {value}: {e}")
                    break

            if not success:
                print(f"[WARNING] Failed to scrape lot {value} after {RETRY_COUNT} attempts")

            print(f"[INFO] Sleeping {REQUEST_SLEEP} seconds before next request...")
            time.sleep(REQUEST_SLEEP)

        # Write to CSV
        file_exists = os.path.isfile(CSV_FILE)
        try:
            with open(CSV_FILE, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                if not file_exists:
                    writer.writeheader()
                for row in table_data:
                    writer.writerow({key: row.get(key, "") for key in csv_headers})
            print(f"[INFO] CSV saved locally: {CSV_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to write CSV: {e}")
        finally:
            self.driver.quit()

if __name__ == "__main__":
    scraper = ForeignJobsScraper()
    scraper.land_first_page()
    scraper.scrape_lots()
