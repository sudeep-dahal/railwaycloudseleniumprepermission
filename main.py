# from foreignJobs.foreignJobs import ForeignJobs 

import csv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import foreignJobs.constants as const
import time


BASE_URL = 'https://dofe.gov.np/'
START_LOT_NUMER = 48363817
END_LOT_NUMBER = 48363820

class ForeignJobs:
    def __init__(self):
        try:
            self.driver = webdriver.Chrome()
        except Exception as e:
            self.driver = webdriver.Chrome(ChromeDriverManager().install())

    def land_first_page(self):
        self.driver.get(const.BASE_URL)
        time.sleep(5)

    def select_Lot_Number(self):
        # Define CSV headers based on expected fields
        csv_headers = [
            "Going Through", "Name", "Gender", "PassportNo", "Company",
            "Country", "ApprovedDate", "StickerNo", "Skill", "Contract Period (in years)",
            "Salary", "Insurance", "Policy No.", "Policy Expiry Date", "Medical", "SSFId", "SubmissionNo"
        ]
        table_data = []

        try:
            for i in range(const.START_LOT_NUMER, const.END_LOT_NUMBER):
                details = {}  # Initialize details for each lot
                value = f"{i:09d}"
                try:
                    # Locate elements
                    lot_input = self.driver.find_element(By.ID, "lytA_ctl23_Stickertext")
                    lot_input.clear()
                    lot_input.send_keys(value)

                    self.driver.find_element(By.ID, "lytA_ctl23_passportSearch").click()
                    time.sleep(5)  # Keep the requested sleep

                    # Wait for the details table
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="PassportMainshowTable"]/table'))
                    )

                    # Extract the "Details" section
                    details_table = self.driver.find_element(By.XPATH, '//*[@id="PassportMainshowTable"]/table')
                    for tr in details_table.find_elements(By.XPATH, './/tr'):
                        row = [item.text for item in tr.find_elements(By.XPATH, './/td')]
                        if len(row) >= 2:  # Ensure row has at least 2 columns
                            details[row[0]] = row[1]

                    if details:  # Only append if details were successfully scraped
                        table_data.append(details)
                        print(f"Scraped details for lot {value}: {details}")
                        self.driver.find_element(By.ID, "passportSearch").click()
                        time.sleep(10)
                    else:
                        print(f"No details found for lot {value}")

                except (StaleElementReferenceException, TimeoutException) as e:
                    print(f"Error scraping lot {value}: {e}")
                    continue
                except Exception as e:
                    print(f"Unexpected error for lot {value}: {e}")
                    continue

        except Exception as e:
            print(f"Exception occurred: {e}")
            print(f"Error scraping at point: {i}")

        finally:
            # Write the data to CSV
            csv_file = "final_permission_selenium_scraped.csv"
            file_exists = os.path.isfile(csv_file)
            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                    if not file_exists:
                        writer.writeheader()
                    for row in table_data:
                        # Write each row, filling missing fields with empty strings
                        writer.writerow({key: row.get(key, "") for key in csv_headers})
            except Exception as e:
                print(f"Error writing to CSV: {e}")
            finally:
                self.driver.close()

if __name__=='__main__':
    fj = ForeignJobs()
    fj.land_first_page()
    fj.select_Lot_Number()
