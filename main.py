import time
import csv
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Create Selenium driver
def create_driver():
    logging.info("Initializing headless Chrome driver...")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # modern headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    logging.info("Chrome driver started successfully.")
    return driver


# Example scrape function
def scrape_lot(lot_number, driver):
    url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_number}"
    logging.info(f"Fetching data for lot {lot_number} -> {url}")

    driver.get(url)
    time.sleep(3)  # Let the page load

    try:
        data_element = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_lblPassportNo")
        passport_no = data_element.text.strip()
    except Exception:
        passport_no = "N/A"

    logging.info(f"Lot {lot_number} -> Passport No: {passport_no}")
    return {"lot": lot_number, "passport_no": passport_no}


# Save data to CSV
def save_to_csv(data, filename="output.csv"):
    logging.info(f"Saving {len(data)} records to {filename}...")
    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["lot", "passport_no"])
        if f.tell() == 0:  # Write header only if file is new
            writer.writeheader()
        writer.writerows(data)
    logging.info("Save completed.")


# Main function
def main():
    driver = create_driver()
    start_lot = 48156965
    end_lot = 48156975

    results = []
    for lot in range(start_lot, end_lot + 1):
        data = scrape_lot(lot, driver)
        results.append(data)

        # Save every 5 records
        if len(results) >= 5:
            save_to_csv(results)
            results = []

        logging.info("Sleeping 20 seconds before next request...")
        time.sleep(20)

    # Save remaining data
    if results:
        save_to_csv(results)

    driver.quit()
    logging.info("Scraping completed successfully!")


if __name__ == "__main__":
    main()
