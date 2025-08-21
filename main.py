import os
import time
import boto3
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


class ForeignJobs:
    def __init__(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # Explicitly set Chromium and chromedriver paths
        options.binary_location = "/usr/bin/chromium"
        service = Service("/usr/bin/chromedriver")

        self.driver = webdriver.Chrome(service=service, options=options)

    def scrape_lot(self, lot_no: int) -> dict:
        """Scrape data from the given lot number"""
        url = f"https://dofe.gov.np/PassportDetail.aspx?lot={lot_no}"
        self.driver.get(url)
        time.sleep(2)

        try:
            name = self.driver.find_element(By.ID, "lblName").text.strip()
            passport = self.driver.find_element(By.ID, "lblPassport").text.strip()
            status = self.driver.find_element(By.ID, "lblStatus").text.strip()
        except Exception:
            name, passport, status = "", "", "Not Found"

        return {"Lot": lot_no, "Name": name, "Passport": passport, "Status": status}

    def quit(self):
        self.driver.quit()


def save_to_s3(df: pd.DataFrame, filename: str):
    """Save DataFrame to S3 as CSV"""
    bucket = os.getenv("S3_BUCKET_NAME")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

    # Save locally first
    df.to_csv(filename, index=False)

    # Upload to S3
    s3.upload_file(filename, bucket, filename)
    print(f"✅ Uploaded {filename} to S3 bucket {bucket}")


def main():
    start_lot = int(os.getenv("START_LOT", "48156965"))
    end_lot = int(os.getenv("END_LOT", "48156975"))

    fj = ForeignJobs()
    all_data = []

    for lot in range(start_lot, end_lot + 1):
        print(f"Scraping lot {lot}...")
        data = fj.scrape_lot(lot)
        all_data.append(data)
        time.sleep(10)  # be polite

        # batch every 5 lots
        if len(all_data) % 5 == 0:
            df = pd.DataFrame(all_data)
            filename = f"lots_{lot-4}_to_{lot}.csv"
            save_to_s3(df, filename)
            all_data = []

    # remaining
    if all_data:
        df = pd.DataFrame(all_data)
        filename = f"lots_{end_lot-len(all_data)+1}_to_{end_lot}.csv"
        save_to_s3(df, filename)

    fj.quit()


if __name__ == "__main__":
    main()
