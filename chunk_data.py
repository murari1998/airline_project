import os
import requests
import zipfile
from pathlib import Path

BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"
LOCAL_SAVE_FOLDER = "./downloaded_files"

Path(LOCAL_SAVE_FOLDER).mkdir(parents=True, exist_ok=True)

YEARS_TO_DOWNLOAD = [2021, 2022]


def download_one_month(year, month, save_folder):

    zip_filename = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    download_url = BTS_WEBSITE + zip_filename

    zip_path = os.path.join(save_folder, zip_filename)
    csv_path = zip_path.replace(".zip", ".csv")

    if os.path.exists(csv_path):
        print(f"Already downloaded: {year}-{month}")
        return csv_path

    print(f"Downloading {year}-{month}...")

    try:
        # download file
        response = requests.get(download_url, stream=True, timeout=120)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(1024 * 1024):
                f.write(chunk)

        # unzip file
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            csv_file = [f for f in zip_ref.namelist() if f.endswith(".csv")][0]
            zip_ref.extract(csv_file, save_folder)

            os.rename(os.path.join(save_folder, csv_file), csv_path)

        # delete zip
        os.remove(zip_path)

        print(f"Done: {csv_path}")
        return csv_path

    except Exception as e:
        print(f"Error: {e}")
        return None


# run loop
for year in YEARS_TO_DOWNLOAD:
    print(f"\nYear {year}")

    for month in range(1, 4):
        download_one_month(year, month, LOCAL_SAVE_FOLDER)
        func(year, month)