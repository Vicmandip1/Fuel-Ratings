import requests
import os
import pandas as pd

# API Endpoint
API_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search?resource_id=d589f2bc-9a85-4f65-be2f-20f17debfcb1&limit=5000"

# File paths
DATA_FOLDER = "data/"
MASTER_FILE = os.path.join(DATA_FOLDER, "fuel_consumption_master.csv")

def fetch_new_data():
    """Fetch new data from API"""
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        return data.get("result", {}).get("records", [])
    print("Failed to retrieve data.")
    return None

def append_new_data(records):
    """Append new data to the master file without duplicates"""
    if records:
        new_df = pd.DataFrame(records)

        # Load existing data
        if os.path.exists(MASTER_FILE):
            existing_df = pd.read_csv(MASTER_FILE)
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates()
        else:
            combined_df = new_df

        # Save the updated data
        combined_df.to_csv(MASTER_FILE, index=False, encoding="utf-8")
        print(f"New data appended to {MASTER_FILE}")
    else:
        print("No new data retrieved.")

if __name__ == "__main__":
    print("Fetching new data...")
    records = fetch_new_data()
    append_new_data(records)
