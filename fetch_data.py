import requests
import os
import pandas as pd
import subprocess
import hashlib

# API Endpoint
RESOURCE_ID = "d589f2bc-9a85-4f65-be2f-20f17debfcb1"
API_BASE_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search"

# File paths
DATA_FOLDER = "data/"
MASTER_FILE = os.path.join(DATA_FOLDER, "fuel_consumption_master.csv")

# Expected column names based on API response
COLUMN_HEADERS = [
    "_id", "year", "make", "model", "vehicle_class",
    "engine_size", "cylinders", "transmission", "fuel_type",
    "city_l_100km", "highway_l_100km", "combined_l_100km",
    "combined_mpg", "co2_emissions_g_km", "co2_rating", "smog_rating"
]

def fetch_all_data():
    """Fetch all fuel consumption data using pagination"""
    all_records = []
    offset = 0
    limit = 5000  # Maximum allowed by API

    while True:
        url = f"{API_BASE_URL}?resource_id={RESOURCE_ID}&limit={limit}&offset={offset}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            records = data.get("result", {}).get("records", [])

            if not records:
                break  # Stop if no more records

            all_records.extend(records)
            offset += limit  # Move to next batch

            print(f"Fetched {len(all_records)} records...")

        else:
            print("Failed to retrieve data:", response.status_code)
            break

    return all_records

def save_to_csv(records):
    """Save the fetched data into a CSV file"""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    df = pd.DataFrame(records, columns=COLUMN_HEADERS)
    df.to_csv(MASTER_FILE, index=False, encoding="utf-8")

    print(f"Data saved to {MASTER_FILE}")

def commit_and_push_changes():
    """Commit and push updated data to GitHub"""
    subprocess.run(["git", "config", "--global", "user.email", "your-email@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "your-github-username"])

    subprocess.run(["git", "add", MASTER_FILE])
    subprocess.run(["git", "commit", "-m", "Updated fuel consumption master dataset"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    print("Fetching all data from CKAN API...")
    records = fetch_all_data()

    if records:
        save_to_csv(records)
        commit_and_push_changes()
    else:
        print("No data retrieved.")
