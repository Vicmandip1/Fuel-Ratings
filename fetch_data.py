import requests
import os
import csv
import subprocess
import hashlib
import pandas as pd
from datetime import datetime

# API Endpoint
API_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search?resource_id=d589f2bc-9a85-4f65-be2f-20f17debfcb1&limit=5000"

# File paths
DATA_FOLDER = "data/"
MASTER_FILE = os.path.join(DATA_FOLDER, "fuel_consumption_master.csv")
LAST_HASH_FILE = "last_hash.txt"

def fetch_all_data():
    """Fetch fuel consumption data from API"""
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        return data.get('result', {}).get('records', [])
    print("Failed to retrieve data.")
    return None

def calculate_hash(records):
    """Create hash of dataset to track changes"""
    data_string = "".join(str(record) for record in records)
    return hashlib.md5(data_string.encode()).hexdigest()

def has_data_changed(new_hash):
    """Check if new data differs from last fetched data"""
    if os.path.exists(LAST_HASH_FILE):
        with open(LAST_HASH_FILE, "r") as file:
            last_hash = file.read().strip()
        return last_hash != new_hash
    return True  # Assume new if no previous record

def append_or_create_csv(records):
    """Append new records to master CSV or create if missing"""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    df_new = pd.DataFrame(records)
    
    if os.path.exists(MASTER_FILE):
        df_old = pd.read_csv(MASTER_FILE)
        df_combined = pd.concat([df_old, df_new]).drop_duplicates()
    else:
        df_combined = df_new

    df_combined.to_csv(MASTER_FILE, index=False)
    print(f"Updated master file: {MASTER_FILE}")

def commit_and_push_changes():
    """Commit and push updated data to GitHub"""
    subprocess.run(["git", "config", "--global", "user.email", "your-email@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "your-github-username"])

    subprocess.run(["git", "add", MASTER_FILE])
    subprocess.run(["git", "commit", "-m", "Updated fuel consumption master dataset"])
    subprocess.run(["git", "push"])

def main():
    """Main function to fetch and merge data"""
    records = fetch_all_data()
    if records:
        new_hash = calculate_hash(records)
        if has_data_changed(new_hash):
            append_or_create_csv(records)
            commit_and_push_changes()
            with open(LAST_HASH_FILE, "w") as file:
                file.write(new_hash)
        else:
            print("No new updates found.")
    else:
        print("Failed to retrieve data.")

if __name__ == "__main__":
    main()
