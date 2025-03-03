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

# Expected column headers based on provided structure
COLUMN_HEADERS = [
    "_id", "Model year", "Make", "Model", "Vehicle class",
    "Engine size (L)", "Cylinders", "Transmission", "Fuel type",
    "City (L/100 km)", "Highway (L/100 km)", "Combined (L/100 km)",
    "Combined (mpg)", "CO2 emissions (g/km)", "CO2 rating", "Smog rating"
]

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

def process_records(records):
    """Convert API response to match expected column structure"""
    processed_data = []
    for record in records:
        row = [
            record.get("_id", ""),
            record.get("year", ""),
            record.get("make", ""),
            record.get("model", ""),
            record.get("vehicle_class", ""),
            record.get("engine_size", ""),
            record.get("cylinders", ""),
            record.get("transmission", ""),
            record.get("fuel_type", ""),
            record.get("city_l_100km", ""),
            record.get("highway_l_100km", ""),
            record.get("combined_l_100km", ""),
            record.get("combined_mpg", ""),
            record.get("co2_emissions_g_km", ""),
            record.get("co2_rating", ""),
            record.get("smog_rating", ""),
        ]
        processed_data.append(row)
    return processed_data

def append_or_create_csv(records):
    """Append new records to master CSV or create if missing"""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    df_new = pd.DataFrame(records, columns=COLUMN_HEADERS)

    # Ensure master file exists with correct headers
    if not os.path.exists(MASTER_FILE):
        df_new.to_csv(MASTER_FILE, index=False)
    else:
        df_old = pd.read_csv(MASTER_FILE)
        df_combined = pd.concat([df_old, df_new]).drop_duplicates()
        df_combined.to_csv(MASTER_FILE, index=False)

    print(f"Updated master file: {MASTER_FILE}")

def commit_and_push_changes():
    """Commit and push updated data to GitHub"""
    subprocess.run(["git", "config", "--global", "user.email", "your-email@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "your-github-username"])

    subprocess.run(["git", "add", "data/fuel_consumption_master.csv"])
    subprocess.run(["git", "add", LAST_HASH_FILE])
    subprocess.run(["git", "commit", "-m", "Updated fuel consumption master dataset"])
    subprocess.run(["git", "push"])
