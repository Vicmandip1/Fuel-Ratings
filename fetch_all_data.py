import requests
import os
import pandas as pd

# API Endpoint
API_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search?resource_id=d589f2bc-9a85-4f65-be2f-20f17debfcb1&limit=5000"

# File paths
DATA_FOLDER = "data/"
MASTER_FILE = os.path.join(DATA_FOLDER, "fuel_consumption_master.csv")

# Ensure data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

def fetch_all_data():
    """Fetch all available data from API"""
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        return data.get("result", {}).get("records", [])
    print("Failed to retrieve data.")
    return None

def save_data(records):
    """Save the full dataset to a CSV file"""
    if records:
        df = pd.DataFrame(records)
        
        # Save to CSV file
        df.to_csv(MASTER_FILE, index=False, encoding="utf-8")
        print(f"✅ Data successfully saved to {MASTER_FILE}")
    else:
        print("❌ No data retrieved.")

if __name__ == "__main__":
    print("📥 Fetching all data from API...")
    records = fetch_all_data()
    save_data(records)
