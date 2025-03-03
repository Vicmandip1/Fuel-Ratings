import requests
import os
import csv
import subprocess
import hashlib
from datetime import datetime

# API Endpoint for the dataset
API_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search?resource_id=d589f2bc-9a85-4f65-be2f-20f17debfcb1&limit=5000"

# Directory where files will be saved in the GitHub repo
DATA_FOLDER = "data/"
LAST_HASH_FILE = "last_hash.txt"  # File to store hash of last dataset

def fetch_all_data():
    """Fetch all available fuel consumption data from the API."""
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()

        if 'result' in data and 'records' in data['result']:
            return data['result']['records']
    
    print("Failed to retrieve data.")
    return None

def calculate_hash(records):
    """Calculate a hash of the dataset to detect changes."""
    data_string = "".join(str(record) for record in records)
    return hashlib.md5(data_string.encode()).hexdigest()

def has_data_changed(new_hash):
    """Check if data has changed by comparing with the last saved hash."""
    if os.path.exists(LAST_HASH_FILE):
        with open(LAST_HASH_FILE, "r") as file:
            last_hash = file.read().strip()
        return last_hash != new_hash
    return True  # If no hash file exists, assume data is new

def save_data_to_csv(records):
    """Save API records into a CSV file."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    filename = f"fuel_consumption_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(DATA_FOLDER, filename)

    if records:
        keys = records[0].keys()
        with open(filepath, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(records)

        print(f"Saved data to {filepath}")
        return filename
    else:
        print("No data available to save.")
        return None

def commit_and_push_changes(filename):
    """Commit and push new files to GitHub."""
    if filename:
        subprocess.run(["git", "config", "--global", "user.email", "your-email@example.com"])
        subprocess.run(["git", "config", "--global", "user.name", "your-github-username"])

        subprocess.run(["git", "add", os.path.join(DATA_FOLDER, filename)])
        subprocess.run(["git", "commit", "-m", f"Added new data file: {filename}"])
        subprocess.run(["git", "push"])
        print("Changes committed and pushed to GitHub.")
    else:
        print("No new data to commit.")

def main():
    """Main function to fetch, save, and update GitHub repo."""
    records = fetch_all_data()

    if records:
        new_hash = calculate_hash(records)

        if has_data_changed(new_hash):
            filename = save_data_to_csv(records)
            commit_and_push_changes(filename)

            # Save new hash to prevent duplicate downloads
            with open(LAST_HASH_FILE, "w") as file:
                file.write(new_hash)
        else:
            print("No new updates found. Skipping download.")
    else:
        print("Failed to retrieve data.")

# Run the script
if __name__ == "__main__":
    main()
