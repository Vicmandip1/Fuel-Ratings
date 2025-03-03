import requests
import os
import subprocess
from datetime import datetime

# API Endpoint for the dataset
API_URL = "https://open.canada.ca/data/en/api/3/action/datastore_search?resource_id=d589f2bc-9a85-4f65-be2f-20f17debfcb1&limit=5"

# Directory where files will be saved in the GitHub repo
DATA_FOLDER = "data/"

def get_latest_file_info():
    """Fetch the latest file details from the API."""
    response = requests.get(API_URL)

    if response.status_code == 200:
        data = response.json()
        
        if 'result' in data and 'records' in data['result']:
            records = data['result']['records']
            
            if records:
                latest_record = records[0]
                latest_title = latest_record.get("title", "Unknown_File")
                latest_timestamp = latest_record.get("_last_updated", str(datetime.now()))
                latest_file_url = latest_record.get("file_url", None)
                
                return latest_title, latest_timestamp, latest_file_url
    return None, None, None

def download_file(file_url, filename):
    """Download the latest file."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    filepath = os.path.join(DATA_FOLDER, filename)

    response = requests.get(file_url)
    if response.status_code == 200:
        with open(filepath, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")
    else:
        print("Failed to download file.")

def commit_and_push_changes(filename):
    """Commit and push new files to GitHub."""
    subprocess.run(["git", "config", "--global", "user.email", "your-email@example.com"])
    subprocess.run(["git", "config", "--global", "user.name", "your-github-username"])
    
    subprocess.run(["git", "add", DATA_FOLDER])
    subprocess.run(["git", "commit", "-m", f"Added new data file: {filename}"])
    subprocess.run(["git", "push"])

def check_for_updates():
    """Check for new updates and upload to GitHub if available."""
    latest_title, latest_timestamp, latest_file_url = get_latest_file_info()

    if latest_title and latest_file_url:
        latest_filename = f"{latest_title.replace(' ', '_')}.csv"
        existing_files = os.listdir(DATA_FOLDER)

        if latest_filename not in existing_files:
            print(f"New update found: {latest_title} (Updated: {latest_timestamp})")
            download_file(latest_file_url, latest_filename)
            commit_and_push_changes(latest_filename)
        else:
            print("No new updates.")
    else:
        print("Could not retrieve latest file information.")

# Run the function
check_for_updates()
