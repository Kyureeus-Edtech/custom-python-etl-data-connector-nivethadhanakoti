import os
import time
import requests
import pymongo
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

BASE_URL = "https://www.dshield.org/ipsascii.html"
LOCAL_FALLBACK_FILE = "dshield_fallback.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

def extract_data():
    print("[INFO] Fetching data from DShield with retries...")
    retries = 3
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                BASE_URL, 
                headers=HEADERS,
                timeout=60,         # longer timeout
                verify=False        # bypass SSL cert issues
            )
            response.raise_for_status()

            # Save a local copy for future fallback
            with open(LOCAL_FALLBACK_FILE, "w", encoding="utf-8") as f:
                f.write(response.text)

            print("[DEBUG] First 5 lines of feed:")
            for line in response.text.split("\n")[:5]:
                print(line)
            return response.text

        except requests.exceptions.RequestException as e:
            print(f"[WARNING] Attempt {attempt} failed: {e}")
            if attempt < retries:
                sleep_time = 5 * attempt
                print(f"[INFO] Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("[ERROR] All retries failed.")
    
    # Fallback to local file if available
    if os.path.exists(LOCAL_FALLBACK_FILE):
        print("[INFO] Using local fallback file instead.")
        with open(LOCAL_FALLBACK_FILE, encoding="utf-8") as f:
            return f.read()
    else:
        raise RuntimeError("Could not fetch DShield data and no fallback file exists.")

def transform_data(raw_text):
    print("[INFO] Transforming data...")
    data = []
    for line in raw_text.strip().split("\n"):
        if line.startswith("#") or not line.strip():
            continue

        parts = line.split("\t")
        if len(parts) == 5:
            ip, asn, cc, attacks, name = parts
        elif len(parts) == 6:
            ip, asn, cc, date_str, attacks, name = parts
        else:
            print(f"[WARNING] Unexpected format: {parts}")
            continue

        try:
            record = {
                "ip": ip,
                "asn": int(asn) if asn.isdigit() else None,
                "country_code": cc,
                "attacks": int(attacks) if attacks.isdigit() else None,
                "name": name if name else None,
                "ingested_at": datetime.utcnow()
            }
            data.append(record)
        except Exception as e:
            print(f"[WARNING] Skipping line due to error: {e}")

    return data

def load_data(records):
    print("[INFO] Loading data into MongoDB...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["dshield_raw"]
    if records:
        collection.insert_many(records)
        print(f"[INFO] Inserted {len(records)} records into MongoDB.")
    else:
        print("[WARNING] No data to insert.")

def run_etl():
    try:
        raw_text = extract_data()
        records = transform_data(raw_text)
        load_data(records)
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    run_etl()
