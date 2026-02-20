import os
import sys
import requests
# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import DBClient

def clean_templates():
    db = DBClient()
    target_hash = "731cc86e"
    
    print(f"Attempting to delete template hash: {target_hash}")
    try:
        url = f"{db.base_url}/rest/v1/csv_templates?header_hash=eq.{target_hash}"
        response = requests.delete(url, headers=db.headers)
        if response.status_code in (200, 204):
            print(f"Delete successful. Status: {response.status_code}")
        else:
            print(f"Delete failed: {response.text}")
    except Exception as e:
        print(f"Error deleting template: {e}")

if __name__ == "__main__":
    clean_templates()
