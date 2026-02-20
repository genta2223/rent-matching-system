from db_client import DBClient, SUPABASE_URL, HEADERS
import requests

def cleanup_220_data():
    client = DBClient()
    print("Checking for 2/20 payment data...")
    
    # Check max date
    try:
        df = client.fetch_payments()
        if df.empty:
            print("No payments found in DB.")
        else:
            print(f"Current max payment date in DB: {df['Date'].max()}")
            
        print("Executing delete query for payments on or after 2026-02-20...")
        url = f"{SUPABASE_URL}/rest/v1/payments?Date=gte.2026-02-20"
        response = requests.delete(url, headers=HEADERS)
        
        if response.status_code in (200, 204):
            print("Successfully cleaned up 2/20 and later payment data.")
        else:
            print(f"Failed to delete: {response.text}")
            
    except Exception as e:
        print(f"Error during cleanup: {e}")

if __name__ == "__main__":
    cleanup_220_data()
