import os
import sys
import pandas as pd
import requests

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import DBClient

def force_delete_date():
    db = DBClient()
    target_date = '2026-02-20'
    
    print(f"!!! EMERGENCY DELETION !!!")
    print(f"Targeting ALL payments dated {target_date}")
    
    # 1. Fetch all payments
    payments_df = db.fetch_payments()
    if payments_df.empty:
        print("No payments found in DB.")
        return

    # 2. Filter by date
    # Ensure DateStr format
    payments_df['DateStr'] = pd.to_datetime(payments_df['Date']).dt.strftime('%Y-%m-%d')
    targets = payments_df[payments_df['DateStr'] == target_date]
    
    if targets.empty:
        print(f"No records found for {target_date}.")
        return

    print(f"Found {len(targets)} records to delete.")
    print(targets[['PropertyID', 'Date', 'Amount', 'Summary']].head())

    # 3. Delete
    headers = db.headers
    base_url = db.base_url
    
    count = 0
    for _, row in targets.iterrows():
        pid = row.get('id') # Supabase ID
        if not pid:
            print(f"Skipping row with no ID: {row}")
            continue
            
        url = f"{base_url}/rest/v1/payments?id=eq.{pid}"
        resp = requests.delete(url, headers=headers)
        if resp.status_code in (200, 204):
            count += 1
            if count % 10 == 0:
                print(f"Deleted {count}/{len(targets)}...")
        else:
            print(f"Failed to delete {pid}: {resp.text}")
            
    print(f"Deletion complete. Removed {count} records.")
    
    # 4. Verify
    print("Verifying deletion...")
    verify_df = db.fetch_payments()
    if not verify_df.empty and 'Date' in verify_df.columns:
        verify_df['DateStr'] = pd.to_datetime(verify_df['Date']).dt.strftime('%Y-%m-%d')
        remaining = verify_df[verify_df['DateStr'] == target_date]
        if not remaining.empty:
            print(f"CRITICAL WARNING: {len(remaining)} records still remain!")
            print(remaining[['id', 'PropertyID', 'Date', 'Amount']])
        else:
            print(f"SUCCESS: Verified 0 records found for {target_date}.")
    else:
        print(f"SUCCESS: Verified 0 records (DB empty or no Date col).")

if __name__ == "__main__":
    force_delete_date()
