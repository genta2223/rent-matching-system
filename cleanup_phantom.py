import os
import sys
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_client import DBClient

def cleanup_phantom_payments():
    db = DBClient()
    
    # 1. Fetch all payments
    print("Fetching payments...")
    payments_df = db.fetch_payments()
    
    if payments_df.empty:
        print("No payments found.")
        return

    # 2. Identify Phantom Payments
    # Condition: Date is 2026-02-20 (The date of the incident)
    # Target Tenants: 3, 9, 14 (as reported) + potentially others
    # We will look for 2026-02-20 payments for these ProeprtyIDs
    
    target_ids = ['3', '9', '14']
    target_date = '2026-02-20'
    
    # Filter for target date
    # Ensure Date column is string YYYY-MM-DD
    payments_df['DateStr'] = pd.to_datetime(payments_df['Date']).dt.strftime('%Y-%m-%d')
    
    phantom_candidates = payments_df[
        (payments_df['DateStr'] == target_date) & 
        (payments_df['PropertyID'].astype(str).str.split('.').str[0].isin(target_ids))
    ]
    
    if phantom_candidates.empty:
        print(f"No payments found for {target_ids} on {target_date}")
        return

    print(f"Found {len(phantom_candidates)} candidate phantom payments:")
    print(phantom_candidates[['PropertyID', 'Date', 'Amount', 'Summary']])
    
    # 3. SAFETY OVERRIDE: Delete ALL 2/20 records for these tenants
    # User instruction: "Existing 2/20 data complete deletion" for these tenants.
    # We will skip the sibling check because the user explicitly stated that 2/20 is WRONG and should be wiped.
    
    ids_to_delete = []
    
    for _, row in phantom_candidates.iterrows():
        pid = row['PropertyID']
        amt = row['Amount']
        summ = row['Summary']
        
        print(f"  [DELETING] Prop {pid} - {amt} - {summ} (Date: {target_date})")
        ids_to_delete.append(row['id'])

    # 4. Execute Delete
    if ids_to_delete:
        print(f"Deleting {len(ids_to_delete)} records...")
        # DBClient doesn't have a delete_payment method explicitly shown in view_file earlier,
        # checking if we can add it or if we need to implement ad-hoc deletion.
        # Check db_client.py again or use requests directly.
        # Since I can't check loop, I'll assume I need to add a method or use raw requests if method missing.
        # I'll try to add a delete method to DBClient or use the `id` to delete via existing connection logic if possible.
        # Actually, simpler to just use requests here since I know the credentials structure.
         
        # ... Wait, I should verify if 'id' exists. 
        # Inspecting 'id' column availability.
        if 'id' not in payments_df.columns:
            print("Error: 'id' column not found in payments dataframe. Cannot delete safely.")
            return

        headers = db.headers
        base_url = db.base_url
        
        import requests
        
        for pid in ids_to_delete:
            url = f"{base_url}/rest/v1/payments?id=eq.{pid}"
            resp = requests.delete(url, headers=headers)
            if resp.status_code in (200, 204):
                print(f"Deleted ID {pid}")
            else:
                print(f"Failed to delete ID {pid}: {resp.text}")
                
    else:
        print("No records to delete.")

if __name__ == "__main__":
    cleanup_phantom_payments()
