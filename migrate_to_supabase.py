import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase Credentials
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" # for upsert
}

import math
import numpy as np

def clean_record(record):
    new_record = {}
    for k, v in record.items():
        if isinstance(v, dict):
            new_record[k] = clean_record(v)
        elif isinstance(v, list):
            new_record[k] = [clean_record(i) if isinstance(i, dict) else i for i in v]
        else:
            if isinstance(v, float) or isinstance(v, np.floating):
                if math.isnan(v) or math.isinf(v):
                    new_record[k] = None
                else:
                    new_record[k] = float(v)
            elif isinstance(v, (np.integer, np.int64)):
                new_record[k] = int(v)
            else:
                new_record[k] = v
    return new_record

def migrate_tenants():
    print("Migrating Tenants...")
    csv_path = "private_data/rent_roll.csv"
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    df = pd.read_csv(csv_path)
    # Ensure NaN are None for JSON (Convert to object first to allow None in float cols)
    df = df.astype(object).where(pd.notnull(df), None)
    
    import sys
    # Debug
    print(f"Columns: {df.columns.tolist()}")
    sys.stdout.flush()
    
    records = []
    for i, row in df.iterrows():
        # Debug: Check first row alignment
        if i == 0:
            print(f"Row 0 Data: {row.to_dict()}")
            sys.stdout.flush()
        
        try:
            raw_debt = row.get('BaseDebtAmount', 0)
            if raw_debt:
                base_debt = float(raw_debt)
            else:
                base_debt = 0.0
        except ValueError:
            print(f"Error parsing BaseDebtAmount for Row {i} (Prop {row.get('PropertyID')}): '{raw_debt}'")
            base_debt = 0.0
            
        base_debt_date = row.get('BaseDebtDate')
        if base_debt_date:
            base_debt_date = str(base_debt_date)

        try:
            raw_rent = row.get('MonthlyRent')
            if pd.notna(raw_rent):
                rent = int(raw_rent)
            else:
                rent = 0
        except:
            rent = 0

        record = {
            "PropertyID": str(row['PropertyID']),
            "Name": row['TenantName'],
            "MonthlyRent": rent,
            "BaseDebtAmount": base_debt if pd.notna(base_debt) else 0.0,
            "BaseDebtDate": base_debt_date,
            "Zip": row.get('Zip'),
            "Address": row.get('Address'),
            "Tel": row.get('Tel'),
            "Memo": row.get('Memo'),
            "LatestPaymentMemo": row.get('LatestPaymentMemo'),
            "Values": {
                "Agent": row.get('Agent'),
                "Manager": row.get('Manager'),
                "BankMatchName1": row.get('BankMatchName1')
            }
        }
        records.append(clean_record(record))
        
    # Bulk upsert to 'tenants' table
    url = f"{SUPABASE_URL}/rest/v1/tenants"
    try:
        response = requests.post(url, headers=HEADERS, json=records)
        if response.status_code in (200, 201):
             print(f"Successfully migrated {len(records)} tenants.")
        else:
             print(f"Error migrating tenants: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exception during tenant migration: {e}")

def migrate_payments():
    print("Migrating Payments...")
    csv_path = "private_data/payment_ledger.csv"
    if not os.path.exists(csv_path):
        print("No payment ledger found.")
        return

    df = pd.read_csv(csv_path)
    df = df.astype(object).where(pd.notnull(df), None)
    
    tenants_df = pd.read_csv("private_data/rent_roll.csv")
    valid_ids = set(tenants_df['PropertyID'].astype(str))

    records = []
    for _, row in df.iterrows():
        prop_id = str(row['PropertyID'])
        if prop_id.endswith('.0'): 
            prop_id = prop_id[:-2]
            
        if prop_id not in valid_ids:
            # print(f"Skipping payment for unknown PropertyID: {prop_id}")
            continue
            
        record = {
            "PropertyID": prop_id,
            "Date": row['PaymentDate'],
            "Amount": float(row['Amount']),
            "Summary": row['Summary'],
            "TransactionKey": row['TransactionKey'],
            "AllocationDesc": row.get('AllocationDesc')
        }
        records.append(clean_record(record))
    
    # Bulk upsert to 'payments' table (on_conflict=TransactionKey is handled by 'id' usually, but here we need constraint)
    # The schema should have UNQIUE constraint on TransactionKey for upsert to work effectively with resolution=merge-duplicates.
    # Note: requests post with Prefer: resolution=merge-duplicates requires the table to have a unique constraint that matches the conflict.
    # In our schema we defined "TransactionKey" TEXT UNIQUE, so it should work.

    url = f"{SUPABASE_URL}/rest/v1/payments?on_conflict=TransactionKey"
    try:
        response = requests.post(url, headers=HEADERS, json=records)
        if response.status_code in (200, 201):
             print(f"Successfully migrated {len(records)} payments.")
        else:
             print(f"Error migrating payments: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exception during payment migration: {e}")

if __name__ == "__main__":
    migrate_tenants()
    migrate_payments()
