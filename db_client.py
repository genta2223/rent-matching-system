import os
import requests
import pandas as pd

# Try loading .env for local development (skip silently on cloud)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Read credentials: st.secrets (Streamlit Cloud) → os.environ (.env / local)
def _get_secret(key):
    try:
        import streamlit as st
        return st.secrets.get(key)
    except Exception:
        pass
    return os.environ.get(key)

SUPABASE_URL = _get_secret("SUPABASE_URL")
SUPABASE_KEY = _get_secret("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env or Streamlit secrets")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

class DBClient:
    def __init__(self):
        self.base_url = SUPABASE_URL
        self.headers = HEADERS

    def fetch_tenants(self):
        """Fetch all tenants as a DataFrame."""
        url = f"{self.base_url}/rest/v1/tenants?select=*"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            raise Exception(f"Failed to fetch tenants: {response.text}")

    def fetch_payments(self):
        """Fetch all payments as a DataFrame."""
        url = f"{self.base_url}/rest/v1/payments?select=*"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            raise Exception(f"Failed to fetch payments: {response.text}")

    def upsert_payments(self, records):
        """Bulk upsert payments."""
        url = f"{self.base_url}/rest/v1/payments"
        # Prefer: resolution=merge-duplicates is needed for upsert behavior if not default
        headers = self.headers.copy()
        headers["Prefer"] = "resolution=merge-duplicates"
        
        # Clean records (NaN handling)
        cleaned_records = [self._clean_record(r) for r in records]
        
        response = requests.post(url, headers=headers, json=cleaned_records)
        if response.status_code in (200, 201):
            return response.json() if response.content else []
        else:
            raise Exception(f"Failed to upsert payments: {response.text}")

    def upsert_tenants(self, records):
        """Bulk upsert tenants."""
        url = f"{self.base_url}/rest/v1/tenants"
        headers = self.headers.copy()
        headers["Prefer"] = "resolution=merge-duplicates"
        
        cleaned_records = [self._clean_record(r) for r in records]
        
        response = requests.post(url, headers=headers, json=cleaned_records)
        if response.status_code in (200, 201):
            return response.json() if response.content else []
        else:
            raise Exception(f"Failed to upsert tenants: {response.text}")

    def update_tenant(self, property_id, data):
        """Update a single tenant."""
        url = f"{self.base_url}/rest/v1/tenants?PropertyID=eq.{property_id}"
        cleaned_data = self._clean_record(data)
        
        response = requests.patch(url, headers=self.headers, json=cleaned_data)
        if response.status_code in (200, 204):
            return True
        else:
            raise Exception(f"Failed to update tenant {property_id}: {response.text}")

    def _clean_record(self, record):
        """Helper to ensure JSON compatibility (handle NaN/Inf) recursively."""
        import math
        import numpy as np
        
        if isinstance(record, dict):
            new_record = {}
            for k, v in record.items():
                new_record[k] = self._clean_record(v)
            return new_record
        elif isinstance(record, list):
            return [self._clean_record(i) for i in record]
        elif isinstance(record, (float, np.floating)):
            if math.isnan(record) or math.isinf(record):
                return None
            return float(record)
        elif isinstance(record, (np.integer, np.int64)):
            return int(record)
        else:
            return record
