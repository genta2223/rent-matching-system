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

    # ------------------------------------------------------------------
    # Tenants
    # ------------------------------------------------------------------

    def fetch_tenants(self, user_id=None):
        """Fetch tenants as a DataFrame. Optionally filter by user_id."""
        url = f"{self.base_url}/rest/v1/tenants?select=*"
        if user_id:
            url += f"&user_id=eq.{user_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            raise Exception(f"Failed to fetch tenants: {response.text}")

    def upsert_tenants(self, records, user_id=None):
        """Bulk upsert tenants. Optionally stamp user_id on each record."""
        url = f"{self.base_url}/rest/v1/tenants"
        headers = self.headers.copy()
        headers["Prefer"] = "resolution=merge-duplicates"

        cleaned_records = []
        for r in records:
            rec = self._clean_record(r)
            if user_id:
                rec["user_id"] = user_id
            cleaned_records.append(rec)

        response = requests.post(url, headers=headers, json=cleaned_records)
        if response.status_code in (200, 201):
            return response.json() if response.content else []
        else:
            raise Exception(f"Failed to upsert tenants: {response.text}")

    def update_tenant(self, property_id, data, user_id=None):
        """Update a single tenant by PropertyID."""
        url = f"{self.base_url}/rest/v1/tenants?PropertyID=eq.{property_id}"
        if user_id:
            url += f"&user_id=eq.{user_id}"
        cleaned_data = self._clean_record(data)
        response = requests.patch(url, headers=self.headers, json=cleaned_data)
        if response.status_code in (200, 204):
            return True
        else:
            raise Exception(f"Failed to update tenant {property_id}: {response.text}")

    def delete_tenant(self, property_id, user_id=None):
        """Delete a tenant by PropertyID, first deleting their associated payments to satisfy FK constraint."""
        # 1. Delete payments
        url_payments = f"{self.base_url}/rest/v1/payments?PropertyID=eq.{property_id}"
        if user_id:
            url_payments += f"&user_id=eq.{user_id}"
        res_p = requests.delete(url_payments, headers=self.headers)
        if res_p.status_code not in (200, 204):
            raise Exception(f"Failed to delete payments for tenant {property_id}: {res_p.text}")
            
        # 2. Delete tenant
        url_tenant = f"{self.base_url}/rest/v1/tenants?PropertyID=eq.{property_id}"
        if user_id:
            url_tenant += f"&user_id=eq.{user_id}"
        res_t = requests.delete(url_tenant, headers=self.headers)
        if res_t.status_code not in (200, 204):
            raise Exception(f"Failed to delete tenant {property_id}: {res_t.text}")
            
        return True

    # ------------------------------------------------------------------

    # Payments
    # ------------------------------------------------------------------

    def fetch_payments(self, user_id=None):
        """Fetch payments as a DataFrame. Optionally filter by user_id."""
        url = f"{self.base_url}/rest/v1/payments?select=*"
        if user_id:
            url += f"&user_id=eq.{user_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            raise Exception(f"Failed to fetch payments: {response.text}")

    def upsert_payments(self, records, user_id=None):
        """Bulk upsert payments. Optionally stamp user_id on each record."""
        url = f"{self.base_url}/rest/v1/payments"
        headers = self.headers.copy()
        headers["Prefer"] = "resolution=merge-duplicates"

        cleaned_records = []
        for r in records:
            rec = self._clean_record(r)
            if user_id:
                rec["user_id"] = user_id
            cleaned_records.append(rec)

        response = requests.post(url, headers=headers, json=cleaned_records)
        if response.status_code in (200, 201):
            return response.json() if response.content else []
        else:
            raise Exception(f"Failed to upsert payments: {response.text}")

    # ------------------------------------------------------------------
    # CSV Templates (Supabase-backed, replaces .csv_templates.json)
    # ------------------------------------------------------------------

    def fetch_csv_templates(self, user_id=None):
        """Fetch CSV templates. Returns user-specific + shared templates."""
        url = f"{self.base_url}/rest/v1/csv_templates?select=*"
        if user_id:
            # user's own templates OR shared ones
            url += f"&or=(user_id.eq.{user_id},shared.eq.true)"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch csv_templates: {response.text}")

    def lookup_csv_template(self, header_hash, user_id=None):
        """Look up a template by header_hash. Prefers user-specific over shared."""
        url = f"{self.base_url}/rest/v1/csv_templates?header_hash=eq.{header_hash}"
        if user_id:
            url += f"&or=(user_id.eq.{user_id},shared.eq.true)"
        url += "&order=user_id.desc.nullslast&limit=1"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            return data[0] if data else None
        else:
            raise Exception(f"Failed to lookup csv_template: {response.text}")

    def upsert_csv_template(self, header_hash, mapping, columns=None,
                            label='', user_id=None, shared=False):
        """Save or update a CSV template."""
        url = f"{self.base_url}/rest/v1/csv_templates"
        headers = self.headers.copy()
        headers["Prefer"] = "resolution=merge-duplicates"

        import json
        record = {
            "header_hash": header_hash,
            "user_id": user_id,  # None = global shared
            "label": label,
            "mapping": json.dumps(mapping, ensure_ascii=False) if isinstance(mapping, dict) else mapping,
            "columns": json.dumps(columns, ensure_ascii=False) if isinstance(columns, list) else columns,
            "shared": shared,
        }
        response = requests.post(url, headers=headers, json=[record])
        if response.status_code in (200, 201):
            return response.json() if response.content else []
        else:
            raise Exception(f"Failed to upsert csv_template: {response.text}")

    def delete_csv_template(self, header_hash, user_id=None):
        """Delete a CSV template by header_hash and user_id."""
        url = f"{self.base_url}/rest/v1/csv_templates?header_hash=eq.{header_hash}"
        if user_id:
            url += f"&user_id=eq.{user_id}"
        else:
            url += "&user_id=is.null"
        response = requests.delete(url, headers=self.headers)
        if response.status_code in (200, 204):
            return True
        else:
            raise Exception(f"Failed to delete csv_template: {response.text}")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

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
