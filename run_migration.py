"""Apply SQL migration to Supabase via different API endpoints."""
import os
import requests
from dotenv import load_dotenv
load_dotenv()

url = os.environ['SUPABASE_URL']
key = os.environ['SUPABASE_KEY']
headers = {
    'apikey': key,
    'Authorization': f'Bearer {key}',
    'Content-Type': 'application/json',
}

# SQL statements to execute
sqls = [
    'ALTER TABLE tenants ADD COLUMN IF NOT EXISTS "user_id" TEXT',
    'ALTER TABLE payments ADD COLUMN IF NOT EXISTS "user_id" TEXT',
    """CREATE TABLE IF NOT EXISTS csv_templates (
        "id" SERIAL PRIMARY KEY,
        "header_hash" TEXT NOT NULL,
        "user_id" TEXT,
        "label" TEXT,
        "mapping" JSONB NOT NULL,
        "columns" JSONB,
        "shared" BOOLEAN DEFAULT false,
        "created_at" TIMESTAMPTZ DEFAULT now(),
        "updated_at" TIMESTAMPTZ DEFAULT now(),
        UNIQUE("header_hash", COALESCE("user_id", '__global__'))
    )""",
    'ALTER TABLE csv_templates ENABLE ROW LEVEL SECURITY',
    """CREATE POLICY "Allow all csv_templates access" ON csv_templates FOR ALL USING (true)""",
    'CREATE INDEX IF NOT EXISTS idx_csv_templates_hash ON csv_templates("header_hash")',
    'CREATE INDEX IF NOT EXISTS idx_tenants_user_id ON tenants("user_id")',
    'CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments("user_id")',
]

# Method 1: Try Supabase SQL endpoint
print("=== Trying SQL execution methods ===\n")

# Try /pg/query (available in some Supabase versions)
full_sql = ";\n".join(sqls)
r = requests.post(f'{url}/pg/query', headers=headers, json={'query': full_sql})
print(f"Method 1 (/pg/query): status={r.status_code}")
if r.status_code == 200:
    print("SUCCESS!")
    print(r.text[:300])
else:
    print(f"Failed: {r.text[:200]}")
    
    # Try /rest/v1/rpc approach
    r2 = requests.post(f'{url}/rest/v1/rpc/exec_sql', headers=headers, json={'sql': full_sql})
    print(f"\nMethod 2 (rpc/exec_sql): status={r2.status_code}")
    if r2.status_code == 200:
        print("SUCCESS!")
    else:
        print(f"Failed: {r2.text[:200]}")
        print("\n=== Manual execution required ===")
        print("Please run the SQL in: migrations/001_multi_tenant.sql")
        print(f"Supabase Dashboard: {url.replace('/rest', '').replace('/v1', '')}")
