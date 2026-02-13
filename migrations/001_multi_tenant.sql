-- ============================================================
-- Migration: Multi-tenant preparation
-- Run in Supabase SQL Editor (https://supabase.com/dashboard)
-- ============================================================

-- 1. Add user_id column to existing tables
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS "user_id" TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS "user_id" TEXT;

-- 2. Create csv_templates table
CREATE TABLE IF NOT EXISTS csv_templates (
    "id" SERIAL PRIMARY KEY,
    "header_hash" TEXT NOT NULL,
    "user_id" TEXT,                    -- NULL = shared template (global)
    "label" TEXT,
    "mapping" JSONB NOT NULL,
    "columns" JSONB,
    "shared" BOOLEAN DEFAULT false,
    "created_at" TIMESTAMPTZ DEFAULT now(),
    "updated_at" TIMESTAMPTZ DEFAULT now(),
    UNIQUE("header_hash", COALESCE("user_id", '__global__'))
);

-- 3. Enable RLS on new table
ALTER TABLE csv_templates ENABLE ROW LEVEL SECURITY;

-- 4. Temporary open policy (restrict later when auth is added)
CREATE POLICY "Allow all csv_templates access" ON csv_templates FOR ALL USING (true);

-- 5. Create index for fast lookup
CREATE INDEX IF NOT EXISTS idx_csv_templates_hash ON csv_templates("header_hash");
CREATE INDEX IF NOT EXISTS idx_tenants_user_id ON tenants("user_id");
CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments("user_id");
