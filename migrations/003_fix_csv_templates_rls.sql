-- ============================================================
-- Migration: Fix Row Level Security (RLS) on csv_templates table
-- Run this in Supabase SQL Editor:
-- https://supabase.com/dashboard/project/glrygwduxhyayigsswcs/sql
-- ============================================================

-- 1. Enable RLS on csv_templates (if not already enabled)
ALTER TABLE IF EXISTS csv_templates ENABLE ROW LEVEL SECURITY;

-- 2. Drop existing restrictive or incomplete policies
DROP POLICY IF EXISTS "Allow all csv_templates access" ON csv_templates;
DROP POLICY IF EXISTS "Allow anon read/write csv_templates" ON csv_templates;

-- 3. Create full access policy including WITH CHECK for INSERT/UPDATE
CREATE POLICY "Allow all csv_templates access"
ON csv_templates
FOR ALL
USING (true)
WITH CHECK (true);

-- 4. Grant table permissions to anon and authenticated roles
GRANT ALL ON TABLE csv_templates TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO anon, authenticated, service_role;
