-- ============================================================
-- Migration: Enable Row Level Security (RLS) on all public tables
-- Run this in Supabase SQL Editor (https://supabase.com/dashboard/project/glrygwduxhyayigsswcs/sql)
-- ============================================================

-- 1. Enable RLS on all tables to block unauthorized public access via anon key
ALTER TABLE IF EXISTS tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS csv_templates ENABLE ROW LEVEL SECURITY;

-- 2. (Optional / Safe Default) Drop overly permissive temporary policies if any
DROP POLICY IF EXISTS "Allow all csv_templates access" ON csv_templates;

-- 3. Explanation:
-- By enabling RLS without public policies, all access via the 'anon' (public) key is blocked.
-- Your Python/Streamlit backend application should use the 'service_role' key (secret key),
-- which bypasses RLS and continues to function normally while keeping your database secure.
