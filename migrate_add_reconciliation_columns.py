#!/usr/bin/env python3
"""
Migration: Add new columns to settlement_reconciliation table
Columns: matched_internal_ids, matched_internal_count, mismatch_json, 
         external_price, internal_price, external_value_date, internal_value_date
"""
import os
import sys
import psycopg2

PG_CONN_STRING = os.environ.get("PG_CONN_STRING", "").strip()

if not PG_CONN_STRING:
    print("ERROR: Missing PG_CONN_STRING environment variable")
    sys.exit(1)

conn_str = PG_CONN_STRING
if "sslmode=" not in conn_str.lower():
    conn_str += " sslmode=require"

print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(conn_str, connect_timeout=30)
except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)

try:
    with conn.cursor() as cur:
        # Execute ALTER TABLE with ADD COLUMN IF NOT EXISTS for each column
        cur.execute("""
            ALTER TABLE back_office_auto.settlement_reconciliation
            ADD COLUMN IF NOT EXISTS matched_internal_ids text,
            ADD COLUMN IF NOT EXISTS matched_internal_count integer,
            ADD COLUMN IF NOT EXISTS mismatch_json jsonb,
            ADD COLUMN IF NOT EXISTS external_price numeric,
            ADD COLUMN IF NOT EXISTS internal_price numeric,
            ADD COLUMN IF NOT EXISTS external_value_date date,
            ADD COLUMN IF NOT EXISTS internal_value_date date;
        """)
    conn.commit()
    print("SUCCESS: All columns added to settlement_reconciliation table")
    
except psycopg2.errors.Error as e:
    print(f"Database error: {e}")
    conn.rollback()
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error: {e}")
    conn.rollback()
    sys.exit(1)
finally:
    conn.close()
