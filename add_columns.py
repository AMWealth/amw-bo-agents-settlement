#!/usr/bin/env python3
"""Add new columns to settlement_reconciliation table"""
import os
import psycopg2

PG_CONN_STRING = os.environ.get("PG_CONN_STRING", "").strip()

if not PG_CONN_STRING:
    raise RuntimeError("Missing PG_CONN_STRING environment variable")

conn_str = PG_CONN_STRING
if "sslmode=" not in conn_str.lower():
    conn_str += " sslmode=require"

conn = psycopg2.connect(conn_str, connect_timeout=30)

try:
    with conn.cursor() as cur:
        cur.execute("""
            alter table back_office_auto.settlement_reconciliation
                add column if not exists matched_internal_ids text,
                add column if not exists matched_internal_count integer,
                add column if not exists mismatch_json jsonb,
                add column if not exists external_price numeric,
                add column if not exists internal_price numeric,
                add column if not exists external_value_date date,
                add column if not exists internal_value_date date;
        """)
    conn.commit()
    print("✓ Successfully added columns to settlement_reconciliation table")
except Exception as e:
    print(f"✗ Error: {e}")
    conn.rollback()
    raise
finally:
    conn.close()
