#!/usr/bin/env python3
"""
Migration:
  1. Add our_ssi column to settlement_trades (if not exists)
  2. Populate back_office_auto.counterparty_ssi_mapping
     (table already exists: counterparty_id → ssi_id)
     with counterparty-SSI pairs from tab_standard_settlement_instructions
     filtered to role='Counterparty' only.
  3. Show current state of the mapping.
"""
import os
import sys
import psycopg2
import psycopg2.extras

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

        # ── 1. Add our_ssi to settlement_trades ───────────────────────────────
        cur.execute("""
            ALTER TABLE back_office_auto.settlement_trades
            ADD COLUMN IF NOT EXISTS our_ssi text;
        """)
        print("✓ our_ssi column ensured on settlement_trades")

        # ── 2. Populate counterparty_ssi_mapping ──────────────────────────────
        # Table already exists with: counterparty_id, ssi_id, trade_type, is_active
        # We insert pairs: (counterparty who is broker) → (their SSI records)
        # Only for counterparties with role='Counterparty', not personal accounts.
        cur.execute("""
            INSERT INTO back_office_auto.counterparty_ssi_mapping
                (counterparty_id, ssi_id, trade_type, is_active)
            SELECT
                tc.id   AS counterparty_id,
                ti.id   AS ssi_id,
                'DVP'   AS trade_type,
                true    AS is_active
            FROM back_office.tab_standard_settlement_instructions ti
            JOIN back_office.tab_counterparty tc ON ti.ssi_owner_id = tc.id
            WHERE tc.role = 'Counterparty'
              AND ti.ssi_name IS NOT NULL
              AND ti.ssi_name NOT IN ('DUMMY', 'FAB BILLING', 'AMWL-AED-OPPS', 'AMWL_FAB-USD-PROP')
            ON CONFLICT DO NOTHING;
        """)
        rows = cur.rowcount
        print(f"✓ Populated counterparty_ssi_mapping: {rows} new rows inserted")

        # ── 3. Show current mapping ────────────────────────────────────────────
        cur.execute("""
            SELECT
                tc.short_name   AS counterparty,
                tc.name         AS full_name,
                ti.ssi_name,
                tc2.short_name  AS custodian,
                ti.ac           AS account
            FROM back_office_auto.counterparty_ssi_mapping csm
            JOIN back_office.tab_counterparty tc ON csm.counterparty_id = tc.id
            JOIN back_office.tab_standard_settlement_instructions ti ON csm.ssi_id = ti.id
            LEFT JOIN back_office.tab_counterparty tc2 ON ti.agent_id = tc2.id
            WHERE csm.is_active = true
            ORDER BY tc.short_name, ti.ssi_name;
        """)
        rows_data = cur.fetchall()
        print(f"\n{'Counterparty':<15} {'SSI Name':<30} {'Custodian':<20} {'Account'}")
        print("-" * 80)
        for r in rows_data:
            print(f"{(r[0] or ''):<15} {(r[2] or ''):<30} {(r[3] or ''):<20} {r[4] or ''}")

    conn.commit()
    print(f"\nSUCCESS. Total active mappings: {len(rows_data)}")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    sys.exit(1)
finally:
    conn.close()
