#!/usr/bin/env python3
"""
Migration:
  1. Add our_ssi column to settlement_trades (if not exists)
  2. Create back_office_auto.cpty_ssi_mapping table
  3. Populate cpty_ssi_mapping from back_office.tab_standard_settlement_instructions
     (only counterparties with role='Counterparty', excluding personal accounts)
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

        # ── 2. Create cpty_ssi_mapping ────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS back_office_auto.cpty_ssi_mapping (
                id          SERIAL PRIMARY KEY,
                ssi_owner   TEXT NOT NULL,   -- short_name from tab_counterparty (e.g. CAMCAP, BRIDPORT)
                broker_name TEXT,            -- broker_name in settlement_trades (fill manually)
                ssi_name    TEXT NOT NULL,   -- canonical SSI name (e.g. CAM-ECLR-50282)
                custodian   TEXT,            -- agent short_name (e.g. EUROCLEAR, CLEARSTREAM, DTC)
                account     TEXT,            -- account number/code for fuzzy matching (e.g. 50282, 0067)
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (ssi_name)
            );
        """)
        print("✓ cpty_ssi_mapping table ensured")

        # ── 3. Populate from SSI directory (counterparties only) ──────────────
        cur.execute("""
            INSERT INTO back_office_auto.cpty_ssi_mapping
                (ssi_owner, ssi_name, custodian, account)
            SELECT
                tc1.short_name                          AS ssi_owner,
                ti.ssi_name                             AS ssi_name,
                tc2.short_name                          AS custodian,
                ti.ac                                   AS account
            FROM back_office.tab_standard_settlement_instructions ti
            LEFT JOIN back_office.tab_counterparty tc1 ON ti.ssi_owner_id = tc1.id
            LEFT JOIN back_office.tab_counterparty tc2 ON ti.agent_id     = tc2.id
            WHERE tc1.role = 'Counterparty'
              AND ti.ssi_name IS NOT NULL
              AND ti.ssi_name NOT IN ('DUMMY', 'FAB BILLING', 'AMWL-AED-OPPS', 'AMWL_FAB-USD-PROP')
            ON CONFLICT (ssi_name) DO UPDATE SET
                ssi_owner = excluded.ssi_owner,
                custodian = excluded.custodian,
                account   = excluded.account;
        """)
        rows = cur.rowcount
        print(f"✓ Populated cpty_ssi_mapping: {rows} rows inserted/updated")

        # ── 4. Show what was loaded ───────────────────────────────────────────
        cur.execute("""
            SELECT ssi_owner, ssi_name, custodian, account, broker_name
            FROM back_office_auto.cpty_ssi_mapping
            ORDER BY ssi_owner, ssi_name;
        """)
        rows_data = cur.fetchall()
        print(f"\n{'SSI Owner':<20} {'SSI Name':<30} {'Custodian':<20} {'Account':<15} {'Broker Name'}")
        print("-" * 95)
        for r in rows_data:
            print(f"{(r[0] or ''):<20} {(r[1] or ''):<30} {(r[2] or ''):<20} {(r[3] or ''):<15} {r[4] or '(needs mapping)'}")

    conn.commit()
    print("\nSUCCESS. Next step: fill in broker_name column for each ssi_owner.")
    print("Example:")
    print("  UPDATE back_office_auto.cpty_ssi_mapping SET broker_name = 'CAMcap Markets Ltd.' WHERE ssi_owner = 'CAMCAP';")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    sys.exit(1)
finally:
    conn.close()
