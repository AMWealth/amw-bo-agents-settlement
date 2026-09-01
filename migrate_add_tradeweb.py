#!/usr/bin/env python3
"""
Migration: new counterparty Tradeweb (added 2026-09-01).

  1. Add sender adouggui@eusers.tradeweb.com to
     back_office_auto.counterparty_email_mapping (template TRADEWEB_PDF).
     (The code also has a domain fallback for *@eusers.tradeweb.com /
     *@tradeweb.com, so this row is mainly for visibility in the mapping table.)
  2. Link the TRADE WEB counterparty to its default SSI "TRADE WEB EC 57159"
     in back_office_auto.counterparty_ssi_mapping, so enrich_cpty_ssi()
     resolves it automatically (single-SSI fallback).
  3. Show the resulting state.

Prerequisites (already present in the BO system, verified 2026-09-01):
  - back_office.tab_counterparty id=271 "Tradeweb Execution Services Limited"
    (short_name TESL, role='Counterparty')
  - back_office.tab_standard_settlement_instructions id=178 "TRADE WEB EC 57159"
    (ac=57159, agent Euroclear Bank SA, ssi_owner_id=271)

Run: PG_CONN_STRING=... python migrate_add_tradeweb.py
"""
import os
import sys
import psycopg2
import psycopg2.extras

SENDER = "adouggui@eusers.tradeweb.com"
TEMPLATE = "TRADEWEB_PDF"
SSI_NAME = "TRADE WEB EC 57159"

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
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # ── 1. counterparty_email_mapping ─────────────────────────────────────
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'back_office_auto'
              AND table_name = 'counterparty_email_mapping'
        """)
        cols = {r["column_name"] for r in cur.fetchall()}
        print(f"counterparty_email_mapping columns: {sorted(cols)}")

        cur.execute("""
            SELECT * FROM back_office_auto.counterparty_email_mapping
            WHERE LOWER(email_address_of_counterparty) = %s
        """, (SENDER,))
        existing = cur.fetchone()
        if existing:
            print(f"= mapping row already exists for {SENDER} "
                  f"(template_code={existing.get('template_code')})")
        else:
            desired = {
                "email_address_of_counterparty": SENDER,
                "counterparty": "Tradeweb Execution Services Limited",
                "counterparty_alias": "Tradeweb",
                "template_code": TEMPLATE,
                "is_active": True,
            }
            row = {k: v for k, v in desired.items() if k in cols}
            names = ", ".join(row)
            marks = ", ".join(["%s"] * len(row))
            cur.execute(
                f"INSERT INTO back_office_auto.counterparty_email_mapping ({names}) "
                f"VALUES ({marks})",
                list(row.values()),
            )
            print(f"+ inserted mapping row for {SENDER}: {row}")

        # ── 2. counterparty + SSI lookup ──────────────────────────────────────
        cur.execute("""
            SELECT id, name, short_name, role
            FROM back_office.tab_counterparty
            WHERE (name ILIKE '%%trade%%web%%' OR short_name ILIKE '%%trade%%web%%'
                   OR name ILIKE '%%tradeweb%%' OR short_name ILIKE '%%tradeweb%%')
        """)
        cps = cur.fetchall()
        print(f"tab_counterparty candidates: "
              f"{[(r['id'], r['name'], r['short_name'], r['role']) for r in cps]}")
        cps = [r for r in cps if r["role"] == "Counterparty"] or cps
        if len(cps) != 1:
            print("ERROR: expected exactly 1 TRADE WEB counterparty — "
                  "create it in the BO system first (role='Counterparty'), then re-run.")
            conn.rollback()
            sys.exit(1)
        counterparty_id = cps[0]["id"]

        cur.execute("""
            SELECT id, ssi_name, ac, agent_id, ssi_owner_id
            FROM back_office.tab_standard_settlement_instructions
            WHERE ssi_name = %s OR ssi_name ILIKE '%%trade%%web%%'
        """, (SSI_NAME,))
        ssis = cur.fetchall()
        print(f"SSI candidates: {[(r['id'], r['ssi_name'], r['ac']) for r in ssis]}")
        exact = [r for r in ssis if r["ssi_name"] == SSI_NAME]
        ssis = exact or ssis
        if len(ssis) != 1:
            print(f"ERROR: expected exactly 1 SSI '{SSI_NAME}' — "
                  "create it in the BO system first, then re-run.")
            conn.rollback()
            sys.exit(1)
        ssi_id = ssis[0]["id"]

        # ── 3. counterparty_ssi_mapping ───────────────────────────────────────
        cur.execute("""
            INSERT INTO back_office_auto.counterparty_ssi_mapping
                (counterparty_id, ssi_id, trade_type, is_active)
            VALUES (%s, %s, 'DVP', true)
            ON CONFLICT DO NOTHING
        """, (counterparty_id, ssi_id))
        print(f"+ counterparty_ssi_mapping ensured "
              f"(counterparty_id={counterparty_id}, ssi_id={ssi_id}, rowcount={cur.rowcount})")

        # ── 4. Show state ─────────────────────────────────────────────────────
        cur.execute("""
            SELECT tc.short_name AS counterparty, ti.ssi_name, ti.ac, csm.is_active
            FROM back_office_auto.counterparty_ssi_mapping csm
            JOIN back_office.tab_counterparty tc ON csm.counterparty_id = tc.id
            JOIN back_office.tab_standard_settlement_instructions ti ON csm.ssi_id = ti.id
            WHERE csm.counterparty_id = %s
        """, (counterparty_id,))
        for r in cur.fetchall():
            print(f"  {r['counterparty']:<15} {r['ssi_name']:<30} "
                  f"ac={r['ac']} active={r['is_active']}")

    conn.commit()
    print("\r\nSUCCESS")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    sys.exit(1)
finally:
    conn.close()
