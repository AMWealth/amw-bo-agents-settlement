import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Assuming PG_CONN_STRING is set in environment
conn_string = os.getenv("PG_CONN_STRING")
if not conn_string:
    raise ValueError("PG_CONN_STRING not set")

conn = psycopg2.connect(conn_string)
conn.autocommit = True

with conn.cursor() as cur:
    # Delete from settlement_reconciliation
    cur.execute("""
        delete from back_office_auto.settlement_reconciliation
        where settlement_trade_id in (
            select id
            from back_office_auto.settlement_trades
            where internet_message_id = '<2033277491.80993.1773453204388@smtp.mfsnet.io>'
        );
    """)
    print("Deleted from settlement_reconciliation")

    # Delete from settlement_trades
    cur.execute("""
        delete from back_office_auto.settlement_trades
        where internet_message_id = '<2033277491.80993.1773453204388@smtp.mfsnet.io>';
    """)
    print("Deleted from settlement_trades")

    # Delete from settlement_files
    cur.execute("""
        delete from back_office_auto.settlement_files
        where internet_message_id = '<2033277491.80993.1773453204388@smtp.mfsnet.io>';
    """)
    print("Deleted from settlement_files")

    # Delete from settlement_emails
    cur.execute("""
        delete from back_office_auto.settlement_emails
        where internet_message_id = '<2033277491.80993.1773453204388@smtp.mfsnet.io>';
    """)
    print("Deleted from settlement_emails")

conn.close()
print("Cleanup completed")