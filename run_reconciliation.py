"""
Скрипт для ручного запуска реконсиляции (settlement reconciliation).

Запускает ту же логику, что и таймер settlement_reconciliation_timer,
но вручную — для тестирования или внепланового запуска.

По умолчанию берёт T0 (сегодня Dubai) и T1 (вчера Dubai) — те же даты, что и таймер.
Можно указать конкретную дату через --date, или убрать фильтр через --all.

Переменные окружения:
  PG_CONN_STRING

Запуск:
  python run_reconciliation.py                    # T0+T1 окно (как таймер)
  python run_reconciliation.py --date 2025-05-14  # конкретная дата
  python run_reconciliation.py --all              # все PARSED сделки без фильтра дат
  python run_reconciliation.py --dry-run          # без сохранения в БД
"""

import argparse
import logging
import sys
from datetime import date as date_type, datetime
from decimal import Decimal
from function_app import (
    get_conn,
    run_settlement_reconciliation,
    start_agent_run,
    finish_agent_run,
    get_t0_t1_dates,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# Status display order and symbols
STATUS_SYMBOL = {
    "MATCHED":                          "OK ",
    "MATCHED_AGGREGATED":               "OK~",   # matched via netting
    "PARTIAL":                          "!  ",   # found but fields differ
    "SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE": "~  ",  # same ISIN, different date
    "NOT_FOUND":                        "???",
    "NO_CONFO":                         "---",   # internal deal, no confo found
}


def fmt(v, width=14) -> str:
    if v is None:
        return " " * width
    s = str(v)
    if isinstance(v, (int, float, Decimal)):
        s = f"{float(v):,.2f}"
    return s[:width].ljust(width)


def print_report(result: dict, date_from, date_to):
    detail_rows = result.get("detail_rows", [])
    unmatched = result.get("unmatched_internal", [])

    print()
    print("=" * 110)
    print(f"  SETTLEMENT RECONCILIATION REPORT")
    if date_from or date_to:
        print(f"  Trade dates: {date_from or '?'} → {date_to or '?'}")
    print("=" * 110)
    print()

    # ── Confo side (settlement_trades → tab_deals) ────────────────────────────
    print("── CONFO vs INTERNAL ─────────────────────────────────────────────────────────────────────────────────────")
    print(
        f"{'ST':3}  {'ISIN':12}  {'Side':4}  {'T-Date':10}  {'V-Date':10}  "
        f"{'Broker':18}  "
        f"{'Ext Qty':14}  {'Int Qty':14}  "
        f"{'Ext Amt':14}  {'Int Amt':14}  "
        f"{'Notes'}"
    )
    print("-" * 110)

    for row in detail_rows:
        symbol = STATUS_SYMBOL.get(row["status"], "?  ")
        ext_qty = fmt(row.get("ext_qty"))
        int_qty = fmt(row.get("int_qty"))
        ext_amt = fmt(row.get("ext_amount"))
        int_amt = fmt(row.get("int_amount"))

        # Flag mismatches with arrows
        qty_flag = "  " if row.get("int_qty") is None or row.get("ext_qty") is None else (
            "  " if abs(float(row["ext_qty"] or 0) - float(row["int_qty"] or 0)) < 0.01 else " !"
        )
        amt_flag = "  " if row.get("int_amount") is None or row.get("ext_amount") is None else (
            "  " if abs(float(row["ext_amount"] or 0) - float(row["int_amount"] or 0)) < 1.0 else " !"
        )

        notes = row.get("notes") or ""
        broker = (row.get("broker") or "")[:18]
        isin = (row.get("isin") or "")[:12]
        side = (row.get("side") or "")[:4]
        trade_date = str(row.get("trade_date") or "")[:10]
        value_date = str(row.get("value_date") or "")[:10]

        print(
            f"{symbol}  {isin:12}  {side:4}  {trade_date:10}  {value_date:10}  "
            f"{broker:18}  "
            f"{ext_qty}{qty_flag}  {int_qty}  "
            f"{ext_amt}{amt_flag}  {int_amt}  "
            f"{notes}"
        )

    print()

    # ── Reverse: internal deals without confo ─────────────────────────────────
    if unmatched:
        print("── INTERNAL DEALS WITH NO CONFO ──────────────────────────────────────────────────────────────────────")
        print(
            f"{'---':3}  {'ISIN':12}  {'Side':4}  {'T-Date':10}  {'V-Date':10}  "
            f"{'Counterparty':18}  "
            f"{'Qty':14}  {'Amount':14}"
        )
        print("-" * 110)
        for td in unmatched:
            isin = (td.get("symbol") or "")[:12]
            side = (td.get("direction") or "")[:4]
            trade_date = str(td.get("trade_date") or "")[:10]
            v_date = str(td.get("settle_date_cash") or td.get("value_date_cash") or "")[:10]
            cpty = (td.get("counterparty") or "")[:18]
            qty = fmt(td.get("qty"))
            amt = fmt(td.get("transaction_value"))
            print(
                f"---  {isin:12}  {side:4}  {trade_date:10}  {v_date:10}  "
                f"{cpty:18}  {qty}  {amt}"
            )
        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("── SUMMARY ───────────────────────────────────────────────────────────────────────────────────────────────")
    print(f"  Confo rows compared:      {result.get('comparison_rows', 0)}")
    print(f"  OK  Matched (exact):      {result.get('matched_count', 0)}")
    print(f"  OK~ Matched (netting):    {result.get('matched_aggregated_count', 0)}")
    print(f"  !   Partial (mismatch):   {result.get('partial_count', 0)}")
    print(f"  ~   Similar (wrong date): {result.get('similar_found_count', 0)}")
    print(f"  ??? Not found in BO:      {result.get('not_found_count', 0)}")
    print(f"  --- Internal, no confo:   {len(unmatched)}")
    print("=" * 110)
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Trade date to reconcile (YYYY-MM-DD). Default: T0+T1 window.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="No date filter — compare all PARSED settlement trades.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run reconciliation but do NOT save results to DB.",
    )
    args = parser.parse_args()

    conn = get_conn()

    # Determine date range
    if args.all:
        date_from = date_to = None
        log.info("=== run_reconciliation.py  mode=ALL  dry_run=%s ===", args.dry_run)
    elif args.date:
        d = date_type.fromisoformat(args.date)
        date_from = date_to = d
        log.info("=== run_reconciliation.py  date=%s  dry_run=%s ===", d, args.dry_run)
    else:
        date_to, date_from = get_t0_t1_dates()   # t0_date=to, t1_date=from
        log.info(
            "=== run_reconciliation.py  T0=%s  T1=%s  dry_run=%s ===",
            date_to, date_from, args.dry_run,
        )

    run_id = None if args.dry_run else start_agent_run(conn, "run_reconciliation_manual")

    try:
        result = run_settlement_reconciliation(
            conn,
            run_id=run_id,
            date_from=date_from,
            date_to=date_to,
        )

        print_report(result, date_from, date_to)

        if run_id is not None:
            finish_agent_run(
                conn,
                run_id,
                "SUCCESS",
                (
                    f"matched={result.get('matched_count',0)} "
                    f"agg={result.get('matched_aggregated_count',0)} "
                    f"partial={result.get('partial_count',0)} "
                    f"not_found={result.get('not_found_count',0)} "
                    f"no_confo={len(result.get('unmatched_internal',[]))}"
                ),
            )

    except Exception as ex:
        log.exception("Ошибка при реконсиляции: %s", ex)
        if run_id is not None:
            finish_agent_run(conn, run_id, "FAILED", str(ex))
        conn.close()
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
