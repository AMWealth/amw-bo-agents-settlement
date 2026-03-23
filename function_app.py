import logging
import os
import io
import re
import json
import base64
import hashlib
import zipfile
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
import requests
import psycopg2
import psycopg2.extras
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
import pdfplumber

app = func.FunctionApp()

# =============================================================================
# ENV
# =============================================================================
TENANT_ID = os.environ.get("TENANT_ID", "").strip()
CLIENT_ID = os.environ.get("CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "").strip()

GRAPH_MAILBOX = os.environ.get("GRAPH_MAILBOX", "back.office@amwealth.ae").strip()
LOOKBACK_HOURS = int(os.environ.get("SETTLEMENT_LOOKBACK_HOURS", "72"))
PG_CONN_STRING = os.environ.get("PG_CONN_STRING", "").strip()

# Template codes that parse the email body (no attachment needed).
# All other templates require a PDF/Excel attachment.
EMAIL_BODY_TEMPLATES = frozenset({
    "STONEX_REPO_EMAIL",
    "GRANT_WESTOVER_REPO_EMAIL",
    "FAB_REPO_EMAIL",
})

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

REPORT_TO = os.environ.get("SETTLEMENT_REPORT_TO", "k.malkova@amwealth.ae").strip()

# Optional fallback only
ALLOWED_SENDERS_FALLBACK = {
    "intl.email.confirms@instinet.com",
    "operations@gtnme.com",
    "donovan.landry@capitalunionbank.com",
    "charles.carroll@capitalunionbank.com",
    "emiliano.cabrera@capitalunionbank.com",
    "s.voll@camcapmarkets.com",
    "bo.tdsm@zarattinibank.ch",
    "backoffice@ashendenfinance.ch",
    "donotreplysecuritiesconfirmations@stonex.com",
    "bplmailer@bpl-bondpartners.ch",
    "emccarthy@seaportglobal.com",
    "settlement@bridport.ch",
    "opsseclendingrepo@stonex.com",
    "statements@stonex.com",
    # New senders added 2026-03-18
    "grant.westover@stonex.com",
    "amna.anwar@bankfab.com",
    "umar.malik@bankfab.com",
    "vijuraj.thandalath@bankfab.com",
}

TEST_SENDERS_DEFAULT = [
    "operations@gtnme.com",
    "donovan.landry@capitalunionbank.com",
    "charles.carroll@capitalunionbank.com",
    "emiliano.cabrera@capitalunionbank.com",
    "s.voll@camcapmarkets.com",
    "intl.email.confirms@instinet.com",
    "bo.tdsm@zarattinibank.ch",
    "backoffice@ashendenfinance.ch",
    "donotreplysecuritiesconfirmations@stonex.com",
    "bplmailer@bpl-bondpartners.ch",
    "statements@stonex.com",
    "emccarthy@seaportglobal.com",
    "settlement@bridport.ch",
    "opsseclendingrepo@stonex.com",
    "grant.westover@stonex.com",
    "amna.anwar@bankfab.com",
    "umar.malik@bankfab.com",
    "vijuraj.thandalath@bankfab.com",
]

# =============================================================================
# APP / DB
# =============================================================================
def get_conn():
    logging.warning("DEBUG DB MODE: using PG_CONN_STRING")
    if not PG_CONN_STRING:
        raise RuntimeError("Missing required env var: PG_CONN_STRING")

    conn_str = PG_CONN_STRING
    if "sslmode=" not in conn_str.lower():
        conn_str += " sslmode=require"

    return psycopg2.connect(conn_str, connect_timeout=30)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def start_agent_run(conn, agent_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into back_office_auto.agent_runs (agent_name, started_at, status, note)
            values (%s, now(), %s, %s)
            returning id
            """,
            (agent_name, "RUNNING", f"{agent_name} started"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_agent_run(conn, run_id: int, status: str, note: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            update back_office_auto.agent_runs
               set finished_at = now(),
                   status = %s,
                   note = %s
             where id = %s
            """,
            (status, note, run_id),
        )
    conn.commit()


def email_already_processed(conn, internet_message_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select 1
            from back_office_auto.settlement_emails
            where internet_message_id = %s
            limit 1
            """,
            (internet_message_id,),
        )
        return cur.fetchone() is not None


def clear_reconciliation_run_rows(conn, run_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "delete from back_office_auto.settlement_reconciliation where run_id = %s",
            (run_id,),
        )
    conn.commit()


def load_mapping(conn) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select *
            from back_office_auto.counterparty_email_mapping
            where is_active = true
            """
        )
        for row in cur.fetchall():
            sender = (row["email_address_of_counterparty"] or "").strip().lower()
            if sender:
                out[sender] = dict(row)
    return out


def get_allowed_senders(mapping_by_sender: Dict[str, Dict[str, Any]]) -> set:
    allowed = set()

    for sender, row in mapping_by_sender.items():
        sender_key = (sender or "").strip().lower()
        if not sender_key:
            continue

        if row.get("is_active") is True:
            allowed.add(sender_key)

    if not allowed:
        allowed = set(ALLOWED_SENDERS_FALLBACK)

    return allowed


# =============================================================================
# GRAPH
# =============================================================================
def get_graph_token() -> str:
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": GRAPH_SCOPE,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def graph_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def normalize_email_address(sender_obj: Dict[str, Any]) -> str:
    try:
        return (sender_obj.get("emailAddress", {}).get("address") or "").strip().lower()
    except Exception:
        return ""


def list_recent_messages(token: str, mailbox: str, since_dt: datetime) -> List[Dict[str, Any]]:
    since_iso = since_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    filter_str = f"receivedDateTime ge {since_iso}"

    url = (
        f"{GRAPH_BASE}/users/{mailbox}/mailFolders/Inbox/messages"
        f"?$top=100"
        f"&$select=id,internetMessageId,subject,receivedDateTime,from,hasAttachments,bodyPreview"
        f"&$orderby=receivedDateTime desc"
        f"&$filter={filter_str}"
    )

    results: List[Dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=graph_headers(token), timeout=120)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return results


def list_recent_messages_by_sender_python_filter(
    token: str,
    mailbox: str,
    sender_email: str,
    since_dt: datetime,
    top_n: int = 3,
    require_attachments: bool = False,
) -> List[Dict[str, Any]]:
    sender_email = (sender_email or "").strip().lower()
    all_msgs = list_recent_messages(token, mailbox, since_dt)
    filtered = [
        m for m in all_msgs
        if normalize_email_address(m.get("from", {})) == sender_email
        and (not require_attachments or m.get("hasAttachments") is True)
    ]
    filtered.sort(key=lambda x: x.get("receivedDateTime") or "", reverse=True)
    return filtered[:top_n]


def get_message_attachments(token: str, mailbox: str, message_id: str) -> List[Dict[str, Any]]:
    url = f"{GRAPH_BASE}/users/{mailbox}/messages/{message_id}/attachments?$top=100"
    r = requests.get(url, headers=graph_headers(token), timeout=120)
    r.raise_for_status()
    data = r.json()
    return data.get("value", [])


def get_message_full(token: str, mailbox: str, message_id: str) -> Dict[str, Any]:
    url = (
        f"{GRAPH_BASE}/users/{mailbox}/messages/{message_id}"
        f"?$select=id,internetMessageId,subject,receivedDateTime,from,hasAttachments,body,bodyPreview"
    )
    r = requests.get(url, headers=graph_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def get_attachment_content_bytes(
    token: str,
    mailbox: str,
    message_id: str,
    attachment_id: str,
) -> bytes:
    url = f"{GRAPH_BASE}/users/{mailbox}/messages/{message_id}/attachments/{attachment_id}"
    r = requests.get(url, headers=graph_headers(token), timeout=120)
    r.raise_for_status()
    data = r.json()

    content_b64 = data.get("contentBytes")
    if not content_b64:
        raise RuntimeError(f"Attachment {attachment_id} has no contentBytes")

    return base64.b64decode(content_b64)


# =============================================================================
# COMMON HELPERS
# =============================================================================
def clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def clean_col(s: Any) -> str:
    s = clean_text(str(s or ""))
    s = re.sub(r"[^a-zA-Z0-9%]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def infer_file_type(filename: str) -> str:
    f = filename.lower()
    if f.endswith(".xlsx"):
        return "xlsx"
    if f.endswith(".xlsm"):
        return "xlsm"
    if f.endswith(".xls"):
        return "xls"
    if f.endswith(".pdf"):
        return "pdf"
    if f.endswith(".zip"):
        return "zip"
    if f.endswith(".csv"):
        return "csv"
    if f.endswith(".msg"):
        return "msg"
    return "other"


def strip_html_tags(html: str) -> str:
    if not html:
        return ""
    html = re.sub(r"(?is)<(script|style).*?>.*?(</\1>)", " ", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</p>", "\n", html)
    html = re.sub(r"<[^>]+>", " ", html)
    html = html.replace("&nbsp;", " ")
    html = html.replace("&amp;", "&")
    html = html.replace("&lt;", "<")
    html = html.replace("&gt;", ">")
    return clean_text(html)


def parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except Exception:
            return None

    s = clean_text(str(value))
    if not s:
        return None

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    s = s.replace(",", "")
    s = s.replace("%", "")
    s = s.replace("'", "")          # Zarattini/Bridport: "200'000" → "200000"
    s = re.sub(r"^[A-Z]{3}\s+", "", s)
    # CUB OCR artifact: "1 15,628" (space inside number after currency strip)
    s = re.sub(r"^(\d)\s+(\d)", r"\1\2", s)
    s = re.sub(r"[^0-9.\-]", "", s).strip()

    if s in ("", "-", ".", "-."):
        return None

    try:
        num = Decimal(s)
        return -num if negative and num > 0 else num
    except InvalidOperation:
        return None


def parse_date_any(value: Any, prefer_day_first: bool = True) -> Optional[date]:
    if value is None:
        return None

    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except Exception:
            pass

    s = str(value).strip()
    if not s:
        return None

    s = re.sub(r"\s+", " ", s)

    # Strip trailing non-date garbage: "03-Mar-26 Last Execution Time" → "03-Mar-26"
    # but keep month names: "18 February 2025" stays intact
    s_trimmed = re.match(r"^(\d{1,4}[-/. ]\w{1,9}[-/. ]\d{2,4})", s)
    if s_trimmed:
        s = s_trimmed.group(1).strip()

    patterns_day_first = [
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%d-%b-%Y",
        "%d-%B-%Y",
        "%d-%b-%y",     # CUB: "20-Feb-26"
        "%d/%m/%y",     # short year slash
        "%d.%m.%y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
    ]

    patterns_month_first = [
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%m/%d/%y",
        "%b %d %Y",
        "%B %d %Y",
    ]

    patterns = patterns_day_first + patterns_month_first if prefer_day_first else patterns_month_first + patterns_day_first

    for p in patterns:
        try:
            return datetime.strptime(s, p).date()
        except Exception:
            continue

    for dayfirst in ([True, False] if prefer_day_first else [False, True]):
        try:
            dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
            if pd.notna(dt):
                return dt.date()
        except Exception:
            continue

    return None


def parse_datetime_any(value: Any, prefer_day_first: bool = True) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    s = clean_text(str(value))
    if not s:
        return None

    patterns_day_first = [
        "%d/%m/%Y %I:%M:%S %p",
        "%d-%m-%Y %I:%M:%S %p",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d %b %Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p",
    ]

    patterns_month_first = [
        "%m/%d/%Y %I:%M:%S %p",
        "%m-%d-%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m-%d-%Y %H:%M:%S",
        "%b %d %Y %I:%M:%S %p",
    ]

    patterns = patterns_day_first + patterns_month_first if prefer_day_first else patterns_month_first + patterns_day_first

    for p in patterns:
        try:
            return datetime.strptime(s, p)
        except Exception:
            continue

    for dayfirst in ([True, False] if prefer_day_first else [False, True]):
        try:
            dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
            if pd.notna(dt):
                return dt.to_pydatetime()
        except Exception:
            continue

    return None


def normalize_side(value: Optional[str], template_code: Optional[str] = None) -> Optional[str]:
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    v = re.sub(r"\s+", " ", raw).strip().upper()

    if v in {"B", "BUY", "BOT", "BOUGHT"}:
        return "BUY"

    if v in {"S", "SELL", "SLD", "SOLD"}:
        return "SELL"

    if "WE SOLD TO YOU" in v:
        return "BUY"

    if "WE BOUGHT FROM YOU" in v:
        return "SELL"

    # Ashenden: "YOU SOLD IN USD" / "YOU BOUGHT IN USD"
    if "YOU SOLD" in v:
        return "SELL"

    if "YOU BOUGHT" in v:
        return "BUY"

    # StoneX FI: "We confirm your BUY/SELL transaction"
    if "CONFIRM YOUR BUY" in v:
        return "BUY"

    if "CONFIRM YOUR SELL" in v:
        return "SELL"

    # FAB REPO: "AM Wealth enters Reverse Repo (lends cash/ borrows securities)"
    # Reverse Repo = AM Wealth lends cash, borrows securities → BUY side (receives securities)
    if "REVERSE REPO" in v or "LENDS CASH" in v:
        return "BUY"

    if v == "BUYS":
        return "BUY"

    if v == "SELLS":
        return "SELL"

    if "BUY" in v and "SELL" not in v:
        return "BUY"

    if "SELL" in v and "BUY" not in v:
        return "SELL"

    if "LENDS COLLATERAL" in v:
        return "SELL"

    if "BORROWS COLLATERAL" in v:
        return "BUY"

    return None


def validate_side(side: Optional[str]) -> List[str]:
    if side not in {"BUY", "SELL"}:
        return [f"invalid_side:{side}"]
    return []


def validate_trade_dates(trade_date: Optional[date], value_date: Optional[date]) -> List[str]:
    issues = []
    today = now_utc().date()

    if trade_date:
        if trade_date < today - timedelta(days=30):
            issues.append(f"trade_date_too_old:{trade_date}")
        if trade_date > today + timedelta(days=2):
            issues.append(f"trade_date_in_future:{trade_date}")

    if trade_date and value_date:
        if value_date < trade_date - timedelta(days=2):
            issues.append(f"value_date_before_trade_date:{value_date}")
        if value_date > trade_date + timedelta(days=30):
            issues.append(f"value_date_too_far:{value_date}")

    return issues


def validate_trade_date_vs_email(trade_date: Optional[date], email_received_at: Optional[datetime]) -> List[str]:
    if not trade_date or not email_received_at:
        return []

    email_date = email_received_at.date()
    delta = abs((trade_date - email_date).days)

    if delta > 14:
        return [f"trade_date_far_from_email_date:{trade_date}_vs_{email_date}"]
    return []


def pick_first(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, "", "nan"):
            return row[k]
    return None


def rx(pattern: str, text: str, flags: int = re.IGNORECASE | re.MULTILINE) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def finalize_trade_validation(trade: Dict[str, Any], email_received_at: Optional[datetime]):
    issues = []

    if not trade.get("isin"):
        issues.append("missing_isin")

    issues.extend(validate_side(trade.get("side")))
    issues.extend(validate_trade_dates(trade.get("trade_date"), trade.get("value_date")))
    issues.extend(validate_trade_date_vs_email(trade.get("trade_date"), email_received_at))

    if not trade.get("trade_date"):
        issues.append("missing_trade_date")

    if not trade.get("counterparty_reference"):
        issues.append("missing_counterparty_reference")

    if issues:
        trade["validation_status"] = "NEEDS_REVIEW"
        trade["validation_note"] = ", ".join(issues)
    else:
        trade["validation_status"] = "PARSED"
        trade["validation_note"] = None


def resolve_broker_name_from_mapping(sender: str, mapping_by_sender: Dict[str, Dict[str, Any]]) -> str:
    sender_key = (sender or "").strip().lower()
    row = mapping_by_sender.get(sender_key)

    if not row:
        return sender_key

    alias_name = clean_text(row.get("counterparty_alias"))
    counterparty_name = clean_text(row.get("counterparty"))

    if alias_name:
        return alias_name
    if counterparty_name:
        return counterparty_name
    return sender_key


def detect_template_from_mapping(sender: str, mapping_by_sender: Dict[str, Dict[str, Any]]) -> Optional[str]:
    row = mapping_by_sender.get((sender or "").strip().lower())
    if not row:
        return None
    return clean_text(row.get("template_code")) or None


def trade_dedup_key(trade: Dict[str, Any]) -> str:
    return "|".join([
        clean_text(str(trade.get("internet_message_id") or "")),
        clean_text(str(trade.get("isin") or "")),
        clean_text(str(trade.get("side") or "")),
        clean_text(str(trade.get("trade_date") or "")),
        clean_text(str(trade.get("value_date") or "")),
        clean_text(str(trade.get("quantity") or trade.get("nominal") or "")),
        clean_text(str(trade.get("price") or trade.get("price_in_percentage") or "")),
    ])


def build_generic_reference(
    isin: Optional[str],
    side: Optional[str],
    trade_date: Optional[date],
    value_date: Optional[date],
    qty: Optional[Decimal],
    price: Optional[Decimal],
    nominal: Optional[Decimal] = None,
) -> str:
    return "|".join([
        clean_text(str(isin or "")),
        clean_text(str(side or "")),
        clean_text(str(trade_date or "")),
        clean_text(str(value_date or "")),
        clean_text(str(qty if qty is not None else nominal if nominal is not None else "")),
        clean_text(str(price if price is not None else "")),
    ])


def normalize_trade_signs(trade: Dict[str, Any]) -> Dict[str, Any]:
    for field in [
        "quantity",
        "price",
        "consideration",
        "commission",
        "net_amount",
        "nominal",
        "price_in_percentage",
        "accrued_interest",
    ]:
        val = trade.get(field)
        dec = parse_decimal(val)
        if dec is not None:
            trade[field] = abs(dec)
    return trade


# =============================================================================
# INSERT HELPERS
# =============================================================================
def insert_settlement_email(
    conn,
    internet_message_id: str,
    message_id: str,
    sender: str,
    subject: str,
    received_at: Optional[datetime],
    status: str,
    note: str,
    mailbox: str,
    attachment_count: int,
    parsed_trade_count: int,
    processing_run_id: Optional[int],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into back_office_auto.settlement_emails
            (
                internet_message_id,
                message_id,
                sender,
                subject,
                received_at,
                processed_at,
                status,
                note,
                mailbox,
                attachment_count,
                parsed_trade_count,
                processing_run_id
            )
            values (%s, %s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s)
            on conflict (internet_message_id) do update
               set processed_at = now(),
                   status = excluded.status,
                   note = excluded.note,
                   mailbox = excluded.mailbox,
                   attachment_count = excluded.attachment_count,
                   parsed_trade_count = excluded.parsed_trade_count,
                   processing_run_id = excluded.processing_run_id
            returning id
            """,
            (
                internet_message_id,
                message_id,
                sender,
                subject,
                received_at,
                status,
                note,
                mailbox,
                attachment_count,
                parsed_trade_count,
                processing_run_id,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return row[0]


def insert_settlement_file(
    conn,
    internet_message_id: str,
    file_name: str,
    file_type: str,
    file_hash: str,
    attachment_size: int,
    attachment_order: Optional[int],
    parent_zip_file_name: Optional[str],
    parse_status: str,
    parse_note: Optional[str],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into back_office_auto.settlement_files
            (
                internet_message_id,
                file_name,
                file_type,
                file_hash,
                created_at,
                attachment_size,
                attachment_order,
                parent_zip_file_name,
                parse_status,
                parse_note
            )
            values (%s, %s, %s, %s, now(), %s, %s, %s, %s, %s)
            on conflict do nothing
            returning id
            """,
            (
                internet_message_id,
                file_name,
                file_type,
                file_hash,
                attachment_size,
                attachment_order,
                parent_zip_file_name,
                parse_status,
                parse_note,
            ),
        )
        row = cur.fetchone()

    conn.commit()

    if row:
        return row[0]

    with conn.cursor() as cur:
        cur.execute(
            """
            select id
            from back_office_auto.settlement_files
            where internet_message_id = %s
              and file_hash = %s
            order by id desc
            limit 1
            """,
            (internet_message_id, file_hash),
        )
        existing = cur.fetchone()
    conn.commit()
    return existing[0]


def upsert_settlement_trade(conn, trade: Dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into back_office_auto.settlement_trades
            (
                internet_message_id,
                source_file,
                source_type,
                broker_name,
                account_number,
                security_name,
                isin,
                sedol,
                side,
                trade_date,
                value_date,
                quantity,
                price,
                price_currency,
                consideration,
                commission,
                stamp_duty,
                transaction_levy,
                trading_fee,
                afrc_fee,
                net_amount,
                settlement_terms,
                counterparty_reference,
                validation_status,
                validation_note,
                created_at,
                nominal,
                price_in_percentage,
                accrued_interest,
                settlement_currency,
                parser_template,
                raw_json,
                matched_by,
                processing_run_id,
                file_id,
                email_id,
                side_original_text,
                trade_date_original_text,
                value_date_original_text
            )
            values
            (
                %(internet_message_id)s,
                %(source_file)s,
                %(source_type)s,
                %(broker_name)s,
                %(account_number)s,
                %(security_name)s,
                %(isin)s,
                %(sedol)s,
                %(side)s,
                %(trade_date)s,
                %(value_date)s,
                %(quantity)s,
                %(price)s,
                %(price_currency)s,
                %(consideration)s,
                %(commission)s,
                %(stamp_duty)s,
                %(transaction_levy)s,
                %(trading_fee)s,
                %(afrc_fee)s,
                %(net_amount)s,
                %(settlement_terms)s,
                %(counterparty_reference)s,
                %(validation_status)s,
                %(validation_note)s,
                now(),
                %(nominal)s,
                %(price_in_percentage)s,
                %(accrued_interest)s,
                %(settlement_currency)s,
                %(parser_template)s,
                %(raw_json)s,
                %(matched_by)s,
                %(processing_run_id)s,
                %(file_id)s,
                %(email_id)s,
                %(side_original_text)s,
                %(trade_date_original_text)s,
                %(value_date_original_text)s
            )
            on conflict (internet_message_id, counterparty_reference)
            do update set
                source_file = excluded.source_file,
                source_type = excluded.source_type,
                broker_name = excluded.broker_name,
                account_number = excluded.account_number,
                security_name = excluded.security_name,
                isin = excluded.isin,
                sedol = excluded.sedol,
                side = excluded.side,
                trade_date = excluded.trade_date,
                value_date = excluded.value_date,
                quantity = excluded.quantity,
                price = excluded.price,
                price_currency = excluded.price_currency,
                consideration = excluded.consideration,
                commission = excluded.commission,
                stamp_duty = excluded.stamp_duty,
                transaction_levy = excluded.transaction_levy,
                trading_fee = excluded.trading_fee,
                afrc_fee = excluded.afrc_fee,
                net_amount = excluded.net_amount,
                settlement_terms = excluded.settlement_terms,
                validation_status = excluded.validation_status,
                validation_note = excluded.validation_note,
                nominal = excluded.nominal,
                price_in_percentage = excluded.price_in_percentage,
                accrued_interest = excluded.accrued_interest,
                settlement_currency = excluded.settlement_currency,
                parser_template = excluded.parser_template,
                raw_json = excluded.raw_json,
                matched_by = excluded.matched_by,
                processing_run_id = excluded.processing_run_id,
                file_id = excluded.file_id,
                email_id = excluded.email_id,
                side_original_text = excluded.side_original_text,
                trade_date_original_text = excluded.trade_date_original_text,
                value_date_original_text = excluded.value_date_original_text
            returning id
            """,
            trade,
        )
        row = cur.fetchone()
    conn.commit()
    return row[0]


# =============================================================================
# EXCEL HELPERS / PARSERS
# =============================================================================
def extract_excel_sheets(file_bytes: bytes, filename: str) -> List[pd.DataFrame]:
    dataframes: List[pd.DataFrame] = []
    lower_name = filename.lower()

    if lower_name.endswith(".xlsx") or lower_name.endswith(".xlsm"):
        try:
            xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
            for sheet_name in xls.sheet_names:
                try:
                    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine="openpyxl")
                    if df is not None and not df.empty:
                        dataframes.append(df)
                except Exception as e:
                    logging.warning("Failed reading sheet %s in %s: %s", sheet_name, filename, e)
            return dataframes
        except Exception as e:
            logging.exception("Unable to read xlsx/xlsm file %s: %s", filename, e)
            return dataframes

    if lower_name.endswith(".xls"):
        try:
            xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="xlrd")
            for sheet_name in xls.sheet_names:
                try:
                    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, engine="xlrd")
                    if df is not None and not df.empty:
                        dataframes.append(df)
                except Exception as e:
                    logging.warning("Failed reading xls sheet %s in %s: %s", sheet_name, filename, e)
            return dataframes
        except Exception as e:
            logging.exception("Unable to read xls file %s: %s", filename, e)
            return dataframes

    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
        for ws in wb.worksheets:
            rows = list(ws.values)
            if not rows:
                continue
            header = [clean_text(x) for x in rows[0]]
            body = rows[1:]
            df = pd.DataFrame(body, columns=header)
            if not df.empty:
                dataframes.append(df)
    except Exception as e:
        logging.exception("Fallback excel reader failed for %s: %s", filename, e)

    return dataframes


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_col(c) for c in df.columns]
    return df


def find_gtn_header_row(df: pd.DataFrame) -> Optional[int]:
    for idx in range(len(df)):
        row_values = [clean_text(str(x)).lower() for x in df.iloc[idx].tolist() if x is not None]
        row_text = " | ".join(row_values)

        if (
            "symbol" in row_text
            and "side" in row_text
            and "quantity" in row_text
            and ("stl.date" in row_text or "stl_date" in row_text or "stl date" in row_text)
            and ("isin code" in row_text or "isin_code" in row_text or "isin" in row_text)
        ):
            return idx
    return None


def rebuild_gtn_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    raw = df.copy().reset_index(drop=True)

    header_row_idx = find_gtn_header_row(raw)
    if header_row_idx is None:
        logging.warning("GTN header row not found")
        return pd.DataFrame()

    header = [clean_col(x) for x in raw.iloc[header_row_idx].tolist()]
    data = raw.iloc[header_row_idx + 1 :].copy()
    data.columns = header
    data = data.reset_index(drop=True)

    data = data.loc[:, [c for c in data.columns if c and not str(c).startswith("unnamed")]]
    data = data.dropna(how="all")
    return data


def extract_isin_from_text(text: str) -> Optional[str]:
    m = re.search(r"\b[A-Z]{2}[A-Z0-9]{9,12}\b", text)
    return m.group(0) if m else None


def extract_date_from_text(text: str) -> Optional[str]:
    m = re.search(r"\b\d{2}/\d{2}/\d{4}\b", text)
    return m.group(0) if m else None


def extract_datetime_from_text(text: str) -> Optional[str]:
    m = re.search(r"\b\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}(?:\s*[ap]m)?\b", text, re.IGNORECASE)
    return m.group(0) if m else None


def parse_gtn_dataframe_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    table = rebuild_gtn_dataframe(df)
    if table.empty:
        return []

    rows: List[Dict[str, Any]] = []

    for _, row in table.iterrows():
        rowd = {str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

        row_text = " ".join([clean_text(str(v)) for v in rowd.values() if v is not None]).strip()
        row_text_upper = row_text.upper()

        if not row_text:
            continue

        if (
            "SUB TOTAL" in row_text_upper
            or "GRAND TOTAL" in row_text_upper
            or "SETTLEMENT INSTRUCTIONS" in row_text_upper
            or "THIS TRADE CONFIRMATION IS PROVIDED TO YOU" in row_text_upper
            or "SYMBOL SIDE QUANTITY PRICE" in row_text_upper
        ):
            continue

        symbol = pick_first(rowd, ["symbol"])
        side = pick_first(rowd, ["side"])
        quantity = pick_first(rowd, ["quantity"])
        price = pick_first(rowd, ["price"])
        gross = pick_first(rowd, ["gross"])
        brok_com = pick_first(rowd, ["brok_com", "brok_com.", "broker_commission", "commission"])
        net_settle = pick_first(rowd, ["net_settle", "net_amount"])
        tr_date = pick_first(rowd, ["tr_date", "trade_date"])
        stl_date = pick_first(rowd, ["stl_date", "settlement_date", "value_date"])
        isin_code = pick_first(rowd, ["isin_code", "isin"])

        if not symbol:
            continue

        if not side:
            if " BUY " in f" {row_text_upper} ":
                side = "Buy"
            elif " SELL " in f" {row_text_upper} ":
                side = "Sell"

        if not isin_code:
            isin_code = extract_isin_from_text(row_text)

        if not tr_date:
            tr_date = extract_datetime_from_text(row_text)

        if not stl_date:
            stl_date = extract_date_from_text(row_text)

        if not side or not isin_code or not tr_date:
            continue

        rows.append({
            "symbol": symbol,
            "side_raw": side,
            "quantity": quantity,
            "price": price,
            "gross": gross,
            "commission": brok_com,
            "net_amount": net_settle,
            "trade_datetime_raw": tr_date,
            "settlement_date_raw": stl_date,
            "isin": isin_code,
            "raw_row": rowd,
        })

    logging.info("GTN rebuilt dataframe parsed rows=%s", len(rows))
    return rows


def parse_gtn_excel(
    df: pd.DataFrame,
    internet_message_id: str,
    source_file: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    parsed_rows = parse_gtn_dataframe_rows(df)

    for row in parsed_rows:
        trade_dt = parse_datetime_any(row["trade_datetime_raw"], prefer_day_first=False)
        trade_date = trade_dt.date() if trade_dt else parse_date_any(row["trade_datetime_raw"], prefer_day_first=False)
        value_date = parse_date_any(row["settlement_date_raw"], prefer_day_first=False)

        trade = {
            "internet_message_id": internet_message_id,
            "source_file": source_file,
            "source_type": "excel",
            "broker_name": resolve_broker_name_from_mapping(sender, mapping_by_sender),
            "account_number": None,
            "security_name": clean_text(row["symbol"]),
            "isin": clean_text(row["isin"]),
            "sedol": None,
            "side": normalize_side(row["side_raw"], "GTN_XLS_PDF"),
            "trade_date": trade_date,
            "value_date": value_date,
            "quantity": parse_decimal(row["quantity"]),
            "price": parse_decimal(row["price"]),
            "price_currency": "USD",
            "consideration": parse_decimal(row["gross"]),
            "commission": parse_decimal(row["commission"]),
            "stamp_duty": None,
            "transaction_levy": None,
            "trading_fee": None,
            "afrc_fee": None,
            "net_amount": parse_decimal(row["net_amount"]),
            "settlement_terms": None,
            "counterparty_reference": None,
            "nominal": None,
            "price_in_percentage": None,
            "accrued_interest": None,
            "settlement_currency": "USD",
            "parser_template": "GTN_XLS_PDF",
            "raw_json": json.dumps(row["raw_row"], default=str),
            "matched_by": None,
            "processing_run_id": processing_run_id,
            "file_id": file_id,
            "email_id": email_id,
            "side_original_text": clean_text(str(row["side_raw"])) if row["side_raw"] is not None else None,
            "trade_date_original_text": clean_text(str(row["trade_datetime_raw"])) if row["trade_datetime_raw"] is not None else None,
            "value_date_original_text": clean_text(str(row["settlement_date_raw"])) if row["settlement_date_raw"] is not None else None,
            "validation_status": None,
            "validation_note": None,
        }
        trade = normalize_trade_signs(trade)
        trade["counterparty_reference"] = build_generic_reference(
            trade["isin"], trade["side"], trade["trade_date"], trade["value_date"], trade["quantity"], trade["price"]
        )
        finalize_trade_validation(trade, email_received_at)
        out.append(trade)

    return out


def parse_instinet_excel(
    df: pd.DataFrame,
    internet_message_id: str,
    source_file: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    df = normalize_dataframe(df)
    df = df.dropna(how="all")
    if df.empty:
        return out

    for _, row in df.iterrows():
        rowd = {str(k): (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

        isin = pick_first(rowd, ["isin", "isin_code", "security_nb", "security_number"])
        security_name = pick_first(rowd, ["security_name", "sec_descr", "description", "symbol_name", "security", "symbol"])
        side_raw = pick_first(rowd, ["side", "buy_sell", "b_s"])
        trade_date_raw = pick_first(rowd, ["trade_date", "tr_date", "tradedate"])
        value_date_raw = pick_first(rowd, ["settl_date", "settlement_date", "value_date", "stl_date"])
        quantity = pick_first(rowd, ["quantity", "qty", "units"])
        price = pick_first(rowd, ["price", "trade_price"])
        price_currency = pick_first(rowd, ["currency", "tr_currency", "settl_currency", "price_currency"])
        consideration = pick_first(rowd, ["consideration", "gross", "principal_amount", "amount"])
        commission = pick_first(rowd, ["commission", "brok_com", "brokerage", "broker_commission"])
        net_amount = pick_first(rowd, ["net", "net_settle", "net_amount"])
        counterparty_reference = pick_first(rowd, ["trans", "trans_no", "transaction_no", "counterparty_reference", "reference", "ref"])

        if not counterparty_reference:
            counterparty_reference = "|".join([
                clean_text(str(isin or "")),
                clean_text(str(normalize_side(side_raw, "INSTINET_XLSM") or "")),
                clean_text(str(parse_date_any(trade_date_raw, prefer_day_first=True) or "")),
                clean_text(str(parse_decimal(quantity) or parse_decimal(consideration) or "")),
            ])

        if not isin and not security_name:
            continue

        trade = {
            "internet_message_id": internet_message_id,
            "source_file": source_file,
            "source_type": "excel",
            "broker_name": resolve_broker_name_from_mapping(sender, mapping_by_sender),
            "account_number": None,
            "security_name": clean_text(security_name),
            "isin": clean_text(isin),
            "sedol": None,
            "side": normalize_side(side_raw, "INSTINET_XLSM"),
            "trade_date": parse_date_any(trade_date_raw, prefer_day_first=True),
            "value_date": parse_date_any(value_date_raw, prefer_day_first=True),
            "quantity": parse_decimal(quantity),
            "price": parse_decimal(price),
            "price_currency": clean_text(price_currency),
            "consideration": parse_decimal(consideration),
            "commission": parse_decimal(commission),
            "stamp_duty": None,
            "transaction_levy": None,
            "trading_fee": None,
            "afrc_fee": None,
            "net_amount": parse_decimal(net_amount),
            "settlement_terms": None,
            "counterparty_reference": clean_text(counterparty_reference),
            "nominal": None,
            "price_in_percentage": None,
            "accrued_interest": None,
            "settlement_currency": clean_text(price_currency),
            "parser_template": "INSTINET_XLSM",
            "raw_json": json.dumps(rowd, default=str),
            "matched_by": None,
            "processing_run_id": processing_run_id,
            "file_id": file_id,
            "email_id": email_id,
            "side_original_text": clean_text(str(side_raw)) if side_raw is not None else None,
            "trade_date_original_text": clean_text(str(trade_date_raw)) if trade_date_raw is not None else None,
            "value_date_original_text": clean_text(str(value_date_raw)) if value_date_raw is not None else None,
            "validation_status": None,
            "validation_note": None,
        }

        if not trade["trade_date"] and trade.get("isin") and trade.get("quantity") and trade.get("price"):
            continue

        trade = normalize_trade_signs(trade)
        finalize_trade_validation(trade, email_received_at)
        out.append(trade)

    return out


# =============================================================================
# PDF TEXT HELPERS
# =============================================================================
def extract_pdf_text(file_bytes: bytes) -> str:
    texts: List[str] = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            try:
                texts.append(page.extract_text() or "")
            except Exception:
                continue
    return "\n".join(texts)


def build_trade_dict(
    *,
    internet_message_id: str,
    source_file: str,
    source_type: str,
    broker_name: str,
    security_name: Optional[str],
    isin: Optional[str],
    side: Optional[str],
    trade_date: Optional[date],
    value_date: Optional[date],
    quantity: Optional[Decimal],
    price: Optional[Decimal],
    price_currency: Optional[str],
    consideration: Optional[Decimal],
    commission: Optional[Decimal],
    net_amount: Optional[Decimal],
    settlement_terms: Optional[str],
    counterparty_reference: Optional[str],
    nominal: Optional[Decimal],
    price_in_percentage: Optional[Decimal],
    accrued_interest: Optional[Decimal],
    settlement_currency: Optional[str],
    parser_template: str,
    raw_json: str,
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    side_original_text: Optional[str],
    trade_date_original_text: Optional[str],
    value_date_original_text: Optional[str],
) -> Dict[str, Any]:
    return {
        "internet_message_id": internet_message_id,
        "source_file": source_file,
        "source_type": source_type,
        "broker_name": broker_name,
        "account_number": None,
        "security_name": clean_text(security_name),
        "isin": clean_text(isin),
        "sedol": None,
        "side": side,
        "trade_date": trade_date,
        "value_date": value_date,
        "quantity": quantity,
        "price": price,
        "price_currency": clean_text(price_currency),
        "consideration": consideration,
        "commission": commission,
        "stamp_duty": None,
        "transaction_levy": None,
        "trading_fee": None,
        "afrc_fee": None,
        "net_amount": net_amount,
        "settlement_terms": settlement_terms,
        "counterparty_reference": clean_text(counterparty_reference),
        "validation_status": None,
        "validation_note": None,
        "nominal": nominal,
        "price_in_percentage": price_in_percentage,
        "accrued_interest": accrued_interest,
        "settlement_currency": clean_text(settlement_currency),
        "parser_template": parser_template,
        "raw_json": raw_json,
        "matched_by": None,
        "processing_run_id": processing_run_id,
        "file_id": file_id,
        "email_id": email_id,
        "side_original_text": clean_text(side_original_text),
        "trade_date_original_text": clean_text(trade_date_original_text),
        "value_date_original_text": clean_text(value_date_original_text),
    }


def parse_bond_style_pdf_common(
    *,
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
    parser_template: str,
) -> List[Dict[str, Any]]:
    isin = (
        rx(r"ISIN(?:\s+Code)?\s*[:\-]?\s*([A-Z0-9]{10,20})", text)
        or rx(r"Security\s+Nb\.?\s*[:\-]?\s*([A-Z0-9]{10,20})", text)
    )
    security_name = (
        rx(r"Description\s*[:\-]?\s*(.+)", text)
        or rx(r"Security\s+Name\s*[:\-]?\s*(.+)", text)
    )

    direction_phrase = (
        rx(r"(We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you)", text)
        or rx(r"\b(SELLS|BUYS|BUY|SELL)\b", text)
    )

    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*([0-9A-Za-z\-/\. ]+)", text)
    value_date_raw = rx(r"(?:Settlement|Value)\s+Date\s*[:\-]?\s*([0-9A-Za-z\-/\. ]+)", text)

    nominal_raw = rx(r"Nominal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    qty_raw = rx(r"Quantity\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    price_pct_raw = rx(r"Price\s*[:\-]?\s*([0-9,.\-]+)\s*%", text)
    price_raw = rx(r"Price\s*[:\-]?\s*([0-9,.\-]+)", text)
    principal_amount = rx(r"Principal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    accrued = rx(r"Accrued\s+Interest\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    total_cash = (
        rx(r"Total\s+Cash\s+Settlement\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
        or rx(r"Net\s+amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    )
    ccy = (
        rx(r"Currency\s*[:\-]?\s*([A-Z]{3})", text)
        or rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)
    )
    ref = rx(r"(?:Reference|Confirmation\s+No\.?|Trade\s+Reference)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)

    side = normalize_side(direction_phrase, parser_template)

    quantity = parse_decimal(qty_raw)
    nominal = parse_decimal(nominal_raw)

    consideration = parse_decimal(principal_amount) or parse_decimal(total_cash)
    price_pct = parse_decimal(price_pct_raw)
    price = parse_decimal(price_raw)

    if not ref:
        ref = build_generic_reference(
            isin=isin,
            side=side,
            trade_date=parse_date_any(trade_date_raw, True),
            value_date=parse_date_any(value_date_raw, True),
            qty=quantity,
            price=price if price is not None else price_pct,
            nominal=nominal,
        )

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=parse_date_any(trade_date_raw, prefer_day_first=True),
        value_date=parse_date_any(value_date_raw, prefer_day_first=True),
        quantity=quantity,
        price=price,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=parse_decimal(total_cash),
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=parse_decimal(accrued),
        settlement_currency=ccy or "USD",
        parser_template=parser_template,
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "qty_raw": qty_raw,
            "nominal_raw": nominal_raw,
            "price_raw": price_raw,
            "price_pct_raw": price_pct_raw,
            "principal_amount": principal_amount,
            "accrued": accrued,
            "total_cash": total_cash,
            "currency": ccy,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )

    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []

    return [trade]


# =============================================================================
# EXPLICIT PDF PARSERS PER BROKER
# =============================================================================
def parse_cub_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Fix CUB OCR artifact: "USD 1 17,994.43" → "USD 117,994.43"
    # OCR sometimes splits a leading digit from the rest of the number
    text = re.sub(r"(USD|EUR|CHF|GBP|AED)\s+(\d)\s+(\d{2,3},\d{3})", r"\1 \2\3", text)

    isin = (
        rx(r"Security\s+Nb\s*:\s*([A-Z0-9]{10,20})", text)
        or rx(r"ISIN(?:\s+Code)?\s*[:\-]?\s*([A-Z0-9]{10,20})", text)
        or rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", text)
    )

    direction_phrase = (
        rx(r"(We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you(?:\s+on)?)", text)
        or rx(r"\b(SELLS|BUYS|BUY|SELL)\b", text)
    )

    trade_date_raw = rx(r"Trade\s+Date\s*:\s*([0-9A-Za-z\-/\. ]+)", text)
    value_date_raw = rx(r"Value\s+Date\s*:\s*([0-9A-Za-z\-/\. ]+)", text)

    security_name = None
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    for i, line in enumerate(lines):
        if "WE SOLD TO YOU" in line.upper() or "WE BOUGHT FROM YOU" in line.upper():
            for j in range(i + 1, min(i + 8, len(lines))):
                candidate = lines[j]
                if (
                    "TRADE DATE" in candidate.upper()
                    or "VALUE DATE" in candidate.upper()
                    or "MATURITY" in candidate.upper()
                    or "ISSUE DATE" in candidate.upper()
                    or "NOTIONAL" in candidate.upper()
                    or "PRICE" in candidate.upper()
                    or "PRINCIPAL AMOUNT" in candidate.upper()
                ):
                    continue
                if re.search(r"[A-Z]{2}[A-Z0-9]{9,12}", candidate):
                    continue
                if len(candidate) > 3:
                    security_name = candidate
                    break
            break

    nominal_raw = (
        rx(r"Notional\s+[A-Z]{3}\s+([0-9,.\-]+)", text)
        or rx(r"Notional\s*:\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
        or rx(r"Nominal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    )

    price_pct_raw = rx(r"Price\s*[:\-]?\s*([0-9,.\-]+)\s*%", text)
    price_raw = rx(r"Price\s*[:\-]?\s*([0-9,.\-]+)", text)

    principal_amount = rx(r"Principal\s+Amount\s+(?:[A-Z]{3}\s+)?([0-9,.\-]+)", text)
    accrued = rx(r"Accrued\s+Interest\s+(?:[A-Z]{3}\s+)?([0-9,.\-]+)", text)
    total_cash = rx(r"Net\s+amount\s+(?:[A-Z]{3}\s+)?([0-9,.\-]+)", text)

    ccy = (
        rx(r"Notional\s+([A-Z]{3})\s+[0-9,.\-]+", text)
        or rx(r"Principal\s+Amount\s+([A-Z]{3})\s+[0-9,.\-]+", text)
        or rx(r"Net\s+amount\s+([A-Z]{3})\s+[0-9,.\-]+", text)
        or rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)
    )

    ref = (
        rx(r"Our\s+ref\s*:\s*([A-Za-z0-9\-/]+)", text)
        or rx(r"(?:Reference|Confirmation\s+No\.?|Trade\s+Reference)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    )

    side = normalize_side(direction_phrase, "CUB_PDF")
    nominal = parse_decimal(nominal_raw)
    price_pct = parse_decimal(price_pct_raw)
    price = parse_decimal(price_raw)
    consideration = parse_decimal(principal_amount) or parse_decimal(total_cash)
    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)

    if not ref:
        ref = build_generic_reference(
            isin=isin,
            side=side,
            trade_date=trade_date,
            value_date=value_date,
            qty=None,
            price=price if price is not None else price_pct,
            nominal=nominal,
        )

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=None,
        price=price,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=parse_decimal(total_cash),
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=parse_decimal(accrued),
        settlement_currency=ccy or "USD",
        parser_template="CUB_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "nominal_raw": nominal_raw,
            "price_raw": price_raw,
            "price_pct_raw": price_pct_raw,
            "principal_amount": principal_amount,
            "accrued": accrued,
            "total_cash": total_cash,
            "currency": ccy,
            "security_name": security_name,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )

    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []

    return [trade]


def parse_camcap_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Format: "CAMcap Markets Ltd SELLS/BUYS"
    # "ISIN Code XS3096274314" (no colon, no %)
    # "Price 98.5000" — percentage price without % sign
    # "Total Cash Settlement Amount 197,000.00 USD"

    direction_phrase = (
        rx(r"CAMcap\s+Markets\s+Ltd\s+(SELLS|BUYS|SELL|BUY)", text)
        or rx(r"\b(SELLS|BUYS|BUY|SELL)\b", text)
    )
    side = normalize_side(direction_phrase, "CAMCAP_PDF")

    isin = (
        rx(r"ISIN\s+Code\s+([A-Z]{2}[A-Z0-9]{9,12})", text)
        or rx(r"ISIN\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{9,12})", text)
        or rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", text)
    )
    security_name = rx(r"ISIN\s+Code\s+\S+\s*\n(.+)", text)

    trade_date_raw = rx(r"Trade\s+Date\s+(\d{2}/\d{2}/\d{4})", text)
    value_date_raw = rx(r"Settlement\s+Date\s+(\d{2}/\d{2}/\d{4})", text)

    nominal_raw = rx(r"Nominal\s+Amount\s+([0-9,\.]+)", text)
    ccy = rx(r"Currency\s+([A-Z]{3})", text) or rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)

    # CamCap uses bond price in % without "%" sign — treat as price_in_percentage
    price_pct_raw = (
        rx(r"Price\s+([0-9\.]+)\s*%", text)
        or rx(r"Price\s+([0-9\.]+)", text)   # no % sign → still percentage for bonds
    )
    principal_raw = rx(r"Principal\s+Amount\s+([0-9,\.]+)", text)
    accrued_raw = rx(r"Accrued\s+Interest\s+([0-9,\.]+)", text)
    total_cash_raw = (
        rx(r"Total\s+Cash\s+Settlement\s+Amount\s+([0-9,\.]+)", text)
        or rx(r"Net\s+amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,\.]+)", text)
    )
    ref = (
        rx(r"Our\s+Ref\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
        or rx(r"(?:Reference|Confirmation\s+No\.?)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    )

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)
    nominal = parse_decimal(nominal_raw)
    price_pct = parse_decimal(price_pct_raw)
    consideration = parse_decimal(principal_raw) or parse_decimal(total_cash_raw)
    net_amount = parse_decimal(total_cash_raw)
    accrued = parse_decimal(accrued_raw)

    if not ref:
        ref = build_generic_reference(isin, side, trade_date, value_date, None, price_pct, nominal)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=None,
        price=None,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=net_amount,
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=accrued,
        settlement_currency=ccy or "USD",
        parser_template="CAMCAP_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "nominal_raw": nominal_raw,
            "price_pct_raw": price_pct_raw,
            "principal_raw": principal_raw,
            "total_cash_raw": total_cash_raw,
            "accrued_raw": accrued_raw,
            "ccy": ccy,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []
    return [trade]


def parse_zarattini_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    isin = (
        rx(r"Security\s+Nb\.?\s*[:\-]?\s*([A-Z0-9]{10,20})", text)
        or rx(r"ISIN(?:\s+Code)?\s*[:\-]?\s*([A-Z0-9]{10,20})", text)
        or rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", text)
    )

    # Zarattini format: company name follows "(Fixed)" on the line after "Foreign Notes"
    # e.g.: "USD 179'000 Foreign Notes\n(Fixed) General Motors Co\n2014-1.4.35 Sr\nISIN:..."
    security_name = (
        rx(r"Description\s*[:\-]?\s*(.+)", text)
        or rx(r"Security\s+Name\s*[:\-]?\s*(.+)", text)
        or rx(r"Foreign\s+Notes\s*\r?\n\s*\([A-Za-z]+\)\s+(.+)", text)
    )

    direction_phrase = (
        rx(r"(YOUR\s+SECURITY\s+SALE)", text)
        or rx(r"(YOUR\s+SECURITY\s+PURCHASE)", text)
        or rx(r"(We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you)", text)
        or rx(r"\b(SELLS|BUYS|BUY|SELL)\b", text)
    )

    side = None
    if direction_phrase:
        dp = direction_phrase.upper()
        if "YOUR SECURITY SALE" in dp:
            side = "SELL"
        elif "YOUR SECURITY PURCHASE" in dp:
            side = "BUY"
        else:
            side = normalize_side(direction_phrase, "ZARATTINI_PDF")

    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*([0-9A-Za-z\-/\. ]+)", text)
    value_date_raw = rx(r"(?:Settlement|Value)\s+Date\s*[:\-]?\s*([0-9A-Za-z\-/\. ]+)", text)

    # Zarattini nominal format: "USD 200'000 Foreign Notes" (after "Stock Exchange:" line)
    nominal_raw = (
        rx(r"Nominal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
        or rx(r"USD\s+([0-9,']+)\s+Foreign\s+Notes", text)
        or rx(r"EUR\s+([0-9,']+)\s+Foreign\s+Notes", text)
        or rx(r"CHF\s+([0-9,']+)\s+Foreign\s+Notes", text)
    )
    qty_raw = rx(r"Quantity\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,.\-]+)", text)
    price_pct_raw = rx(r"Price\s*[:\-]?\s*([0-9,'.\-]+)\s*%", text)
    price_raw = rx(r"Price\s*[:\-]?\s*([0-9,'.\-]+)", text)
    principal_amount = rx(r"Principal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
    accrued = (
        rx(r"Accrued\s+Interests?\s*[:\-]?\s*\d+\s+days?\s+(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
        or rx(r"Accrued\s+Interest\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
    )
    total_cash = (
        rx(r"To\s+your\s+(?:DEBIT|CREDIT)\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
        or rx(r"Total\s+Cash\s+Settlement\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
        or rx(r"Net\s+amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'.\-]+)", text)
    )

    ccy = (
        rx(r"Currency\s*[:\-]?\s*([A-Z]{3})", text)
        or rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)
    )

    ref = (
        rx(r"Ref\.\s*No\.?\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
        or rx(r"(?:Reference|Confirmation\s+No\.?|Trade\s+Reference)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    )

    quantity = parse_decimal(qty_raw)
    nominal = parse_decimal(nominal_raw)
    price_pct = parse_decimal(price_pct_raw)
    price = parse_decimal(price_raw)
    consideration = parse_decimal(principal_amount) or parse_decimal(total_cash)

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)

    if not ref:
        ref = build_generic_reference(
            isin=isin,
            side=side,
            trade_date=trade_date,
            value_date=value_date,
            qty=quantity,
            price=price if price is not None else price_pct,
            nominal=nominal,
        )

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=quantity,
        price=price,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=parse_decimal(total_cash),
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=parse_decimal(accrued),
        settlement_currency=ccy or "USD",
        parser_template="ZARATTINI_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "qty_raw": qty_raw,
            "nominal_raw": nominal_raw,
            "price_raw": price_raw,
            "price_pct_raw": price_pct_raw,
            "principal_amount": principal_amount,
            "accrued": accrued,
            "total_cash": total_cash,
            "currency": ccy,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )

    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []

    return [trade]


def parse_ashenden_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Format: "YOU SOLD IN USD" / "YOU BOUGHT IN USD"
    # ISIN on line after "TICKER: ISIN: SEDOL: CUSIP:" headers
    # "Trade ref: 127815.01" for reference
    # "Net Amount: 108,705.90 USD" — value after headers on next line

    direction_phrase = (
        rx(r"(YOU\s+SOLD\s+IN\s+[A-Z]{3})", text)
        or rx(r"(YOU\s+BOUGHT\s+IN\s+[A-Z]{3})", text)
    )
    side = normalize_side(direction_phrase, "ASHENDEN_PDF")

    # ISIN: on line after "TICKER: ISIN: SEDOL: CUSIP:" — second token in that line
    # Use strict 12-char pattern ending in digit to avoid false positives (e.g. "NOTIFICATION")
    isin = rx(r"ISIN[: \t]+([A-Z]{2}[A-Z0-9]{9}[0-9])", text)
    if not isin:
        # Table layout: "TICKER:  ISIN:  SEDOL:  CUSIP:\nNOKIA  US654902AC90  ..."
        m = re.search(
            r"TICKER[: \t]+ISIN[: \t]+SEDOL[: \t]+CUSIP[: \t]*\r?\n\S+[ \t]+([A-Z]{2}[A-Z0-9]{9}[0-9])",
            text, re.IGNORECASE
        )
        isin = m.group(1) if m else None
    if not isin:
        # Fallback: strict ISIN — exactly 12 chars, last char must be digit (check digit)
        isin = rx(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b", text)

    # Security name: first non-digit line directly after "Quantity   At the price of:" header.
    # Real PDF layout (all variants):
    #   Quantity At the price of:
    #   BOND NAME [possibly with coupon/maturity]
    #   400,000 97.00            ← qty/price row (starts with digit — excluded)
    _EXCLUDE = ("AM WEALTH", "ASHENDEN", "FOLLOWING TRANSACTION", "YOUR BEHALF", "YOUR ACCOUNT")
    security_name = None
    _m = re.search(
        r"Quantity\s+At\s+the\s+price\s+of\s*[:\-]?\s*\r?\n\s*([^0-9].+)",
        text, re.IGNORECASE
    )
    if _m:
        _cand = _m.group(1).strip()
        if not any(e in _cand.upper() for e in _EXCLUDE):
            security_name = _cand or None

    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*(\d{2}[-/]\w{3}[-/]\d{2,4})", text)
    value_date_raw = rx(r"Settlement\s+Date\s*[:\-]?\s*(\d{2}[-/]\w{3}[-/]\d{2,4})", text)

    # Quantity and price: on the data row that starts with the bond nominal.
    # pdfplumber merges columns so the row may look like any of:
    #   "400,000 97.00"
    #   "250,000 16-Feb-2031 71.25"           (maturity date merged in)
    #   "555,000 Coupon Share Basket 25-Feb-2031 97.00"  (name continuation merged in)
    #   "200,000 7.200000% 17-Jul-2030 105.75"  (coupon rate merged in)
    # Rule: first large integer on the line = qty; last decimal number = price.
    qty_raw = None
    price_raw = None
    _qty_price_m = re.search(
        r"^([1-9][0-9,]+)\b.*\b([0-9]+\.[0-9]+)\s*$",
        text, re.MULTILINE
    )
    if _qty_price_m:
        qty_raw = _qty_price_m.group(1)
        price_raw = _qty_price_m.group(2)

    # Safer override: if "At the price of: NNN" is explicit, use that as price
    _explicit_price = rx(r"At\s+the\s+price\s+of\s*[:\-]?\s*([0-9,.]+)", text)
    if _explicit_price:
        price_raw = _explicit_price

    # Amounts: "Gross Amount: Net Amount:\n 106,700.00 USD  108,705.90 USD"
    # Net amount is the second value on that data line
    net_amount_raw = None
    gross_amount_raw = None
    m = re.search(r"Gross\s+Amount\s*[:\-]?\s*Net\s+Amount\s*[:\-]?\s*\n\s*([0-9,\.]+)\s*[A-Z]{3}\s+([0-9,\.]+)\s*[A-Z]{3}", text, re.IGNORECASE)
    if m:
        gross_amount_raw = m.group(1)
        net_amount_raw = m.group(2)
    else:
        gross_amount_raw = rx(r"Gross\s+Amount\s*[:\-]?\s*([0-9,\.]+)", text)
        net_amount_raw = rx(r"Net\s+Amount\s*[:\-]?\s*([0-9,\.]+)", text)

    accrued = rx(r"Accrued\s+Interest\s*[/\s]\s*Days\s*\n?\s*([0-9,\.]+)", text)

    ccy = rx(r"YOU\s+(?:SOLD|BOUGHT)\s+IN\s+([A-Z]{3})", text) or rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)

    ref = (
        rx(r"Trade\s+ref\s*[:\-]?\s*([A-Za-z0-9\.\-/]+)", text)
        or rx(r"(?:Reference|Confirmation\s+No\.?)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    )

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)
    quantity = parse_decimal(qty_raw)
    price = parse_decimal(price_raw)
    consideration = parse_decimal(gross_amount_raw)
    net_amount = parse_decimal(net_amount_raw)
    accrued_dec = parse_decimal(accrued)

    # Derive nominal from consideration / price if quantity not extracted directly.
    # For bonds: nominal = gross_amount / (price/100) → must round to nearest integer.
    nominal = quantity
    if nominal is None and price and consideration and price > 0:
        try:
            derived = (consideration * Decimal("100") / price).quantize(Decimal("1"))
            nominal = derived
            if quantity is None:
                quantity = derived
        except Exception:
            pass

    if not ref:
        ref = build_generic_reference(isin, side, trade_date, value_date, quantity, price)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=quantity,
        price=price,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=net_amount,
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price,
        accrued_interest=accrued_dec,
        settlement_currency=ccy or "USD",
        parser_template="ASHENDEN_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "qty_raw": qty_raw,
            "price_raw": price_raw,
            "gross_amount_raw": gross_amount_raw,
            "net_amount_raw": net_amount_raw,
            "accrued": accrued,
            "currency": ccy,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []
    return [trade]


def parse_stonex_fixed_income_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Format: "We confirm your BUY/SELL transaction with us"
    # Fields: Our Reference, Trade Date, Settlement Date, ISIN, Quantity,
    #         Gross Price (USD), Accrued Interest (USD), Net Settlement Amount (USD)

    direction_phrase = rx(r"We\s+confirm\s+your\s+(BUY|SELL)\s+transaction", text)
    side = normalize_side(direction_phrase, "STONEX_PDF")

    isin = rx(r"ISIN\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{9,12})", text)
    security_name = rx(r"Security\s+Description\s*[:\-]?\s*(.+)", text)
    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*([0-9/\-\.]+)", text)
    value_date_raw = rx(r"Settlement\s+Date\s*[:\-]?\s*([0-9/\-\.]+)", text)
    qty_raw = rx(r"Quantity\s*[:\-]?\s*([0-9,]+)", text)
    # Gross Price: "104.910000USD" or "99.000000 GBP" — price in % of par
    price_pct_raw = rx(r"Gross\s+Price\s*[:\-]?\s*([0-9,.]+?)\s*[A-Z]{3}", text)
    gross_amount_raw = rx(r"Gross\s+Amount\s*[:\-]?\s*([0-9,\.]+?)\s*[A-Z]{3}", text)
    accrued_raw = rx(r"Accrued\s+Interest\s*[:\-]?\s*([0-9,\.]+?)\s*[A-Z]{3}", text)
    net_amount_raw = rx(r"Net\s+Settlement\s+Amount\s*[:\-]?\s*([0-9,\.]+?)\s*[A-Z]{3}", text)
    ref = rx(r"Our\s+Reference\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    # Currency: from Gross Price field (handles GBP, EUR, etc.) or FX Rate line, fallback USD
    ccy = (
        rx(r"Gross\s+Price\s*[:\-]?\s*[0-9,.]+\s*([A-Z]{3})", text)
        or rx(r"FX\s+Rate\s+\d+\s+([A-Z]{3})/[A-Z]{3}", text)
        or "USD"
    )

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)
    quantity = parse_decimal(qty_raw)
    price_pct = parse_decimal(price_pct_raw)
    consideration = parse_decimal(gross_amount_raw)
    net_amount = parse_decimal(net_amount_raw)
    accrued = parse_decimal(accrued_raw)

    if not ref:
        ref = build_generic_reference(isin, side, trade_date, value_date, quantity, price_pct)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=quantity,
        price=None,
        price_currency=ccy,
        consideration=consideration,
        commission=None,
        net_amount=net_amount,
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=quantity,
        price_in_percentage=price_pct,
        accrued_interest=accrued,
        settlement_currency=ccy,
        parser_template="STONEX_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "qty_raw": qty_raw,
            "price_pct_raw": price_pct_raw,
            "gross_amount_raw": gross_amount_raw,
            "accrued_raw": accrued_raw,
            "net_amount_raw": net_amount_raw,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []
    return [trade]


def parse_bondpartners_pdf(text, internet_message_id, source_file, email_received_at, processing_run_id, file_id, email_id, broker_name):
    return parse_bond_style_pdf_common(
        text=text,
        internet_message_id=internet_message_id,
        source_file=source_file,
        email_received_at=email_received_at,
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        broker_name=broker_name,
        parser_template="BONDPARTNERS_PDF",
    )


def parse_seaport_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Format: single data row after 3-line header block
    # Columns: Side  ISIN  Quantity  Price  SettlDate  NetConsideration  Ccy  Accrued  GrossConsideration  TradeDate  InstrumentName
    # Example: "Buy XS0701227075 200000 116.75 17/02/2026 -237548.61 USD 4048.6100 -233500 12/02/2026 AM WEALTH ..."
    pattern = re.compile(
        r"^(Buy|Sell)\s+"
        r"([A-Z]{2}[A-Z0-9]{9,12})\s+"
        r"(-?[\d,]+)\s+"           # quantity
        r"(-?[\d.]+)\s+"           # price
        r"(\d{2}/\d{2}/\d{4})\s+"  # settlement date
        r"(-?[\d,.]+)\s+"          # net consideration
        r"([A-Z]{3})\s+"           # currency
        r"(-?[\d,.]+)\s+"          # accrued interest
        r"(-?[\d,.]+)\s+"          # gross consideration
        r"(\d{2}/\d{2}/\d{4})",    # trade date
        re.IGNORECASE | re.MULTILINE,
    )

    rows: List[Dict[str, Any]] = []
    for m in pattern.finditer(text):
        side_raw, isin, qty_raw, price_raw, settle_raw, net_raw, ccy, accrued_raw, gross_raw, trade_raw = m.groups()

        # Instrument name: rest of the line after trade date
        rest = text[m.end():].split("\n")[0].strip()
        security_name = re.sub(r"^AM\s+WEALTH\s*", "", rest).strip() or None

        trade_date = parse_date_any(trade_raw, prefer_day_first=True)
        value_date = parse_date_any(settle_raw, prefer_day_first=True)
        quantity = parse_decimal(qty_raw)
        price = parse_decimal(price_raw)
        net_amount = parse_decimal(net_raw)
        consideration = parse_decimal(gross_raw)
        accrued = parse_decimal(accrued_raw)
        side = normalize_side(side_raw, "SEAPORT_PDF")

        ref = build_generic_reference(isin, side, trade_date, value_date, quantity, price)

        trade = build_trade_dict(
            internet_message_id=internet_message_id,
            source_file=source_file,
            source_type="pdf",
            broker_name=broker_name,
            security_name=security_name,
            isin=isin,
            side=side,
            trade_date=trade_date,
            value_date=value_date,
            quantity=quantity,
            price=price,
            price_currency=ccy,
            consideration=consideration,
            commission=None,
            net_amount=net_amount,
            settlement_terms="DVP",
            counterparty_reference=ref,
            nominal=quantity,
            price_in_percentage=price,   # bond price is in % of par
            accrued_interest=accrued,
            settlement_currency=ccy,
            parser_template="SEAPORT_PDF",
            raw_json=json.dumps({
                "side_raw": side_raw, "isin": isin,
                "qty_raw": qty_raw, "price_raw": price_raw,
                "settle_raw": settle_raw, "trade_raw": trade_raw,
                "net_raw": net_raw, "gross_raw": gross_raw,
                "accrued_raw": accrued_raw, "ccy": ccy,
            }, default=str),
            processing_run_id=processing_run_id,
            file_id=file_id,
            email_id=email_id,
            side_original_text=side_raw,
            trade_date_original_text=trade_raw,
            value_date_original_text=settle_raw,
        )
        trade = normalize_trade_signs(trade)
        finalize_trade_validation(trade, email_received_at)
        rows.append(trade)

    logging.info("SEAPORT PDF parsed rows=%s", len(rows))
    return rows


def parse_bridport_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    # Format: "We BOUGHT/SOLD from/to you on DATE for settlement on DATE"
    # ISIN labeled as "Security Nb : USU71878AA76"
    # "Nominal USD 100'000.00" (no "Amount" keyword)
    # "Price 101.42500 %" — with space before %
    # Apostrophe thousands separator throughout

    isin = (
        rx(r"Security\s+Nb\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{9,12})", text)
        or rx(r"ISIN\s*[:\-]?\s*([A-Z]{2}[A-Z0-9]{9,12})", text)
        or rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", text)
    )

    security_name = rx(r"Security\s+Nb\s*[:\-]?\s*\S+\s*\n(.+)", text)

    # Direction + trade date embedded: "We BOUGHT from you on 18 February 2025"
    direction_phrase = (
        rx(r"(We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you\s+on)", text)
        or rx(r"(We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you)", text)
    )
    side = normalize_side(direction_phrase, "BRIDPORT_PDF")

    # Trade date from direction phrase
    trade_date_raw = rx(
        r"We\s+(?:SOLD|BOUGHT)\s+(?:to|from)\s+you\s+on\s+(\d{1,2}\s+\w+\s+\d{4}|\d{2}[-/]\w{3}[-/]\d{2,4}|\d{2}/\d{2}/\d{4})",
        text,
    )
    # Settlement date from "for settlement on DATE"
    value_date_raw = rx(
        r"for\s+settlement\s+on\s+(\d{1,2}\s+\w+\s+\d{4}|\d{2}[-/]\w{3}[-/]\d{2,4}|\d{2}/\d{2}/\d{4})",
        text,
    )
    if not value_date_raw:
        value_date_raw = rx(r"(?:Settlement|Value)\s+Date\s*[:\-]?\s*([0-9A-Za-z\-/\. ]+)", text)

    # "Nominal USD 100'000.00"
    nominal_raw = (
        rx(r"Nominal\s+[A-Z]{3}\s+([0-9,'\.]+)", text)
        or rx(r"Nominal\s+Amount\s*[:\-]?\s*(?:[A-Z]{3}\s*)?([0-9,'\.]+)", text)
    )
    price_pct_raw = rx(r"Price\s+([0-9,'\.]+)\s*%", text)
    price_raw = rx(r"Price\s*[:\-]?\s*([0-9,'\.]+)", text)
    principal_raw = rx(r"Principal\s+Amount\s+(?:[A-Z]{3}\s+)?([0-9,'\.]+)", text)
    accrued_raw = rx(r"Accrued\s+Interest\s*(?:[A-Z]{3}\s+)?([0-9,'\.]+)", text)
    net_raw = rx(r"Net\s+amount\s+(?:[A-Z]{3}\s+)?([0-9,'\.]+)", text)
    ccy = rx(r"\b(USD|EUR|AED|GBP|CHF|HKD)\b", text)
    ref = (
        rx(r"Our\s+ref\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
        or rx(r"(?:Reference|Confirmation\s+No\.?)\s*[:\-]?\s*([A-Za-z0-9\-/]+)", text)
    )

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(value_date_raw, prefer_day_first=True)
    nominal = parse_decimal(nominal_raw)
    price_pct = parse_decimal(price_pct_raw)
    price = parse_decimal(price_raw) if not price_pct_raw else None
    consideration = parse_decimal(principal_raw) or parse_decimal(net_raw)
    net_amount = parse_decimal(net_raw)
    accrued = parse_decimal(accrued_raw)

    if not ref:
        ref = build_generic_reference(isin, side, trade_date, value_date, None, price_pct or price, nominal)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file=source_file,
        source_type="pdf",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=None,
        price=price,
        price_currency=ccy or "USD",
        consideration=consideration,
        commission=None,
        net_amount=net_amount,
        settlement_terms="DVP",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=accrued,
        settlement_currency=ccy or "USD",
        parser_template="BRIDPORT_PDF",
        raw_json=json.dumps({
            "direction_phrase": direction_phrase,
            "trade_date_raw": trade_date_raw,
            "value_date_raw": value_date_raw,
            "nominal_raw": nominal_raw,
            "price_pct_raw": price_pct_raw,
            "principal_raw": principal_raw,
            "accrued_raw": accrued_raw,
            "net_raw": net_raw,
            "ccy": ccy,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=file_id,
        email_id=email_id,
        side_original_text=direction_phrase,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=value_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    if not trade.get("isin") and not trade.get("security_name"):
        return []
    return [trade]


def parse_gtn_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    text = re.sub(r"([A-Z0-9]{8,12})\s*\n\s*([A-Z0-9]{1,4})", r"\1\2", text)

    pattern = re.compile(
        r"""
        (?P<symbol>[A-Z0-9._-]+)\s+
        (?P<side>Buy|Sell)\s+
        (?P<quantity>[\d,]+\.\d+)\s+
        (?P<price>\(?[\d,]+\.\d+\)?)\s+
        (?P<gross>\(?[\d,]+\.\d+\)?)\s+
        (?P<brok_com>[\d,]+\.\d+)\s+
        (?P<mkt_com>[\d,]+\.\d+)\s+
        (?P<net_settle>\(?[\d,]+\.\d+\)?)\s+
        (?P<stl_date>\d{2}/\d{2}/\d{4})\s+
        (?P<isin>[A-Z0-9]{10,14}).*?
        (?P<tr_date>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\s+[ap]m)
        """,
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )

    for m in pattern.finditer(text):
        row = m.groupdict()

        trade_dt = parse_datetime_any(row["tr_date"], prefer_day_first=False)
        trade_date = trade_dt.date() if trade_dt else parse_date_any(row["tr_date"], prefer_day_first=False)
        value_date = parse_date_any(row["stl_date"], prefer_day_first=False)

        trade = build_trade_dict(
            internet_message_id=internet_message_id,
            source_file=source_file,
            source_type="pdf",
            broker_name=broker_name,
            security_name=row["symbol"],
            isin=row["isin"],
            side=normalize_side(row["side"], "GTN_XLS_PDF"),
            trade_date=trade_date,
            value_date=value_date,
            quantity=parse_decimal(row["quantity"]),
            price=parse_decimal(row["price"]),
            price_currency="USD",
            consideration=parse_decimal(row["gross"]),
            commission=parse_decimal(row["brok_com"]),
            net_amount=parse_decimal(row["net_settle"]),
            settlement_terms=None,
            counterparty_reference=None,
            nominal=None,
            price_in_percentage=None,
            accrued_interest=None,
            settlement_currency="USD",
            parser_template="GTN_XLS_PDF",
            raw_json=json.dumps(row, default=str),
            processing_run_id=processing_run_id,
            file_id=file_id,
            email_id=email_id,
            side_original_text=row["side"],
            trade_date_original_text=row["tr_date"],
            value_date_original_text=row["stl_date"],
        )

        trade = normalize_trade_signs(trade)
        trade["counterparty_reference"] = build_generic_reference(
            trade["isin"], trade["side"], trade["trade_date"], trade["value_date"], trade["quantity"], trade["price"]
        )
        finalize_trade_validation(trade, email_received_at)
        rows.append(trade)

    logging.info("GTN PDF parsed rows=%s", len(rows))
    return rows


def parse_stonex_daily_statement_pdf(
    text: str,
    internet_message_id: str,
    source_file: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    broker_name: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    compact = re.sub(r"[ \t]+", " ", text)
    pattern = re.compile(
        r"""
        (?P<symbol>\b[A-Z]{3}\b)\s+
        (?P<qty>[\d,]+\.\d+)\s+
        (?P<price>[\d,]+\.\d+)\s+
        (?P<traded_amount>[\d,]+\.\d+)\s+
        (?P<trade_date>\d{2}/\d{2}/\d{4})\s+
        (?P<settle_date>\d{2}/\d{2}/\d{4})
        """,
        re.VERBOSE,
    )

    for m in pattern.finditer(compact):
        g = m.groupdict()

        side = "BUY"
        qty = parse_decimal(g["qty"])
        price = parse_decimal(g["price"])
        amount = parse_decimal(g["traded_amount"])
        trade_date = parse_date_any(g["trade_date"], prefer_day_first=True)
        value_date = parse_date_any(g["settle_date"], prefer_day_first=True)

        trade = build_trade_dict(
            internet_message_id=internet_message_id,
            source_file=source_file,
            source_type="pdf",
            broker_name=broker_name,
            security_name=g["symbol"],
            isin=g["symbol"],
            side=side,
            trade_date=trade_date,
            value_date=value_date,
            quantity=qty,
            price=price,
            price_currency="EUR",
            consideration=amount,
            commission=None,
            net_amount=amount,
            settlement_terms="FOP",
            counterparty_reference=build_generic_reference(g["symbol"], side, trade_date, value_date, qty, price),
            nominal=qty,
            price_in_percentage=None,
            accrued_interest=None,
            settlement_currency="USD",
            parser_template="STONEX_DAILY_STATEMENT_PDF",
            raw_json=json.dumps(g, default=str),
            processing_run_id=processing_run_id,
            file_id=file_id,
            email_id=email_id,
            side_original_text=side,
            trade_date_original_text=g["trade_date"],
            value_date_original_text=g["settle_date"],
        )
        trade = normalize_trade_signs(trade)
        finalize_trade_validation(trade, email_received_at)
        rows.append(trade)

    logging.info("STONEX daily statement PDF parsed rows=%s", len(rows))
    return rows


# =============================================================================
# EMAIL BODY PARSER FOR REPO
# =============================================================================
def parse_stonex_repo_email_body(
    body_text: str,
    internet_message_id: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    text = clean_text(body_text)
    if not text:
        return []

    broker_name = resolve_broker_name_from_mapping(sender, mapping_by_sender)

    position_id = rx(r"Position\s*ID\s*([A-Za-z0-9\-]+)", text) or rx(r"Position\s*ID\s*[:\-]?\s*([A-Za-z0-9\-]+)", text)
    trade_type = rx(r"Trade\s*Type\s*\(StoneX\)\s*([A-Za-z ]+)", text)
    qty_raw = rx(r"Quantity\s*([0-9,.\-]+)", text)
    price_raw = rx(r"Price\s+in\s+Trade\s+Currency\s+\(including\s+HC\)\s*([0-9,.\-]+)", text)
    traded_amount_raw = rx(r"Traded\s*Amount\s*([0-9,.\-]+)", text)
    isin = rx(r"ISIN\s*([A-Z0-9]{10,20})", text)
    security_name = rx(r"Security\s*Name\s*(.+?)\s+Trade\s*Currency")
    trade_currency = rx(r"Trade\s*Currency\s*([A-Z]{3})", text)
    haircut_raw = rx(r"Hair\s*Cut\s*([0-9,.\-]+%)", text)
    trade_date_raw = rx(r"Trade\s*Date\s*([0-9/\-\.]+)", text)
    settle_date_raw = rx(r"Settle\s*Date\s*([0-9/\-\.]+)", text)
    end_date_raw = rx(r"End\s*Date\s*([A-Za-z0-9/\-\.]+)", text)
    benchmark = rx(r"Benchmark\s*(.+?)\s+Rate")
    rate_raw = rx(r"Rate\s*([0-9,.\-]+)", text)
    spread_raw = rx(r"Spread\s*([0-9,.\-]+)", text)
    direction_raw = rx(r"Direction\s*(.+?)\s*(?:Comments|$)")
    comments = rx(r"Comments\s*(.+)$", text)

    side = normalize_side(direction_raw, "STONEX_REPO_EMAIL")
    qty = parse_decimal(qty_raw)
    price = parse_decimal(price_raw)
    traded_amount = parse_decimal(traded_amount_raw)
    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(settle_date_raw, prefer_day_first=True)

    raw_json = {
        "position_id": position_id,
        "trade_type": trade_type,
        "qty_raw": qty_raw,
        "price_raw": price_raw,
        "traded_amount_raw": traded_amount_raw,
        "isin": isin,
        "security_name": security_name,
        "trade_currency": trade_currency,
        "haircut_raw": haircut_raw,
        "trade_date_raw": trade_date_raw,
        "settle_date_raw": settle_date_raw,
        "end_date_raw": end_date_raw,
        "benchmark": benchmark,
        "rate_raw": rate_raw,
        "spread_raw": spread_raw,
        "direction_raw": direction_raw,
        "comments": comments,
    }

    ref = position_id or build_generic_reference(
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        qty=qty,
        price=price,
        nominal=qty,
    )

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file="EMAIL_BODY",
        source_type="email_body",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=qty,
        price=price,
        price_currency=trade_currency or "USD",
        consideration=traded_amount,
        commission=None,
        net_amount=traded_amount,
        settlement_terms="REPO",
        counterparty_reference=ref,
        nominal=qty,
        price_in_percentage=parse_decimal(haircut_raw),
        accrued_interest=None,
        settlement_currency=trade_currency or "USD",
        parser_template="STONEX_REPO_EMAIL",
        raw_json=json.dumps(raw_json, default=str),
        processing_run_id=processing_run_id,
        file_id=None,
        email_id=email_id,
        side_original_text=direction_raw,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=settle_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)

    return [trade]


def parse_grant_westover_email_body(
    body_text: str,
    internet_message_id: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Grant Westover (grant.westover@stonex.com) sends REPO/settlement emails with fields:
    Security Name, Trading Broker, Amount, ISIN, Transaction Type, Units, CCY,
    Clearing Broker, Trade Date, Settle Date, Loc
    """
    text = clean_text(body_text)
    if not text:
        return []

    broker_name = resolve_broker_name_from_mapping(sender, mapping_by_sender)

    isin = rx(r"ISIN\s*[:\-]?\s*\n?\s*([A-Z]{2}[A-Z0-9]{9,12})", body_text)
    if not isin:
        isin = rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", body_text)

    security_name = rx(r"Security\s+Name\s*[:\-]?\s*\n?\s*(.+)", body_text)
    transaction_type = rx(r"Transaction\s+Type\s*[:\-]?\s*\n?\s*(Buy|Sell|BUY|SELL)", body_text)
    qty_raw = rx(r"Units\s*[:\-]?\s*\n?\s*([0-9,\.]+)", body_text)
    amount_raw = rx(r"Amount\s*[:\-]?\s*\n?\s*\(?([0-9,\.]+)\)?", body_text)
    ccy = rx(r"CCY\s*[:\-]?\s*\n?\s*([A-Z]{3})", body_text) or "USD"
    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*\n?\s*([0-9/\-\.]+)", body_text)
    settle_date_raw = rx(r"Settle\s+Date\s*[:\-]?\s*\n?\s*([0-9/\-\.]+)", body_text)

    side = normalize_side(transaction_type, "GRANT_WESTOVER_REPO_EMAIL")
    trade_date = parse_date_any(trade_date_raw, prefer_day_first=False)
    value_date = parse_date_any(settle_date_raw, prefer_day_first=False)
    quantity = parse_decimal(qty_raw)
    amount = parse_decimal(amount_raw)

    ref = build_generic_reference(isin, side, trade_date, value_date, quantity, None, quantity)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file="EMAIL_BODY",
        source_type="email_body",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=quantity,
        price=None,
        price_currency=ccy,
        consideration=amount,
        commission=None,
        net_amount=amount,
        settlement_terms="REPO",
        counterparty_reference=ref,
        nominal=quantity,
        price_in_percentage=None,
        accrued_interest=None,
        settlement_currency=ccy,
        parser_template="GRANT_WESTOVER_REPO_EMAIL",
        raw_json=json.dumps({
            "transaction_type": transaction_type, "isin": isin,
            "security_name": security_name, "qty_raw": qty_raw,
            "amount_raw": amount_raw, "ccy": ccy,
            "trade_date_raw": trade_date_raw, "settle_date_raw": settle_date_raw,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=None,
        email_id=email_id,
        side_original_text=transaction_type,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=settle_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)
    return [trade]


def parse_fab_repo_email_body(
    body_text: str,
    internet_message_id: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    FAB Global Market senders (amna.anwar@bankfab.com, umar.malik@bankfab.com,
    vijuraj.thandalath@bankfab.com).
    Format: "AM Wealth enters Reverse Repo (lends cash/ borrows securities)"
    Fields: ISIN, Description, All in Price, Collateral, Face Amount,
            Start Cash / Cash, Interest, Trade Date, Settlement Date
    Reverse Repo = AM Wealth lends cash, receives (borrows) securities → BUY side.
    """
    text = body_text
    if not text:
        return []

    broker_name = resolve_broker_name_from_mapping(sender, mapping_by_sender)

    # Direction from subject line
    direction_raw = rx(r"(AM\s+Wealth\s+enters\s+Reverse\s+Repo[^\\n]*)", text)
    side = normalize_side(direction_raw or "REVERSE REPO", "FAB_REPO_EMAIL")

    isin = rx(r"ISIN\s*[:\-]?\s*\n?\s*([A-Z]{2}[A-Z0-9]{9,12})", text)
    if not isin:
        isin = rx(r"\b([A-Z]{2}[A-Z0-9]{9,12})\b", text)

    security_name = rx(r"Description\s*[:\-]?\s*\n?\s*(.+)", text)
    price_pct_raw = rx(r"All\s+in\s+Price\s*[:\-]?\s*\n?\s*([0-9,.]+)", text)
    nominal_raw = rx(r"Face\s+Amount\s*[:\-]?\s*\n?\s*([0-9,\.]+)", text)
    start_cash_raw = (
        rx(r"Start\s+Cash\s*[:\-]?\s*\n?\s*(?:USD\s+)?([0-9,\.]+)", text)
        or rx(r"Cash\s*[:\-]?\s*\n?\s*(?:USD\s+)?([0-9,\.]+)", text)
    )
    interest_raw = rx(r"Interest\s*[:\-]?\s*\n?\s*(?:USD\s+)?([0-9,\.]+)", text)
    trade_date_raw = rx(r"Trade\s+Date\s*[:\-]?\s*\n?\s*(.+)", text)
    settle_date_raw = rx(r"Settlement\s+Date\s*[:\-]?\s*\n?\s*(.+)", text)
    rate_raw = (
        rx(r"Fixed\s+Rate\s*[:\-]?\s*\n?\s*([0-9\.]+%?)", text)
        or rx(r"Repo\s+Rate\s*[:\-]?\s*\n?\s*([0-9\.]+%?)", text)
    )

    trade_date = parse_date_any(trade_date_raw, prefer_day_first=True)
    value_date = parse_date_any(settle_date_raw, prefer_day_first=True)
    nominal = parse_decimal(nominal_raw)
    price_pct = parse_decimal(price_pct_raw)
    start_cash = parse_decimal(start_cash_raw)

    ref = build_generic_reference(isin, side, trade_date, value_date, nominal, price_pct, nominal)

    trade = build_trade_dict(
        internet_message_id=internet_message_id,
        source_file="EMAIL_BODY",
        source_type="email_body",
        broker_name=broker_name,
        security_name=security_name,
        isin=isin,
        side=side,
        trade_date=trade_date,
        value_date=value_date,
        quantity=nominal,
        price=None,
        price_currency="USD",
        consideration=start_cash,
        commission=None,
        net_amount=start_cash,
        settlement_terms="REPO",
        counterparty_reference=ref,
        nominal=nominal,
        price_in_percentage=price_pct,
        accrued_interest=parse_decimal(interest_raw),
        settlement_currency="USD",
        parser_template="FAB_REPO_EMAIL",
        raw_json=json.dumps({
            "direction_raw": direction_raw, "isin": isin,
            "security_name": security_name, "price_pct_raw": price_pct_raw,
            "nominal_raw": nominal_raw, "start_cash_raw": start_cash_raw,
            "interest_raw": interest_raw, "trade_date_raw": trade_date_raw,
            "settle_date_raw": settle_date_raw, "rate_raw": rate_raw,
        }, default=str),
        processing_run_id=processing_run_id,
        file_id=None,
        email_id=email_id,
        side_original_text=direction_raw,
        trade_date_original_text=trade_date_raw,
        value_date_original_text=settle_date_raw,
    )
    trade = normalize_trade_signs(trade)
    finalize_trade_validation(trade, email_received_at)
    return [trade]


# =============================================================================
# PDF ROUTER
# =============================================================================
def parse_pdf_file(
    file_bytes: bytes,
    filename: str,
    internet_message_id: str,
    sender: str,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    file_id: Optional[int],
    email_id: Optional[int],
    mapping_by_sender: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    text = extract_pdf_text(file_bytes)
    template_code = detect_template_from_mapping(sender, mapping_by_sender)
    broker_name = resolve_broker_name_from_mapping(sender, mapping_by_sender)

    if template_code == "CUB_PDF":
        return parse_cub_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "CAMCAP_PDF":
        return parse_camcap_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "ZARATTINI_PDF":
        return parse_zarattini_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "ASHENDEN_PDF":
        return parse_ashenden_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "STONEX_PDF":
        return parse_stonex_fixed_income_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code in {"BONDPARTNERS_PDF", "BPL_PDF"}:
        return parse_bondpartners_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "SEAPORT_PDF":
        return parse_seaport_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "BRIDPORT_PDF":
        return parse_bridport_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "GTN_XLS_PDF":
        return parse_gtn_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    if template_code == "STONEX_DAILY_STATEMENT_PDF":
        return parse_stonex_daily_statement_pdf(text, internet_message_id, filename, email_received_at, processing_run_id, file_id, email_id, broker_name)

    return []


# =============================================================================
# ATTACHMENT PROCESSING
# =============================================================================
def parse_single_attachment(
    conn,
    internet_message_id: str,
    sender: str,
    filename: str,
    file_bytes: bytes,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    email_id: Optional[int],
    attachment_order: Optional[int] = None,
    parent_zip_file_name: Optional[str] = None,
    mapping_by_sender: Optional[Dict[str, Dict[str, Any]]] = None,
) -> int:
    parsed_count = 0
    seen_keys = set()
    file_hash = sha256_bytes(file_bytes)
    file_type = infer_file_type(filename)
    mapping_by_sender = mapping_by_sender or {}

    file_id = insert_settlement_file(
        conn=conn,
        internet_message_id=internet_message_id,
        file_name=filename,
        file_type=file_type,
        file_hash=file_hash,
        attachment_size=len(file_bytes),
        attachment_order=attachment_order,
        parent_zip_file_name=parent_zip_file_name,
        parse_status="RECEIVED",
        parse_note=None,
    )

    template_code = detect_template_from_mapping(sender, mapping_by_sender)

    if file_type in {"xlsx", "xlsm", "xls"}:
        dfs = extract_excel_sheets(file_bytes, filename)
        logging.info("Excel file %s produced %s dataframes", filename, len(dfs))

        for df in dfs:
            trades: List[Dict[str, Any]] = []

            if template_code == "INSTINET_XLSM":
                trades = parse_instinet_excel(
                    df=df,
                    internet_message_id=internet_message_id,
                    source_file=filename,
                    sender=sender,
                    email_received_at=email_received_at,
                    processing_run_id=processing_run_id,
                    file_id=file_id,
                    email_id=email_id,
                    mapping_by_sender=mapping_by_sender,
                )
            elif template_code == "GTN_XLS_PDF":
                trades = parse_gtn_excel(
                    df=df,
                    internet_message_id=internet_message_id,
                    source_file=filename,
                    sender=sender,
                    email_received_at=email_received_at,
                    processing_run_id=processing_run_id,
                    file_id=file_id,
                    email_id=email_id,
                    mapping_by_sender=mapping_by_sender,
                )

            logging.info("Excel file %s parsed trades=%s", filename, len(trades))

            for trade in trades:
                dedup_key = trade_dedup_key(trade)
                if dedup_key in seen_keys:
                    logging.info("Skipping duplicate parsed trade: %s", dedup_key)
                    continue
                seen_keys.add(dedup_key)

                upsert_settlement_trade(conn, trade)
                parsed_count += 1

    elif file_type == "pdf":
        trades = parse_pdf_file(
            file_bytes=file_bytes,
            filename=filename,
            internet_message_id=internet_message_id,
            sender=sender,
            email_received_at=email_received_at,
            processing_run_id=processing_run_id,
            file_id=file_id,
            email_id=email_id,
            mapping_by_sender=mapping_by_sender,
        )
        logging.info("PDF file %s parsed trades=%s", filename, len(trades))

        for trade in trades:
            dedup_key = trade_dedup_key(trade)
            if dedup_key in seen_keys:
                logging.info("Skipping duplicate parsed trade: %s", dedup_key)
                continue
            seen_keys.add(dedup_key)

            upsert_settlement_trade(conn, trade)
            parsed_count += 1

    elif file_type == "zip":
        try:
            with zipfile.ZipFile(io.BytesIO(file_bytes), "r") as zf:
                members = [m for m in zf.infolist() if not m.is_dir()]
                logging.info("ZIP file %s contains %s files", filename, len(members))

                for idx, member in enumerate(members, start=1):
                    inner_name = member.filename
                    inner_bytes = zf.read(member)
                    normalized_inner_name = inner_name.split("/")[-1].split("\\")[-1]

                    parsed_count += parse_single_attachment(
                        conn=conn,
                        internet_message_id=internet_message_id,
                        sender=sender,
                        filename=normalized_inner_name,
                        file_bytes=inner_bytes,
                        email_received_at=email_received_at,
                        processing_run_id=processing_run_id,
                        email_id=email_id,
                        attachment_order=idx,
                        parent_zip_file_name=filename,
                        mapping_by_sender=mapping_by_sender,
                    )
        except zipfile.BadZipFile:
            logging.exception("Bad ZIP file: %s", filename)
        except Exception as e:
            logging.exception("Failed processing ZIP file %s: %s", filename, e)

    with conn.cursor() as cur:
        cur.execute(
            """
            update back_office_auto.settlement_files
               set parse_status = %s,
                   parse_note = %s
             where id = %s
            """,
            (
                "PARSED" if parsed_count > 0 else "NO_TRADES_FOUND",
                f"parsed_trades={parsed_count}",
                file_id,
            ),
        )
    conn.commit()

    return parsed_count


# =============================================================================
# DEBUG / TEST PARSING WITHOUT WRITING
# =============================================================================
def parse_single_attachment_dry_run(
    internet_message_id: str,
    sender: str,
    filename: str,
    file_bytes: bytes,
    email_received_at: Optional[datetime],
    processing_run_id: Optional[int],
    mapping_by_sender: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    parsed_count = 0
    seen_keys = set()
    file_type = infer_file_type(filename)
    mapping_by_sender = mapping_by_sender or {}
    template_code = detect_template_from_mapping(sender, mapping_by_sender)

    result = {
        "file_name": filename,
        "file_type": file_type,
        "template_code": template_code,
        "parsed_trade_count": 0,
        "parsed_trades": [],
        "status": "NO_TRADES_FOUND",
        "error": None,
    }

    try:
        if file_type in {"xlsx", "xlsm", "xls"}:
            dfs = extract_excel_sheets(file_bytes, filename)
            all_trades = []

            for df in dfs:
                trades: List[Dict[str, Any]] = []

                if template_code == "INSTINET_XLSM":
                    trades = parse_instinet_excel(
                        df=df,
                        internet_message_id=internet_message_id,
                        source_file=filename,
                        sender=sender,
                        email_received_at=email_received_at,
                        processing_run_id=processing_run_id,
                        file_id=None,
                        email_id=None,
                        mapping_by_sender=mapping_by_sender,
                    )
                elif template_code == "GTN_XLS_PDF":
                    trades = parse_gtn_excel(
                        df=df,
                        internet_message_id=internet_message_id,
                        source_file=filename,
                        sender=sender,
                        email_received_at=email_received_at,
                        processing_run_id=processing_run_id,
                        file_id=None,
                        email_id=None,
                        mapping_by_sender=mapping_by_sender,
                    )

                for trade in trades:
                    dedup_key = trade_dedup_key(trade)
                    if dedup_key in seen_keys:
                        continue
                    seen_keys.add(dedup_key)
                    parsed_count += 1
                    all_trades.append(trade)

            result["parsed_trades"] = all_trades

        elif file_type == "pdf":
            trades = parse_pdf_file(
                file_bytes=file_bytes,
                filename=filename,
                internet_message_id=internet_message_id,
                sender=sender,
                email_received_at=email_received_at,
                processing_run_id=processing_run_id,
                file_id=None,
                email_id=None,
                mapping_by_sender=mapping_by_sender,
            )

            all_trades = []
            for trade in trades:
                dedup_key = trade_dedup_key(trade)
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                parsed_count += 1
                all_trades.append(trade)

            result["parsed_trades"] = all_trades

        elif file_type == "zip":
            zip_results = []
            with zipfile.ZipFile(io.BytesIO(file_bytes), "r") as zf:
                members = [m for m in zf.infolist() if not m.is_dir()]
                for member in members:
                    inner_name = member.filename
                    inner_bytes = zf.read(member)
                    normalized_inner_name = inner_name.split("/")[-1].split("\\")[-1]

                    child_result = parse_single_attachment_dry_run(
                        internet_message_id=internet_message_id,
                        sender=sender,
                        filename=normalized_inner_name,
                        file_bytes=inner_bytes,
                        email_received_at=email_received_at,
                        processing_run_id=processing_run_id,
                        mapping_by_sender=mapping_by_sender,
                    )
                    zip_results.append(child_result)
                    parsed_count += child_result.get("parsed_trade_count", 0)

            result["zip_children"] = zip_results
            result["parsed_trades"] = []

        else:
            result["status"] = "UNSUPPORTED_FILE_TYPE"

        result["parsed_trade_count"] = parsed_count
        result["status"] = "PARSED" if parsed_count > 0 else result["status"]

    except Exception as e:
        logging.exception("Dry-run parsing failed for file %s: %s", filename, e)
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result


def process_message_for_debug(
    conn,
    token: str,
    mailbox: str,
    msg: Dict[str, Any],
    mapping_by_sender: Dict[str, Dict[str, Any]],
    processing_run_id: int,
    dry_run: bool = True,
) -> Dict[str, Any]:
    message_id = msg["id"]
    internet_message_id = clean_text(msg.get("internetMessageId"))
    subject = clean_text(msg.get("subject"))
    received_at_raw = msg.get("receivedDateTime")
    sender = normalize_email_address(msg.get("from", {}))

    if isinstance(received_at_raw, str):
        try:
            received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
        except Exception:
            received_at = None
    else:
        received_at = received_at_raw

    result = {
        "message_id": message_id,
        "internet_message_id": internet_message_id,
        "sender": sender,
        "subject": subject,
        "received_at": str(received_at) if received_at else None,
        "template_code": detect_template_from_mapping(sender, mapping_by_sender),
        "has_mapping": sender in mapping_by_sender,
        "body_parsed_trade_count": 0,
        "attachments_total": 0,
        "attachments_processed": [],
        "total_parsed_trade_count": 0,
        "status": "SKIPPED",
        "error": None,
    }

    if not internet_message_id:
        result["status"] = "SKIPPED_NO_INTERNET_MESSAGE_ID"
        return result

    if sender not in mapping_by_sender:
        result["status"] = "SKIPPED_NO_MAPPING"
        return result

    if sender == "bo.tdsm@zarattinibank.ch" and "confirmation" not in subject.lower():
        logging.warning("ZARATTINI SUBJECT FILTER HIT DEBUG | subject=%s", subject)
        result["status"] = "SKIPPED_SUBJECT_FILTER"
        return result

    try:
        full_msg = get_message_full(token, mailbox, message_id)
        body_html = (full_msg.get("body", {}) or {}).get("content") or ""
        body_text = strip_html_tags(body_html)
        attachments = get_message_attachments(token, mailbox, message_id)
        result["attachments_total"] = len(attachments)

        template_code = detect_template_from_mapping(sender, mapping_by_sender)

        _EMAIL_BODY_PARSERS = {
            "STONEX_REPO_EMAIL": parse_stonex_repo_email_body,
            "GRANT_WESTOVER_REPO_EMAIL": parse_grant_westover_email_body,
            "FAB_REPO_EMAIL": parse_fab_repo_email_body,
        }

        if template_code in _EMAIL_BODY_PARSERS:
            repo_trades = _EMAIL_BODY_PARSERS[template_code](
                body_text=body_text,
                internet_message_id=internet_message_id,
                sender=sender,
                email_received_at=received_at,
                processing_run_id=processing_run_id,
                email_id=None,
                mapping_by_sender=mapping_by_sender,
            )
            result["body_parsed_trade_count"] = len(repo_trades)
            result["total_parsed_trade_count"] += len(repo_trades)

            if not dry_run:
                email_id = insert_settlement_email(
                    conn=conn,
                    internet_message_id=internet_message_id,
                    message_id=message_id,
                    sender=sender,
                    subject=subject,
                    received_at=received_at,
                    status="DEBUG_REPARSE",
                    note="Debug reparse body",
                    mailbox=mailbox,
                    attachment_count=len(attachments),
                    parsed_trade_count=0,
                    processing_run_id=processing_run_id,
                )
                for trade in repo_trades:
                    trade["email_id"] = email_id
                    upsert_settlement_trade(conn, trade)

        for idx, att in enumerate(attachments, start=1):
            odata_type = att.get("@odata.type")
            filename = att.get("name") or "unnamed"
            attachment_id = att.get("id")

            if odata_type != "#microsoft.graph.fileAttachment":
                result["attachments_processed"].append({
                    "file_name": filename,
                    "status": "SKIPPED_NON_FILE_ATTACHMENT",
                })
                continue

            content_bytes_b64 = att.get("contentBytes")
            if content_bytes_b64:
                file_bytes = base64.b64decode(content_bytes_b64)
            else:
                if not attachment_id:
                    result["attachments_processed"].append({
                        "file_name": filename,
                        "status": "SKIPPED_NO_ATTACHMENT_ID",
                    })
                    continue

                file_bytes = get_attachment_content_bytes(
                    token=token,
                    mailbox=mailbox,
                    message_id=message_id,
                    attachment_id=attachment_id,
                )

            if dry_run:
                att_result = parse_single_attachment_dry_run(
                    internet_message_id=internet_message_id,
                    sender=sender,
                    filename=filename,
                    file_bytes=file_bytes,
                    email_received_at=received_at,
                    processing_run_id=processing_run_id,
                    mapping_by_sender=mapping_by_sender,
                )
                result["attachments_processed"].append(att_result)
                result["total_parsed_trade_count"] += att_result.get("parsed_trade_count", 0)
            else:
                parsed_count = parse_single_attachment(
                    conn=conn,
                    internet_message_id=internet_message_id,
                    sender=sender,
                    filename=filename,
                    file_bytes=file_bytes,
                    email_received_at=received_at,
                    processing_run_id=processing_run_id,
                    email_id=None,
                    attachment_order=idx,
                    parent_zip_file_name=None,
                    mapping_by_sender=mapping_by_sender,
                )
                result["attachments_processed"].append({
                    "file_name": filename,
                    "status": "PARSED" if parsed_count > 0 else "NO_TRADES_FOUND",
                    "parsed_trade_count": parsed_count,
                })
                result["total_parsed_trade_count"] += parsed_count

        result["status"] = "PARSED" if result["total_parsed_trade_count"] > 0 else "NO_TRADES_FOUND"
        return result

    except Exception as e:
        logging.exception("Debug processing failed for message %s: %s", internet_message_id, e)
        result["status"] = "ERROR"
        result["error"] = str(e)
        return result


def debug_test_last_messages_parsing(
    conn,
    token: str,
    mailbox: str,
    senders: List[str],
    processing_run_id: int,
    top_n: int = 3,
    days_back: int = 120,
    dry_run: bool = True,
) -> Dict[str, Any]:
    mapping_by_sender = load_mapping(conn)
    since_dt = now_utc() - timedelta(days=days_back)

    summary = {
        "run_id": processing_run_id,
        "mailbox": mailbox,
        "top_n_per_sender": top_n,
        "days_back": days_back,
        "dry_run": dry_run,
        "total_senders": len(senders),
        "total_messages_found": 0,
        "total_messages_parsed": 0,
        "total_messages_no_trades": 0,
        "total_messages_errors": 0,
        "total_messages_skipped_subject_filter": 0,
        "total_trades_found": 0,
        "senders": [],
    }

    for sender in senders:
        sender = (sender or "").strip().lower()
        sender_result = {
            "sender": sender,
            "template_code": detect_template_from_mapping(sender, mapping_by_sender),
            "in_mapping": sender in mapping_by_sender,
            "messages_found": 0,
            "messages": [],
        }

        # For file-based parsers: require PDF/Excel attachment.
        # Fetch up to top_n*5 candidates so we can skip image-only emails
        # (e.g. image001.png logo in signature) and still find top_n real ones.
        # Email-body parsers need no attachment.
        template_for_sender = detect_template_from_mapping(sender, mapping_by_sender)
        needs_attachment = template_for_sender not in EMAIL_BODY_TEMPLATES

        try:
            candidates = list_recent_messages_by_sender_python_filter(
                token=token,
                mailbox=mailbox,
                sender_email=sender,
                since_dt=since_dt,
                top_n=top_n * 5 if needs_attachment else top_n,
                require_attachments=needs_attachment,
            )

            msgs = []
            if needs_attachment:
                # Process candidates until we collect top_n with real PDF/Excel
                for candidate in candidates:
                    if len(msgs) >= top_n:
                        break
                    msg_result = process_message_for_debug(
                        conn=conn,
                        token=token,
                        mailbox=mailbox,
                        msg=candidate,
                        mapping_by_sender=mapping_by_sender,
                        processing_run_id=processing_run_id,
                        dry_run=dry_run,
                    )
                    # Skip emails where every attachment was an unsupported type (image, etc.)
                    all_unsupported = (
                        msg_result.get("attachments_total", 0) > 0
                        and msg_result.get("total_parsed_trade_count", 0) == 0
                        and all(
                            a.get("status") == "UNSUPPORTED_FILE_TYPE"
                            for a in msg_result.get("attachments_processed", [])
                        )
                    )
                    if all_unsupported:
                        logging.info(
                            "Skipping image-only email from %s: %s",
                            sender, candidate.get("subject", "")[:60],
                        )
                        continue
                    msgs.append(candidate)
                    sender_result["messages"].append(msg_result)
                    summary["total_trades_found"] += msg_result.get("total_parsed_trade_count", 0)
                    if msg_result["status"] == "PARSED":
                        summary["total_messages_parsed"] += 1
                    elif msg_result["status"] == "NO_TRADES_FOUND":
                        summary["total_messages_no_trades"] += 1
                    elif msg_result["status"] == "ERROR":
                        summary["total_messages_errors"] += 1
                    elif msg_result["status"] == "SKIPPED_SUBJECT_FILTER":
                        summary["total_messages_skipped_subject_filter"] += 1
            else:
                msgs = candidates
                for msg in msgs:
                    msg_result = process_message_for_debug(
                        conn=conn,
                        token=token,
                        mailbox=mailbox,
                        msg=msg,
                        mapping_by_sender=mapping_by_sender,
                        processing_run_id=processing_run_id,
                        dry_run=dry_run,
                    )
                    sender_result["messages"].append(msg_result)
                    summary["total_trades_found"] += msg_result.get("total_parsed_trade_count", 0)
                    if msg_result["status"] == "PARSED":
                        summary["total_messages_parsed"] += 1
                    elif msg_result["status"] == "NO_TRADES_FOUND":
                        summary["total_messages_no_trades"] += 1
                    elif msg_result["status"] == "ERROR":
                        summary["total_messages_errors"] += 1
                    elif msg_result["status"] == "SKIPPED_SUBJECT_FILTER":
                        summary["total_messages_skipped_subject_filter"] += 1

            sender_result["messages_found"] = len(msgs)
            summary["total_messages_found"] += len(msgs)

        except Exception as e:
            logging.exception("Debug test failed for sender %s: %s", sender, e)
            sender_result["error"] = str(e)
            summary["total_messages_errors"] += 1

        summary["senders"].append(sender_result)

    return summary


# =============================================================================
# MESSAGE PROCESSING
# =============================================================================
def process_message(
    conn,
    token: str,
    mailbox: str,
    msg: Dict[str, Any],
    mapping_by_sender: Dict[str, Dict[str, Any]],
    processing_run_id: int,
) -> Tuple[str, int]:
    message_id = msg["id"]
    internet_message_id = clean_text(msg.get("internetMessageId"))
    subject = clean_text(msg.get("subject"))
    received_at_raw = msg.get("receivedDateTime")
    sender = normalize_email_address(msg.get("from", {}))

    if isinstance(received_at_raw, str):
        try:
            received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
        except Exception:
            received_at = None
    else:
        received_at = received_at_raw

    if not internet_message_id:
        return ("SKIPPED", 0)

    if sender not in mapping_by_sender:
        insert_settlement_email(
            conn=conn,
            internet_message_id=internet_message_id,
            message_id=message_id,
            sender=sender,
            subject=subject,
            received_at=received_at,
            status="SKIPPED",
            note="Sender not found in counterparty_email_mapping",
            mailbox=mailbox,
            attachment_count=0,
            parsed_trade_count=0,
            processing_run_id=processing_run_id,
        )
        return ("SKIPPED", 0)

    if sender == "bo.tdsm@zarattinibank.ch" and "confirmation" not in subject.lower():
        logging.warning("ZARATTINI SUBJECT FILTER HIT MAIN | subject=%s", subject)
        insert_settlement_email(
            conn=conn,
            internet_message_id=internet_message_id,
            message_id=message_id,
            sender=sender,
            subject=subject,
            received_at=received_at,
            status="SKIPPED",
            note="Zarattini subject filter: 'confirmation' not in subject",
            mailbox=mailbox,
            attachment_count=0,
            parsed_trade_count=0,
            processing_run_id=processing_run_id,
        )
        return ("SKIPPED", 0)

    if email_already_processed(conn, internet_message_id):
        return ("ALREADY_PROCESSED", 0)

    full_msg = get_message_full(token, mailbox, message_id)
    body_html = (full_msg.get("body", {}) or {}).get("content") or ""
    body_text = strip_html_tags(body_html)
    attachments = get_message_attachments(token, mailbox, message_id)

    email_id = insert_settlement_email(
        conn=conn,
        internet_message_id=internet_message_id,
        message_id=message_id,
        sender=sender,
        subject=subject,
        received_at=received_at,
        status="RECEIVED",
        note="Message received for parsing",
        mailbox=mailbox,
        attachment_count=len(attachments),
        parsed_trade_count=0,
        processing_run_id=processing_run_id,
    )

    parsed_count = 0

    template_code = detect_template_from_mapping(sender, mapping_by_sender)
    _EMAIL_BODY_PARSERS = {
        "STONEX_REPO_EMAIL": parse_stonex_repo_email_body,
        "GRANT_WESTOVER_REPO_EMAIL": parse_grant_westover_email_body,
        "FAB_REPO_EMAIL": parse_fab_repo_email_body,
    }
    if template_code in _EMAIL_BODY_PARSERS:
        repo_trades = _EMAIL_BODY_PARSERS[template_code](
            body_text=body_text,
            internet_message_id=internet_message_id,
            sender=sender,
            email_received_at=received_at,
            processing_run_id=processing_run_id,
            email_id=email_id,
            mapping_by_sender=mapping_by_sender,
        )
        for trade in repo_trades:
            upsert_settlement_trade(conn, trade)
            parsed_count += 1

    for idx, att in enumerate(attachments, start=1):
        odata_type = att.get("@odata.type")
        filename = att.get("name") or "unnamed"
        attachment_id = att.get("id")

        if odata_type != "#microsoft.graph.fileAttachment":
            continue

        content_bytes_b64 = att.get("contentBytes")
        if content_bytes_b64:
            file_bytes = base64.b64decode(content_bytes_b64)
        else:
            if not attachment_id:
                continue
            try:
                file_bytes = get_attachment_content_bytes(
                    token=token,
                    mailbox=mailbox,
                    message_id=message_id,
                    attachment_id=attachment_id,
                )
            except Exception as e:
                logging.exception("Failed to load attachment body for %s: %s", filename, e)
                continue

        parsed_count += parse_single_attachment(
            conn=conn,
            internet_message_id=internet_message_id,
            sender=sender,
            filename=filename,
            file_bytes=file_bytes,
            email_received_at=received_at,
            processing_run_id=processing_run_id,
            email_id=email_id,
            attachment_order=idx,
            parent_zip_file_name=None,
            mapping_by_sender=mapping_by_sender,
        )

    status = "PARSED" if parsed_count > 0 else "NO_TRADES_FOUND"
    note = f"Parsed trades: {parsed_count}"

    insert_settlement_email(
        conn=conn,
        internet_message_id=internet_message_id,
        message_id=message_id,
        sender=sender,
        subject=subject,
        received_at=received_at,
        status=status,
        note=note,
        mailbox=mailbox,
        attachment_count=len(attachments),
        parsed_trade_count=parsed_count,
        processing_run_id=processing_run_id,
    )

    return (status, parsed_count)


# =============================================================================
# TIMERS
# =============================================================================
@app.function_name(name="settlement_email_parser_timer")
@app.schedule(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def settlement_email_parser_timer(mytimer: func.TimerRequest) -> None:
    logging.info("### settlement_email_parser_timer started ###")

    run_id = None
    conn = None

    try:
        token = get_graph_token()
        conn = get_conn()
        run_id = start_agent_run(conn, "settlement_email_parser_timer")

        mapping_by_sender = load_mapping(conn)
        allowed_senders = get_allowed_senders(mapping_by_sender)

        since_dt = now_utc() - timedelta(hours=LOOKBACK_HOURS)
        messages = list_recent_messages(token, GRAPH_MAILBOX, since_dt)

        total = 0
        parsed_messages = 0
        parsed_trades = 0
        skipped = 0

        for msg in messages:
            total += 1
            sender = normalize_email_address(msg.get("from", {}))
            if sender not in allowed_senders:
                skipped += 1
                continue

            status, count = process_message(
                conn=conn,
                token=token,
                mailbox=GRAPH_MAILBOX,
                msg=msg,
                mapping_by_sender=mapping_by_sender,
                processing_run_id=run_id,
            )

            if status in {"PARSED", "NO_TRADES_FOUND"}:
                parsed_messages += 1
            elif status in {"SKIPPED", "ALREADY_PROCESSED"}:
                skipped += 1

            parsed_trades += count

        finish_agent_run(
            conn,
            run_id,
            "SUCCESS",
            f"Checked messages={total}, processed_messages={parsed_messages}, parsed_trades={parsed_trades}, skipped={skipped}",
        )

        logging.info("Settlement parsing completed successfully")

    except Exception as e:
        logging.exception("Settlement parsing failed: %s", e)
        try:
            if conn and run_id:
                finish_agent_run(conn, run_id, "FAILED", str(e))
        except Exception:
            logging.exception("Failed to mark run as FAILED")
        raise

    finally:
        if conn:
            conn.close()


# =============================================================================
# HTTP DEBUG FUNCTION TO TEST EXISTING EMAILS
# =============================================================================
@app.function_name(name="debug_settlement_sender_test")
@app.route(route="debug/settlement-sender-test", auth_level=func.AuthLevel.FUNCTION, methods=["GET", "POST"])
def debug_settlement_sender_test(req: func.HttpRequest) -> func.HttpResponse:
    conn = None
    run_id = None

    try:
        body = {}
        try:
            body = req.get_json()
        except Exception:
            body = {}

        senders_param = body.get("senders") or req.params.get("senders")
        top_n_param = body.get("top_n") or req.params.get("top_n")
        days_back_param = body.get("days_back") or req.params.get("days_back")
        dry_run_param = body.get("dry_run") if "dry_run" in body else req.params.get("dry_run")
        mailbox_param = body.get("mailbox") or req.params.get("mailbox")

        if senders_param:
            if isinstance(senders_param, list):
                senders = [str(x).strip().lower() for x in senders_param if str(x).strip()]
            else:
                senders = [x.strip().lower() for x in str(senders_param).split(",") if x.strip()]
        else:
            senders = TEST_SENDERS_DEFAULT

        top_n = int(top_n_param) if top_n_param else 3
        days_back = int(days_back_param) if days_back_param else 120

        if isinstance(dry_run_param, bool):
            dry_run = dry_run_param
        else:
            dry_run = str(dry_run_param or "true").strip().lower() in {"true", "1", "yes", "y"}

        mailbox_to_use = (mailbox_param or GRAPH_MAILBOX).strip()

        token = get_graph_token()
        conn = get_conn()
        run_id = start_agent_run(conn, "debug_settlement_sender_test")

        result = debug_test_last_messages_parsing(
            conn=conn,
            token=token,
            mailbox=mailbox_to_use,
            senders=senders,
            processing_run_id=run_id,
            top_n=top_n,
            days_back=days_back,
            dry_run=dry_run,
        )

        result["code_version"] = "2026-03-16-zarattini-mailbox-fix-v1"
        result["generated_at_utc"] = str(now_utc())
        logging.warning("DEBUG MAILBOX USED: %s", mailbox_to_use)

        finish_agent_run(
            conn,
            run_id,
            "SUCCESS",
            (
                f"senders={len(senders)}, "
                f"messages_found={result['total_messages_found']}, "
                f"messages_parsed={result['total_messages_parsed']}, "
                f"messages_no_trades={result['total_messages_no_trades']}, "
                f"errors={result['total_messages_errors']}, "
                f"trades_found={result['total_trades_found']}, "
                f"dry_run={dry_run}"
            ),
        )

        return func.HttpResponse(
            json.dumps(result, default=str, indent=2),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.exception("debug_settlement_sender_test failed: %s", e)
        try:
            if conn and run_id:
                finish_agent_run(conn, run_id, "FAILED", str(e))
        except Exception:
            logging.exception("Failed to mark debug run as FAILED")

        return func.HttpResponse(
            json.dumps({"status": "ERROR", "error": str(e)}, indent=2),
            mimetype="application/json",
            status_code=500,
        )

    finally:
        if conn:
            conn.close()


# =============================================================================
# SETTLEMENT RECONCILIATION
# =============================================================================
# =============================================================================
# RECONCILIATION HELPERS
# =============================================================================
def values_equal_decimal(a, b, tolerance: Decimal = Decimal("0.0001")) -> bool:
    da = parse_decimal(a)
    db = parse_decimal(b)

    if da is None and db is None:
        return True
    if da is None or db is None:
        return False

    return abs(da - db) <= tolerance


def build_reconciliation_key(st: Dict[str, Any]) -> str:
    return "|".join([
        clean_text(str(st.get("isin") or "")),
        clean_text(str(st.get("side") or "")),
        clean_text(str(st.get("trade_date") or "")),
        clean_text(str(st.get("value_date") or "")),
        clean_text(str(st.get("quantity") or st.get("nominal") or "")),
        clean_text(str(st.get("price") or st.get("price_in_percentage") or "")),
    ])


def load_settlement_trades_for_reconciliation(
    conn,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    value_date_from: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """
    Loads settlement trades (parsed from counterparty emails) for reconciliation.
    date_from / date_to filter by trade_date (inclusive).
    value_date_from: exclude confos that already settled before this date (value_date < value_date_from).
    """
    where = "where validation_status = 'PARSED'"
    params: list = []
    if date_from is not None:
        where += " AND trade_date >= %s"
        params.append(date_from)
    if date_to is not None:
        where += " AND trade_date <= %s"
        params.append(date_to)
    if value_date_from is not None:
        where += " AND (value_date IS NULL OR value_date >= %s)"
        params.append(value_date_from)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            select
                id,
                internet_message_id,
                source_file,
                source_type,
                broker_name,
                security_name,
                isin,
                side,
                trade_date,
                value_date,
                quantity,
                price,
                price_currency,
                consideration,
                commission,
                net_amount,
                nominal,
                price_in_percentage,
                accrued_interest,
                settlement_currency,
                parser_template,
                validation_status,
                validation_note,
                created_at
            from back_office_auto.settlement_trades
            {where}
            order by created_at desc, id desc
            """,
            params or None,
        )
        return [dict(r) for r in cur.fetchall()]


def load_strict_deals_to_process(
    conn,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    include_settled: bool = False,
) -> List[Dict[str, Any]]:
    """
    Loads internal deals (tab_deals) eligible for reconciliation comparison.
    date_from / date_to filter by trade_date (inclusive).
    include_settled=True includes status=4 (SETTLED) deals (for Table A matching).
    """
    extra = ""
    params: list = []
    if date_from is not None:
        extra += " AND trades.trade_date >= %s"
        params.append(date_from)
    if date_to is not None:
        extra += " AND trades.trade_date <= %s"
        params.append(date_to)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                trades.id,
                trades.id AS back_id,
                trades.deal,
                trades.comment,
                CASE
                    WHEN trades.action = 0 THEN 'BUY'
                    WHEN trades.action = 2 THEN 'BALANCE'
                    WHEN trades.action = 1 THEN 'SELL'
                END AS direction,
                trades.symbol,
                trades.qty,
                trades.price,
                trades.price_in_percentage,
                trades.currency_price,
                trades.transaction_value,
                trades.currency_pay,
                trades.login,
                cp.name AS counterparty,
                trades.trade_date,
                trades.value_date_cash,
                trades.value_date_securities,
                trades.settle_date_cash,
                trades.settle_date_securities,
                trades.type_calculations,
                trades.settle_type,
                s.description AS status,
                trades.reason,
                trades.commission,
                trades.commission_fee,
                trades.dealer,
                trades.lot,
                trades.nominal,
                trades.accrued,
                trades.external_id,
                trades.order_id,
                trades.time
            FROM back_office.tab_deals trades
            LEFT JOIN back_office.tab_counterparty cp
                ON trades.counterparty_id = cp.id
            LEFT JOIN back_office.tab_status s
                ON trades.status = s.id
            WHERE trades.reason = 0
              AND trades.status NOT IN (4, 7)
              AND trades.login <> 1007
              AND trades.type_deal <> 2
              AND trades.settle_type = 'external'
              {extra}
            ORDER BY trades.trade_date DESC, trades.time DESC
            """,
            params or None,
        )
        return [dict(r) for r in cur.fetchall()]


def load_broad_trade_search(conn) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                trades.id,
                trades.id AS back_id,
                CASE
                    WHEN trades.action = 0 THEN 'BUY'
                    WHEN trades.action = 2 THEN 'BALANCE'
                    WHEN trades.action = 1 THEN 'SELL'
                END AS direction,
                trades.symbol,
                trades.qty,
                trades.price,
                trades.price_in_percentage,
                trades.transaction_value,
                trades.currency_pay,
                cp.name AS counterparty,
                trades.trade_date,
                trades.value_date_cash,
                trades.settle_date_cash,
                trades.settle_type,
                trades.reason,
                trades.status,
                trades.login,
                trades.order_id,
                trades.time,
                trades.nominal,
                trades.accrued
            FROM back_office.tab_deals trades
            LEFT JOIN back_office.tab_counterparty cp
                ON trades.counterparty_id = cp.id
            ORDER BY trades.trade_date DESC, trades.time DESC
            """
        )
        return [dict(r) for r in cur.fetchall()]


# =============================================================================
# T0 / T1 DATE HELPERS
# =============================================================================
# Dubai timezone is UTC+4.
# T0 = trade date (today Dubai).
# T1 = next business day (tomorrow Dubai, or Monday if Friday).
# The rules define WHEN reconciliation window opens per currency:
#   AED, HKD, EUR, GBP, CHF  → compare window opens at 17:00 T0 Dubai
#   USD                       → compare window opens at 08:00 T1 Dubai
#   Other                     → compare window opens at 08:00 T1 Dubai
#
# For DATA FILTERING in reconciliation we include:
#   - settlement_trades with trade_date in [t1_date .. t0_date] (last 2 business days)
#   - tab_deals with trade_date in [t1_date .. t0_date]
# =============================================================================

DUBAI_TZ = timezone(timedelta(hours=4))

# Currencies settled same day (T+0 — compare at T0 5pm)
T0_CURRENCIES = frozenset({"AED", "HKD", "EUR", "GBP", "CHF"})

# Hour thresholds (Dubai time) after which the comparison window is open
T0_COMPARE_HOUR = 17   # 5pm Dubai  — for T0 currencies
T1_COMPARE_HOUR = 8    # 8am Dubai  — for T1 currencies (USD, Other)


def prev_business_day(d: date) -> date:
    """Return the previous calendar business day (Mon–Fri), skipping weekends."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:   # 5=Sat, 6=Sun
        prev -= timedelta(days=1)
    return prev


def next_business_day(d: date) -> date:
    """Return the next calendar business day (Mon–Fri), skipping weekends."""
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:   # 5=Sat, 6=Sun
        nxt += timedelta(days=1)
    return nxt


def n_prev_business_days(d: date, n: int) -> date:
    """Return the date n business days before d."""
    result = d
    for _ in range(n):
        result = prev_business_day(result)
    return result


def get_t0_t1_dates() -> tuple[date, date, date]:
    """
    Returns (t0_date, t1_date, t_next_date) in Dubai timezone:
      t0_date      = today (Dubai)
      t1_date      = 5 business days ago — covers holiday gaps
      t_next_date  = next business day (Dubai)
    """
    t0 = datetime.now(DUBAI_TZ).date()
    t1 = n_prev_business_days(t0, 5)
    t_next = next_business_day(t0)
    return t0, t1, t_next


def is_reconciliation_window_open(currency: str | None) -> bool:
    """
    Returns True if the reconciliation compare window is currently open
    for the given currency based on Dubai time.
    """
    now_dubai = datetime.now(DUBAI_TZ)
    ccy = (currency or "").upper()
    if ccy in T0_CURRENCIES:
        return now_dubai.hour >= T0_COMPARE_HOUR
    else:
        return now_dubai.hour >= T1_COMPARE_HOUR


# =============================================================================
# RECONCILIATION FIELD MAPPING
# =============================================================================
# Показывает какие поля из settlement_trades сравниваются с полями tab_deals.
#
# ЖЁСТКИЙ ФИЛЬТР (все три обязательны — без совпадения кандидат не рассматривается):
#
#   settlement_trades.isin          ==  tab_deals.symbol
#   settlement_trades.side          ==  tab_deals.direction   (action: 0→BUY, 1→SELL)
#   settlement_trades.trade_date    ==  tab_deals.trade_date
#
# SCORE (нужно набрать ≥ 90 из максимальных 90 баллов):
#
#   +20  settlement_trades.value_date      ==  tab_deals.settle_date_cash  (или value_date_cash если settle_date_cash пустой)
#   +30  settlement_trades.quantity        ≈   tab_deals.qty               (допуск ±0.0001)
#   +20  settlement_trades.price           ≈   tab_deals.price             (допуск ±0.0001)
#   +20  settlement_trades.consideration   ≈   tab_deals.transaction_value (допуск ±0.01)
#
# АГРЕГИРОВАННЫЙ МАТЧИНГ (если точного нет):
#   SUM(tab_deals.qty)                ≈   settlement_trades.quantity      (допуск ±0.0001)
#   SUM(tab_deals.transaction_value)  ≈   settlement_trades.consideration (допуск ±0.01)
#   группировка по tab_deals.settle_date_cash / value_date_cash
#
# ПОХОЖИЕ (SIMILAR): те же кандидаты из broad search по isin в пределах ±3 дней от trade_date
# =============================================================================

def exact_score(st: Dict[str, Any], td: Dict[str, Any]) -> Tuple[int, List[str]]:
    score = 0
    notes = []

    td_value_date = td.get("settle_date_cash") or td.get("value_date_cash")

    if st.get("value_date") == td_value_date:
        score += 20
    else:
        notes.append("value_date_mismatch")

    if st.get("quantity") is not None and td.get("qty") is not None:
        if values_equal_decimal(st.get("quantity"), td.get("qty")):
            score += 30
        else:
            notes.append("quantity_mismatch")

    if st.get("price") is not None and td.get("price") is not None:
        if values_equal_decimal(st.get("price"), td.get("price"), Decimal("0.5")):
            score += 20
        else:
            notes.append("price_mismatch")

    if st.get("consideration") is not None and td.get("transaction_value") is not None:
        if values_equal_decimal(st.get("consideration"), td.get("transaction_value"), Decimal("0.50")):
            score += 20
        else:
            notes.append("amount_mismatch")

    return score, notes


def find_strict_candidates(st: Dict[str, Any], deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []

    for td in deals:
        if clean_text(st.get("isin")) != clean_text(td.get("symbol")):
            continue
        if clean_text(st.get("side")) != clean_text(td.get("direction")):
            continue
        if st.get("trade_date") != td.get("trade_date"):
            continue
        candidates.append(td)

    return candidates


def try_exact_single_match(
    st: Dict[str, Any],
    candidates: List[Dict[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[List[str]]]:
    if not candidates:
        return None, None, None

    scored = []
    for td in candidates:
        score, notes = exact_score(st, td)
        scored.append((score, notes, td))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_notes, best_td = scored[0]

    if best_score >= 90:
        return best_td, "MATCHED", best_notes

    return best_td, None, best_notes


def try_aggregate_match(
    st: Dict[str, Any],
    candidates: List[Dict[str, Any]]
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str], Optional[str]]:
    if not candidates:
        return None, None, None

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for td in candidates:
        key = str(td.get("settle_date_cash") or td.get("value_date_cash") or "")
        groups.setdefault(key, []).append(td)

    for _, rows in groups.items():
        summed_qty = sum([float(r.get("qty") or 0) for r in rows])
        summed_amount = sum([float(r.get("transaction_value") or 0) for r in rows])

        qty_ok = st.get("quantity") is not None and values_equal_decimal(st.get("quantity"), summed_qty)
        amount_ok = st.get("consideration") is not None and values_equal_decimal(
            st.get("consideration"), summed_amount, Decimal("0.01")
        )

        if qty_ok and amount_ok:
            return rows, "MATCHED_AGGREGATED", f"matched against {len(rows)} internal rows"

    return None, None, None


def find_similar_broad_rows(st: Dict[str, Any], broad_deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    similar = []
    st_trade_date = st.get("trade_date")
    date_window = 3

    for td in broad_deals:
        if clean_text(st.get("isin")) != clean_text(td.get("symbol")):
            continue

        td_trade_date = td.get("trade_date")
        if st_trade_date and td_trade_date:
            date_diff = abs((st_trade_date - td_trade_date).days)
            if date_diff > date_window:
                continue

        similar.append(td)

    similar.sort(key=lambda x: (x.get("trade_date") or date.min, x.get("id") or 0), reverse=True)
    return similar[:5]


def upsert_reconciliation_result(
    conn,
    settlement_trade_id: int,
    internal_order_id: Optional[int],
    match_status: str,
    match_note: Optional[str],
    reconciliation_key: str,
    counterparty: Optional[str],
    isin: Optional[str],
    direction: Optional[str],
    trade_date: Optional[date],
    value_date: Optional[date],
    external_qty,
    internal_qty,
    external_amount,
    internal_amount,
    compare_side: Optional[str],
    run_id: Optional[int],
    matched_internal_ids: Optional[str] = None,
    matched_internal_count: Optional[int] = None,
    mismatch_json: Optional[str] = None,
    external_price=None,
    internal_price=None,
    external_value_date: Optional[date] = None,
    internal_value_date: Optional[date] = None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into back_office_auto.settlement_reconciliation
            (
                settlement_trade_id,
                internal_order_id,
                match_status,
                match_note,
                created_at,
                reconciliation_key,
                counterparty,
                isin,
                direction,
                trade_date,
                value_date,
                external_qty,
                internal_qty,
                external_amount,
                internal_amount,
                compare_side,
                run_id,
                matched_internal_ids,
                matched_internal_count,
                mismatch_json,
                external_price,
                internal_price,
                external_value_date,
                internal_value_date
            )
            values
            (%s, %s, %s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            on conflict (settlement_trade_id, reconciliation_key)
            do update set
                internal_order_id = excluded.internal_order_id,
                match_status = excluded.match_status,
                match_note = excluded.match_note,
                counterparty = excluded.counterparty,
                isin = excluded.isin,
                direction = excluded.direction,
                trade_date = excluded.trade_date,
                value_date = excluded.value_date,
                external_qty = excluded.external_qty,
                internal_qty = excluded.internal_qty,
                external_amount = excluded.external_amount,
                internal_amount = excluded.internal_amount,
                compare_side = excluded.compare_side,
                run_id = excluded.run_id,
                matched_internal_ids = excluded.matched_internal_ids,
                matched_internal_count = excluded.matched_internal_count,
                mismatch_json = excluded.mismatch_json,
                external_price = excluded.external_price,
                internal_price = excluded.internal_price,
                external_value_date = excluded.external_value_date,
                internal_value_date = excluded.internal_value_date,
                created_at = now()
            """,
            (
                settlement_trade_id,
                internal_order_id,
                match_status,
                match_note,
                reconciliation_key,
                counterparty,
                isin,
                direction,
                trade_date,
                value_date,
                external_qty,
                internal_qty,
                external_amount,
                internal_amount,
                compare_side,
                run_id,
                matched_internal_ids,
                matched_internal_count,
                mismatch_json,
                external_price,
                internal_price,
                external_value_date,
                internal_value_date,
            ),
        )
    conn.commit()


def _detail_row(
    st: Dict[str, Any],
    status: str,
    td: Optional[Dict[str, Any]] = None,
    agg_rows: Optional[List[Dict[str, Any]]] = None,
    agg_note: Optional[str] = None,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build a flat dict for the human-readable reconciliation report."""
    internal_qty = None
    internal_amount = None
    internal_price = None
    internal_value_date = None
    internal_ids = None
    counterparty = None

    if td is not None:
        internal_qty = td.get("qty")
        internal_amount = td.get("transaction_value")
        internal_price = td.get("price")
        internal_value_date = td.get("settle_date_cash") or td.get("value_date_cash")
        internal_ids = str(td["id"])
        counterparty = td.get("counterparty")
    elif agg_rows:
        internal_qty = sum(float(r.get("qty") or 0) for r in agg_rows)
        internal_amount = sum(float(r.get("transaction_value") or 0) for r in agg_rows)
        internal_value_date = agg_rows[0].get("settle_date_cash") or agg_rows[0].get("value_date_cash")
        internal_ids = ",".join(str(r["id"]) for r in agg_rows)
        counterparty = agg_rows[0].get("counterparty")

    return {
        "status": status,
        "isin": st.get("isin"),
        "security_name": st.get("security_name"),
        "side": st.get("side"),
        "trade_date": st.get("trade_date"),
        "value_date": st.get("value_date"),
        "broker": st.get("broker_name"),
        "source_file": st.get("source_file"),
        "counterparty": counterparty,
        # confo (external) fields
        "ext_qty": st.get("quantity"),
        "ext_price": st.get("price") or st.get("price_in_percentage"),
        "ext_amount": st.get("consideration"),
        "ext_value_date": st.get("value_date"),
        # internal (tab_deals) fields
        "int_qty": internal_qty,
        "int_price": internal_price,
        "int_amount": internal_amount,
        "int_value_date": internal_value_date,
        "int_ids": internal_ids,
        # mismatch details
        "notes": ", ".join(notes) if notes else (agg_note or ""),
    }


def _fmt_num(v) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):,.0f}"
    except Exception:
        return str(v)


def _fmt_date(v) -> str:
    if v is None:
        return ""
    return str(v)[:10]


def build_reconciliation_excel(result: dict, date_from, date_to) -> bytes:
    """Build an Excel workbook with reconciliation results. Returns bytes."""
    STATUS_COLOR = {
        "MATCHED":                            "C6EFCE",   # green
        "MATCHED_AGGREGATED":                 "C6EFCE",
        "PARTIAL":                            "FFEB9C",   # yellow
        "SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE": "F4B942",   # orange
        "NOT_FOUND":                          "FFC7CE",   # red
        "NO_CONFO":                           "FCE4D6",   # light orange
    }

    wb = Workbook()

    # ── Sheet 1: Confo vs Internal ─────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Confo vs Internal"

    hdr = ["Status", "ISIN", "Side", "Trade Date", "Value Date", "Broker",
           "Ext Qty", "Int Qty", "Ext Price", "Int Price", "Ext Amount", "Int Amount", "Notes"]
    ws1.append(hdr)
    for col_idx, _ in enumerate(hdr, 1):
        cell = ws1.cell(1, col_idx)
        cell.fill = PatternFill("solid", fgColor="2F5496")
        cell.font = Font(bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center")

    for row in result.get("detail_rows", []):
        status = row.get("status", "")
        color = STATUS_COLOR.get(status, "FFFFFF")
        data = [
            status,
            row.get("isin") or "",
            row.get("side") or "",
            _fmt_date(row.get("trade_date")),
            _fmt_date(row.get("value_date")),
            row.get("broker") or "",
            _fmt_num(row.get("ext_qty")),
            _fmt_num(row.get("int_qty")),
            _fmt_num(row.get("ext_price")),
            _fmt_num(row.get("int_price")),
            _fmt_num(row.get("ext_amount")),
            _fmt_num(row.get("int_amount")),
            row.get("notes") or "",
        ]
        ws1.append(data)
        fill = PatternFill("solid", fgColor=color)
        for col_idx in range(1, len(data) + 1):
            ws1.cell(ws1.max_row, col_idx).fill = fill

    # Auto-width
    for col_idx, _ in enumerate(hdr, 1):
        ws1.column_dimensions[get_column_letter(col_idx)].width = 16
    ws1.column_dimensions["B"].width = 14
    ws1.column_dimensions["F"].width = 22
    ws1.column_dimensions["K"].width = 30

    # ── Sheet 2: Internal without Confo ───────────────────────────────────────
    ws2 = wb.create_sheet("Internal - No Confo")
    hdr2 = ["ISIN", "Side", "Trade Date", "Value Date", "Counterparty", "Qty", "Amount"]
    ws2.append(hdr2)
    for col_idx, _ in enumerate(hdr2, 1):
        cell = ws2.cell(1, col_idx)
        cell.fill = PatternFill("solid", fgColor="843C0C")
        cell.font = Font(bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center")

    fill_red = PatternFill("solid", fgColor="FCE4D6")
    for td in result.get("unmatched_internal", []):
        data2 = [
            td.get("symbol") or "",
            td.get("direction") or "",
            _fmt_date(td.get("trade_date")),
            _fmt_date(td.get("settle_date_cash") or td.get("value_date_cash")),
            td.get("counterparty") or "",
            _fmt_num(td.get("qty")),
            _fmt_num(td.get("transaction_value")),
        ]
        ws2.append(data2)
        for col_idx in range(1, len(data2) + 1):
            ws2.cell(ws2.max_row, col_idx).fill = fill_red

    for col_idx, _ in enumerate(hdr2, 1):
        ws2.column_dimensions[get_column_letter(col_idx)].width = 18

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def build_reconciliation_html(result: dict, date_from, date_to) -> str:
    """Build HTML email body for reconciliation report."""
    matched = result.get("matched_count", 0) + result.get("matched_aggregated_count", 0)
    partial = result.get("partial_count", 0)
    similar = result.get("similar_found_count", 0)
    not_found = result.get("not_found_count", 0)
    no_confo = len(result.get("unmatched_internal", []))
    review_count = partial + similar + not_found + no_confo

    date_label = ""
    if date_from and date_to:
        date_label = f"{date_from} – {date_to}" if date_from != date_to else str(date_from)
    elif date_from or date_to:
        date_label = str(date_from or date_to)
    else:
        date_label = "all dates"

    STATUS_BG = {
        "MATCHED":                            "#C6EFCE",
        "MATCHED_AGGREGATED":                 "#C6EFCE",
        "PARTIAL":                            "#FFEB9C",
        "SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE": "#F4B942",
        "NOT_FOUND":                          "#FFC7CE",
    }
    STATUS_LABEL = {
        "MATCHED":                            "✅ MATCHED",
        "MATCHED_AGGREGATED":                 "✅ MATCHED~",
        "PARTIAL":                            "⚠️ PARTIAL",
        "SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE": "🟠 SIMILAR",
        "NOT_FOUND":                          "❌ NOT FOUND",
    }

    style = """
    body { font-family: Calibri, Arial, sans-serif; font-size: 13px; color: #222; }
    h2 { color: #2F5496; margin-bottom: 4px; }
    .summary { display: flex; gap: 12px; margin: 16px 0; }
    .badge { padding: 12px 20px; border-radius: 6px; font-size: 15px; font-weight: bold; min-width: 120px; text-align: center; }
    .green  { background: #C6EFCE; color: #276221; }
    .yellow { background: #FFEB9C; color: #7D6608; }
    .red    { background: #FFC7CE; color: #9C0006; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 24px; font-size: 12px; }
    th { background: #2F5496; color: white; padding: 6px 8px; text-align: center; }
    td { padding: 5px 8px; border: 1px solid #D0D0D0; white-space: nowrap; }
    .sect { color: #2F5496; font-size: 14px; font-weight: bold; margin: 20px 0 6px 0; border-bottom: 2px solid #2F5496; padding-bottom: 3px; }
    .mismatch { color: #C00000; font-weight: bold; }
    """

    def td_pair(ext_val, int_val, is_amount=False):
        """Render two cells; highlight if they differ significantly."""
        ext_s = _fmt_num(ext_val) if ext_val is not None else "&nbsp;"
        int_s = _fmt_num(int_val) if int_val is not None else "&nbsp;"
        threshold = 1.0 if is_amount else 0.01
        mismatch = (
            ext_val is not None and int_val is not None
            and abs(float(ext_val or 0) - float(int_val or 0)) > threshold
        )
        cls = ' class="mismatch"' if mismatch else ""
        return f"<td{cls}>{ext_s}</td><td{cls}>{int_s}</td>"

    # ── Summary badges ─────────────────────────────────────────────────────────
    html = f"""<html><head><style>{style}</style></head><body>
<h2>Settlement Reconciliation Report</h2>
<div style="color:#555; margin-bottom:8px;">Trade dates: <b>{date_label}</b></div>
<div class="summary">
  <div class="badge green">✅ Matched<br><span style="font-size:22px">{matched}</span></div>
  <div class="badge yellow">⚠️ Needs Review<br><span style="font-size:22px">{review_count}</span></div>
  <div class="badge red">❌ Not Found<br><span style="font-size:22px">{not_found + no_confo}</span></div>
</div>
<div style="font-size:12px; color:#555; margin-bottom:16px;">
  Exact match: {result.get('matched_count',0)} &nbsp;|&nbsp;
  Netting: {result.get('matched_aggregated_count',0)} &nbsp;|&nbsp;
  Partial (mismatch): {partial} &nbsp;|&nbsp;
  Similar (wrong date): {similar} &nbsp;|&nbsp;
  Not in BO: {not_found} &nbsp;|&nbsp;
  Internal no confo: {no_confo}
</div>
"""

    # ── Table A: Confo vs Internal ─────────────────────────────────────────────
    html += """<div class="sect">A. Confo vs Internal (what counterparties sent)</div>
<table>
<tr>
  <th>Status</th><th>ISIN</th><th>Side</th><th>Trade Date</th><th>Value Date</th>
  <th>Broker</th><th>Ext Qty</th><th>Int Qty</th><th>Ext Price</th><th>Int Price</th><th>Ext Amount</th><th>Int Amount</th><th>Notes</th>
</tr>
"""
    for row in result.get("detail_rows", []):
        status = row.get("status", "")
        bg = STATUS_BG.get(status, "#FFFFFF")
        label = STATUS_LABEL.get(status, status)
        html += f'<tr style="background:{bg}">'
        html += f"<td><b>{label}</b></td>"
        html += f"<td>{row.get('isin') or ''}</td>"
        html += f"<td>{row.get('side') or ''}</td>"
        html += f"<td>{_fmt_date(row.get('trade_date'))}</td>"
        html += f"<td>{_fmt_date(row.get('value_date'))}</td>"
        html += f"<td>{row.get('broker') or ''}</td>"
        html += td_pair(row.get("ext_qty"), row.get("int_qty"))
        html += td_pair(row.get("ext_price"), row.get("int_price"))
        html += td_pair(row.get("ext_amount"), row.get("int_amount"), is_amount=True)
        html += f"<td>{row.get('notes') or ''}</td>"
        html += "</tr>\n"
    html += "</table>\n"

    # ── Table B: Internal without Confo ───────────────────────────────────────
    unmatched = result.get("unmatched_internal", [])
    if unmatched:
        html += """<div class="sect">B. Internal Deals — No Confo Received</div>
<table>
<tr>
  <th>ISIN</th><th>Side</th><th>Trade Date</th><th>Value Date</th>
  <th>Counterparty</th><th>Qty</th><th>Amount</th>
</tr>
"""
        for td in unmatched:
            html += '<tr style="background:#FCE4D6">'
            html += f"<td>{td.get('symbol') or ''}</td>"
            html += f"<td>{td.get('direction') or ''}</td>"
            html += f"<td>{_fmt_date(td.get('trade_date'))}</td>"
            html += f"<td>{_fmt_date(td.get('settle_date_cash') or td.get('value_date_cash'))}</td>"
            html += f"<td>{td.get('counterparty') or ''}</td>"
            html += f"<td>{_fmt_num(td.get('qty'))}</td>"
            html += f"<td>{_fmt_num(td.get('transaction_value'))}</td>"
            html += "</tr>\n"
        html += "</table>\n"

    html += "<div style='color:#888; font-size:11px; margin-top:20px'>Generated by AM Wealth Settlement Agent</div>"
    html += "</body></html>"
    return html


def send_reconciliation_email(token: str, result: dict, date_from, date_to) -> None:
    """Send reconciliation report email with Excel attachment via Graph API."""
    matched = result.get("matched_count", 0) + result.get("matched_aggregated_count", 0)
    partial = result.get("partial_count", 0)
    not_found = result.get("not_found_count", 0)
    similar = result.get("similar_found_count", 0)
    no_confo = len(result.get("unmatched_internal", []))
    review_count = partial + not_found + similar + no_confo

    date_label = str(date_from or date_to or "")
    subject = (
        f"Settlement Reconciliation {date_label}"
        f" | ✅ {matched} matched"
        f" | ⚠️ {review_count} review"
        f" | ❌ {not_found + no_confo} missing"
    )

    html_body = build_reconciliation_html(result, date_from, date_to)

    # Build Excel attachment
    xlsx_bytes = build_reconciliation_excel(result, date_from, date_to)
    xlsx_b64 = base64.b64encode(xlsx_bytes).decode("utf-8")
    attach_name = f"reconciliation_{date_label or 'report'}.xlsx"

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": REPORT_TO}}],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": attach_name,
                    "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "contentBytes": xlsx_b64,
                }
            ],
        },
        "saveToSentItems": False,
    }

    url = f"{GRAPH_BASE}/users/{GRAPH_MAILBOX}/sendMail"
    r = requests.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    logging.info("Reconciliation email sent to %s", REPORT_TO)


def run_settlement_reconciliation(
    conn,
    run_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    deal_date_from: Optional[date] = None,
    deal_date_to: Optional[date] = None,
    value_date_from: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Runs settlement reconciliation.
    date_from / date_to       — confo window by trade_date (last 2 business days)
    deal_date_from / deal_date_to — internal deals window by trade_date (last 5 business days)
    value_date_from           — exclude confos already settled before this date
    Returns a summary dict plus:
      detail_rows          – list of per-confo comparison results
      unmatched_internal   – internal deals with no matching confo in the wider window
    """
    settlement_trades = load_settlement_trades_for_reconciliation(
        conn, date_from=date_from, date_to=date_to, value_date_from=value_date_from
    )
    strict_deals = load_strict_deals_to_process(
        conn,
        date_from=deal_date_from if deal_date_from is not None else date_from,
        date_to=deal_date_to if deal_date_to is not None else date_to,
    )
    broad_deals = load_broad_trade_search(conn)

    # Build set of (isin, side, trade_date) for ALL confos ever received
    # (no date filter) — used to exclude already-confirmed deals from Table B
    all_confo_trades = load_settlement_trades_for_reconciliation(conn)
    all_confo_keys = {
        (clean_text(st.get("isin")), clean_text(st.get("side")), st.get("trade_date"))
        for st in all_confo_trades
    }

    comparison_rows = 0
    matched_count = 0
    matched_aggregated_count = 0
    partial_count = 0
    not_found_count = 0
    similar_found_count = 0

    detail_rows: List[Dict[str, Any]] = []   # per-confo row for readable report
    matched_internal_ids: set = set()         # track which internal deals were matched

    if run_id is not None:
        clear_reconciliation_run_rows(conn, run_id)

    for st in settlement_trades:
        comparison_rows += 1
        strict_candidates = find_strict_candidates(st, strict_deals)

        td, match_status, best_notes = try_exact_single_match(st, strict_candidates)
        if td is not None and match_status == "MATCHED":
            matched_count += 1
            matched_internal_ids.add(td["id"])

            upsert_reconciliation_result(
                conn=conn,
                settlement_trade_id=st["id"],
                internal_order_id=td["id"],
                match_status="MATCHED",
                match_note=None,
                reconciliation_key=build_reconciliation_key(st),
                counterparty=td.get("counterparty"),
                isin=st.get("isin"),
                direction=st.get("side"),
                trade_date=st.get("trade_date"),
                value_date=st.get("value_date"),
                external_qty=st.get("quantity"),
                internal_qty=td.get("qty"),
                external_amount=st.get("consideration"),
                internal_amount=td.get("transaction_value"),
                compare_side=st.get("side"),
                run_id=run_id,
                matched_internal_ids=str(td["id"]),
                matched_internal_count=1,
                mismatch_json=None,
                external_price=st.get("price"),
                internal_price=td.get("price"),
                external_value_date=st.get("value_date"),
                internal_value_date=td.get("settle_date_cash") or td.get("value_date_cash"),
            )
            detail_rows.append(_detail_row(st, "MATCHED", td=td))
            continue

        rows, agg_status, agg_note = try_aggregate_match(st, strict_candidates)
        if rows is not None and agg_status is not None:
            matched_aggregated_count += 1
            internal_ids = ",".join([str(r["id"]) for r in rows])
            summed_qty = sum([float(r.get("qty") or 0) for r in rows])
            summed_amount = sum([float(r.get("transaction_value") or 0) for r in rows])
            for r in rows:
                matched_internal_ids.add(r["id"])

            upsert_reconciliation_result(
                conn=conn,
                settlement_trade_id=st["id"],
                internal_order_id=None,
                match_status=agg_status,
                match_note=agg_note,
                reconciliation_key=build_reconciliation_key(st),
                counterparty=rows[0].get("counterparty") if rows else None,
                isin=st.get("isin"),
                direction=st.get("side"),
                trade_date=st.get("trade_date"),
                value_date=st.get("value_date"),
                external_qty=st.get("quantity"),
                internal_qty=summed_qty,
                external_amount=st.get("consideration"),
                internal_amount=summed_amount,
                compare_side=st.get("side"),
                run_id=run_id,
                matched_internal_ids=internal_ids,
                matched_internal_count=len(rows),
                mismatch_json=None,
                external_price=st.get("price"),
                internal_price=None,
                external_value_date=st.get("value_date"),
                internal_value_date=rows[0].get("settle_date_cash") or rows[0].get("value_date_cash"),
            )
            detail_rows.append(_detail_row(st, agg_status, agg_rows=rows, agg_note=agg_note))
            continue

        if td is not None:
            partial_count += 1
            matched_internal_ids.add(td["id"])
            mismatch_json = json.dumps({
                "external_qty": str(st.get("quantity")),
                "internal_qty": str(td.get("qty")),
                "external_amount": str(st.get("consideration")),
                "internal_amount": str(td.get("transaction_value")),
                "external_price": str(st.get("price")),
                "internal_price": str(td.get("price")),
                "notes": best_notes or [],
            }, default=str)

            upsert_reconciliation_result(
                conn=conn,
                settlement_trade_id=st["id"],
                internal_order_id=td["id"],
                match_status="PARTIAL",
                match_note=", ".join(best_notes or []) if best_notes else "partial_match",
                reconciliation_key=build_reconciliation_key(st),
                counterparty=td.get("counterparty"),
                isin=st.get("isin"),
                direction=st.get("side"),
                trade_date=st.get("trade_date"),
                value_date=st.get("value_date"),
                external_qty=st.get("quantity"),
                internal_qty=td.get("qty"),
                external_amount=st.get("consideration"),
                internal_amount=td.get("transaction_value"),
                compare_side=st.get("side"),
                run_id=run_id,
                matched_internal_ids=str(td["id"]),
                matched_internal_count=1,
                mismatch_json=mismatch_json,
                external_price=st.get("price"),
                internal_price=td.get("price"),
                external_value_date=st.get("value_date"),
                internal_value_date=td.get("settle_date_cash") or td.get("value_date_cash"),
            )
            detail_rows.append(_detail_row(st, "PARTIAL", td=td, notes=best_notes))
            continue

        similar_rows = find_similar_broad_rows(st, broad_deals)
        if similar_rows:
            # Try to score the broad matches — if we find a candidate with same ISIN+side+date,
            # show as MATCHED or PARTIAL depending on score (covers settled internal deals).
            broad_strict_candidates = find_strict_candidates(st, similar_rows)
            broad_td, broad_status, broad_notes = try_exact_single_match(st, broad_strict_candidates)
            if broad_td is not None:
                matched_internal_ids.add(broad_td["id"])
                if broad_status == "MATCHED":
                    matched_count += 1
                    upsert_reconciliation_result(
                        conn=conn,
                        settlement_trade_id=st["id"],
                        internal_order_id=broad_td["id"],
                        match_status="MATCHED",
                        match_note=None,
                        reconciliation_key=build_reconciliation_key(st),
                        counterparty=broad_td.get("counterparty"),
                        isin=st.get("isin"),
                        direction=st.get("side"),
                        trade_date=st.get("trade_date"),
                        value_date=st.get("value_date"),
                        external_qty=st.get("quantity"),
                        internal_qty=broad_td.get("qty"),
                        external_amount=st.get("consideration"),
                        internal_amount=broad_td.get("transaction_value"),
                        compare_side=st.get("side"),
                        run_id=run_id,
                        matched_internal_ids=str(broad_td["id"]),
                        matched_internal_count=1,
                        mismatch_json=None,
                        external_price=st.get("price"),
                        internal_price=broad_td.get("price"),
                        external_value_date=st.get("value_date"),
                        internal_value_date=broad_td.get("settle_date_cash") or broad_td.get("value_date_cash"),
                    )
                    detail_rows.append(_detail_row(st, "MATCHED", td=broad_td))
                else:
                    # Score < 90 — show as PARTIAL with internal data
                    partial_count += 1
                    mismatch_json = json.dumps({
                        "external_qty": str(st.get("quantity")),
                        "internal_qty": str(broad_td.get("qty")),
                        "external_amount": str(st.get("consideration")),
                        "internal_amount": str(broad_td.get("transaction_value")),
                        "external_price": str(st.get("price")),
                        "internal_price": str(broad_td.get("price")),
                        "notes": (broad_notes or []) + ["internal deal outside strict date/status window"],
                    }, default=str)
                    upsert_reconciliation_result(
                        conn=conn,
                        settlement_trade_id=st["id"],
                        internal_order_id=broad_td["id"],
                        match_status="PARTIAL",
                        match_note=", ".join(broad_notes or []) + "; outside strict window",
                        reconciliation_key=build_reconciliation_key(st),
                        counterparty=broad_td.get("counterparty"),
                        isin=st.get("isin"),
                        direction=st.get("side"),
                        trade_date=st.get("trade_date"),
                        value_date=st.get("value_date"),
                        external_qty=st.get("quantity"),
                        internal_qty=broad_td.get("qty"),
                        external_amount=st.get("consideration"),
                        internal_amount=broad_td.get("transaction_value"),
                        compare_side=st.get("side"),
                        run_id=run_id,
                        matched_internal_ids=str(broad_td["id"]),
                        matched_internal_count=1,
                        mismatch_json=mismatch_json,
                        external_price=st.get("price"),
                        internal_price=broad_td.get("price"),
                        external_value_date=st.get("value_date"),
                        internal_value_date=broad_td.get("settle_date_cash") or broad_td.get("value_date_cash"),
                    )
                    detail_rows.append(_detail_row(st, "PARTIAL", td=broad_td, notes=broad_notes))
                continue

            similar_found_count += 1
            upsert_reconciliation_result(
                conn=conn,
                settlement_trade_id=st["id"],
                internal_order_id=None,
                match_status="SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE",
                match_note=f"Found {len(similar_rows)} similar rows for same ISIN outside strict match",
                reconciliation_key=build_reconciliation_key(st),
                counterparty=None,
                isin=st.get("isin"),
                direction=st.get("side"),
                trade_date=st.get("trade_date"),
                value_date=st.get("value_date"),
                external_qty=st.get("quantity"),
                internal_qty=None,
                external_amount=st.get("consideration"),
                internal_amount=None,
                compare_side=st.get("side"),
                run_id=run_id,
                matched_internal_ids=",".join([str(r["id"]) for r in similar_rows]),
                matched_internal_count=len(similar_rows),
                mismatch_json=json.dumps(similar_rows, default=str),
                external_price=st.get("price"),
                internal_price=None,
                external_value_date=st.get("value_date"),
                internal_value_date=None,
            )
            detail_rows.append(_detail_row(st, "SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE"))
            continue

        not_found_count += 1
        upsert_reconciliation_result(
            conn=conn,
            settlement_trade_id=st["id"],
            internal_order_id=None,
            match_status="NOT_FOUND",
            match_note="No candidate matched by isin + side + trade_date and no similar ISIN rows found",
            reconciliation_key=build_reconciliation_key(st),
            counterparty=None,
            isin=st.get("isin"),
            direction=st.get("side"),
            trade_date=st.get("trade_date"),
            value_date=st.get("value_date"),
            external_qty=st.get("quantity"),
            internal_qty=None,
            external_amount=st.get("consideration"),
            internal_amount=None,
            compare_side=st.get("side"),
            run_id=run_id,
            matched_internal_ids=None,
            matched_internal_count=None,
            mismatch_json=None,
            external_price=st.get("price"),
            internal_price=None,
            external_value_date=st.get("value_date"),
            internal_value_date=None,
        )
        detail_rows.append(_detail_row(st, "NOT_FOUND"))

    # Reverse check: internal deals that have no matching confo in this window.
    # Exclude deals whose confo exists (even if filtered by value_date) — already settled in prior run.
    unmatched_internal = [
        td for td in strict_deals
        if td["id"] not in matched_internal_ids
        and (clean_text(td.get("symbol")), clean_text(td.get("direction")), td.get("trade_date"))
            not in all_confo_keys
    ]

    return {
        "comparison_rows": comparison_rows,
        "matched_count": matched_count,
        "matched_aggregated_count": matched_aggregated_count,
        "partial_count": partial_count,
        "not_found_count": not_found_count,
        "similar_found_count": similar_found_count,
        "detail_rows": detail_rows,
        "unmatched_internal": unmatched_internal,
    }


@app.function_name(name="settlement_reconciliation_timer")
@app.schedule(schedule="0 */10 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def settlement_reconciliation_timer(mytimer: func.TimerRequest) -> None:
    logging.warning("### settlement_reconciliation_timer started ###")

    run_id = None
    conn = None

    try:
        conn = get_conn()
        run_id = start_agent_run(conn, "settlement_reconciliation_timer")

        t0_date, _t1_date, _t_next_date = get_t0_t1_dates()
        confo_from = n_prev_business_days(t0_date, 10)  # confo: last 10 business days (~2 weeks)
        result = run_settlement_reconciliation(
            conn,
            run_id=run_id,
            date_from=confo_from,     # confo window: last 10 business days
            date_to=t0_date,
            deal_date_from=None,      # internal deals: no date restriction (all active)
            deal_date_to=None,
            # no value_date_from: show all confos in window including already-settled
        )

        finish_agent_run(
            conn,
            run_id,
            "SUCCESS",
            (
                f"comparison_rows={result['comparison_rows']}, "
                f"matched_count={result['matched_count']}, "
                f"matched_aggregated_count={result['matched_aggregated_count']}, "
                f"partial_count={result['partial_count']}, "
                f"not_found_count={result['not_found_count']}, "
                f"similar_found_count={result['similar_found_count']}"
            ),
        )

        has_data = (
            result.get("comparison_rows", 0) > 0
            or len(result.get("unmatched_internal", [])) > 0
        )
        if has_data:
            try:
                token = get_graph_token()
                send_reconciliation_email(token, result, confo_from, t0_date)
            except Exception as email_err:
                logging.exception("Failed to send reconciliation email: %s", email_err)
        else:
            logging.info("settlement_reconciliation_timer: no trades in window, skipping email")

        logging.warning(
            "### settlement_reconciliation_timer finished | result=%s ###",
            result
        )

    except Exception as e:
        logging.exception("Settlement reconciliation failed: %s", e)

        try:
            if conn:
                conn.rollback()
        except Exception:
            logging.exception("Rollback failed")

        try:
            if conn and run_id:
                finish_agent_run(conn, run_id, "FAILED", str(e))
        except Exception:
            logging.exception("Failed to mark reconciliation run as FAILED")

        raise

    finally:
        if conn:
            conn.close()


# =============================================================================
# HTTP TRIGGER: reparse_recent
# POST /api/reparse-recent
# Body (JSON, all optional):
#   { "n": 20, "dry_run": false, "senders": ["bo.tdsm@zarattinibank.ch"] }
# =============================================================================
@app.function_name(name="reparse_recent")
@app.route(route="reparse-recent", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def reparse_recent_http(req: func.HttpRequest) -> func.HttpResponse:
    conn = None
    run_id = None
    try:
        body = {}
        try:
            body = req.get_json()
        except Exception:
            body = {}

        n = int(body.get("n") or req.params.get("n") or 3)
        dry_run_param = body.get("dry_run") if "dry_run" in body else req.params.get("dry_run")
        if isinstance(dry_run_param, bool):
            dry_run = dry_run_param
        else:
            dry_run = str(dry_run_param or "false").strip().lower() in {"true", "1", "yes", "y"}

        filter_senders_param = body.get("senders") or req.params.get("senders")
        if filter_senders_param:
            if isinstance(filter_senders_param, list):
                filter_senders = [s.strip().lower() for s in filter_senders_param if s.strip()]
            else:
                filter_senders = [s.strip().lower() for s in str(filter_senders_param).split(",") if s.strip()]
        else:
            filter_senders = None

        conn = get_conn()
        token = get_graph_token()
        mapping_by_sender = load_mapping(conn)

        # Build candidate email list
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    internet_message_id, message_id, sender,
                    subject, received_at, status, attachment_count
                from (
                    select *,
                        row_number() over (
                            partition by sender
                            order by received_at desc nulls last
                        ) as rn
                    from back_office_auto.settlement_emails
                    where status <> 'SKIPPED'
                ) ranked
                where rn <= %s
                order by sender, received_at desc
                """,
                (n,),
            )
            cols = [d[0] for d in cur.description]
            emails = [dict(zip(cols, row)) for row in cur.fetchall()]

        if filter_senders:
            emails = [e for e in emails if e["sender"].lower() in filter_senders]

        # Filter: skip no-attachment emails for PDF/Excel parsers
        filtered = []
        for e in emails:
            template = detect_template_from_mapping(e["sender"], mapping_by_sender)
            if template not in EMAIL_BODY_TEMPLATES and (e.get("attachment_count") or 0) == 0:
                continue
            filtered.append(e)

        if not filtered:
            return func.HttpResponse(
                json.dumps({"dry_run": dry_run, "n": n, "emails_found": 0, "message": "Nothing to reparse"}),
                mimetype="application/json", status_code=200,
            )

        results = {"ok": 0, "failed": 0, "skipped": 0, "trades": []}

        if not dry_run:
            run_id = start_agent_run(conn, "reparse_recent_http")

        for e in filtered:
            imid = e["internet_message_id"]
            message_id = e["message_id"]
            sender = e["sender"]

            if dry_run:
                results["ok"] += 1
                results["trades"].append({
                    "sender": sender,
                    "internet_message_id": imid,
                    "subject": e.get("subject"),
                    "dry_run": True,
                })
                continue

            # 1. Delete from DB
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "delete from back_office_auto.settlement_reconciliation"
                        " where settlement_trade_id in ("
                        "  select id from back_office_auto.settlement_trades"
                        "  where internet_message_id = %s)", (imid,))
                    cur.execute(
                        "delete from back_office_auto.settlement_trades where internet_message_id = %s", (imid,))
                    cur.execute(
                        "delete from back_office_auto.settlement_files where internet_message_id = %s", (imid,))
                    cur.execute(
                        "delete from back_office_auto.settlement_emails where internet_message_id = %s", (imid,))
                conn.commit()
            except Exception as ex:
                logging.error("reparse_recent: delete failed for %s: %s", imid, ex)
                conn.rollback()
                results["failed"] += 1
                continue

            # 2. Fetch message stub from Graph
            try:
                url = (
                    f"{GRAPH_BASE}/users/{GRAPH_MAILBOX}/messages/{message_id}"
                    f"?$select=id,internetMessageId,subject,receivedDateTime,from,hasAttachments"
                )
                r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
                r.raise_for_status()
                msg_stub = r.json()
            except Exception as ex:
                logging.error("reparse_recent: graph fetch failed for %s: %s", imid, ex)
                results["failed"] += 1
                continue

            # 3. Reparse
            try:
                status, count = process_message(
                    conn=conn, token=token, mailbox=GRAPH_MAILBOX,
                    msg=msg_stub, mapping_by_sender=mapping_by_sender,
                    processing_run_id=run_id,
                )
                results["trades"].append({"sender": sender, "internet_message_id": imid, "status": status, "trades": count})
                if status in ("PARSED", "NO_TRADES_FOUND"):
                    results["ok"] += 1
                else:
                    results["skipped"] += 1
            except Exception as ex:
                logging.error("reparse_recent: parse failed for %s: %s", imid, ex)
                conn.rollback()
                results["failed"] += 1

        if not dry_run and run_id:
            finish_agent_run(
                conn, run_id,
                "SUCCESS" if results["failed"] == 0 else "PARTIAL",
                f"ok={results['ok']} skipped={results['skipped']} failed={results['failed']}",
            )

        return func.HttpResponse(
            json.dumps({
                "dry_run": dry_run,
                "n": n,
                "emails_found": len(filtered),
                "ok": results["ok"],
                "skipped": results["skipped"],
                "failed": results["failed"],
                "trades": results["trades"],
            }, default=str),
            mimetype="application/json", status_code=200,
        )

    except Exception as ex:
        logging.exception("reparse_recent_http failed: %s", ex)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
            if run_id:
                try:
                    finish_agent_run(conn, run_id, "FAILED", str(ex))
                except Exception:
                    pass
        return func.HttpResponse(
            json.dumps({"error": str(ex)}),
            mimetype="application/json", status_code=500,
        )
    finally:
        if conn:
            conn.close()


# =============================================================================
# HTTP TRIGGER: run_reconciliation
# POST /api/run-reconciliation
# =============================================================================
@app.function_name(name="run_reconciliation")
@app.route(route="run-reconciliation", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def run_reconciliation_http(req: func.HttpRequest) -> func.HttpResponse:
    conn = None
    run_id = None
    try:
        conn = get_conn()
        run_id = start_agent_run(conn, "run_reconciliation_http")
        t0_date, _t1_date, _t_next_date = get_t0_t1_dates()
        confo_from = n_prev_business_days(t0_date, 2)
        deal_from  = n_prev_business_days(t0_date, 5)
        result = run_settlement_reconciliation(
            conn, run_id=run_id,
            date_from=confo_from, date_to=t0_date,
            deal_date_from=deal_from, deal_date_to=t0_date,
        )
        finish_agent_run(conn, run_id, "SUCCESS",
            f"comparison_rows={result['comparison_rows']}, matched={result['matched_count']}")
        has_data = (result.get("comparison_rows", 0) > 0
                    or len(result.get("unmatched_internal", [])) > 0)
        if has_data:
            token = get_graph_token()
            send_reconciliation_email(token, result, confo_from, t0_date)
        return func.HttpResponse(
            json.dumps({"ok": True, "comparison_rows": result["comparison_rows"],
                        "matched": result["matched_count"], "run_id": run_id}),
            mimetype="application/json", status_code=200,
        )
    except Exception as ex:
        logging.exception("run_reconciliation_http failed: %s", ex)
        if conn:
            try: conn.rollback()
            except Exception: pass
        if conn and run_id:
            try: finish_agent_run(conn, run_id, "FAILED", str(ex))
            except Exception: pass
        return func.HttpResponse(
            json.dumps({"error": str(ex)}),
            mimetype="application/json", status_code=500,
        )
    finally:
        if conn:
            conn.close()


# =============================================================================
# HTTP TRIGGER: run_email_parser
# POST /api/run-email-parser
# =============================================================================
@app.function_name(name="run_email_parser")
@app.route(route="run-email-parser", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def run_email_parser_http(req: func.HttpRequest) -> func.HttpResponse:
    conn = None
    run_id = None
    try:
        token = get_graph_token()
        conn = get_conn()
        run_id = start_agent_run(conn, "run_email_parser_http")
        mapping_by_sender = load_mapping(conn)
        allowed_senders = get_allowed_senders(mapping_by_sender)
        since_dt = now_utc() - timedelta(hours=LOOKBACK_HOURS)
        messages = list_recent_messages(token, GRAPH_MAILBOX, since_dt)
        total = parsed_messages = parsed_trades = skipped = 0
        for msg in messages:
            total += 1
            sender = (msg.get("from", {}).get("emailAddress", {}).get("address") or "").lower()
            if sender not in allowed_senders:
                skipped += 1
                continue
            attachments = get_message_attachments(token, GRAPH_MAILBOX, msg["id"])
            result = process_message(conn, msg, attachments, mapping_by_sender, dry_run=False)
            if result.get("parsed"):
                parsed_messages += 1
                parsed_trades += result.get("trades_saved", 0)
        finish_agent_run(conn, run_id, "SUCCESS",
            f"total={total}, parsed={parsed_messages}, trades={parsed_trades}, skipped={skipped}")
        return func.HttpResponse(
            json.dumps({"ok": True, "total": total, "parsed_messages": parsed_messages,
                        "parsed_trades": parsed_trades, "skipped": skipped, "run_id": run_id}),
            mimetype="application/json", status_code=200,
        )
    except Exception as ex:
        logging.exception("run_email_parser_http failed: %s", ex)
        if conn:
            try: conn.rollback()
            except Exception: pass
        if conn and run_id:
            try: finish_agent_run(conn, run_id, "FAILED", str(ex))
            except Exception: pass
        return func.HttpResponse(
            json.dumps({"error": str(ex)}),
            mimetype="application/json", status_code=500,
        )
    finally:
        if conn:
            conn.close()