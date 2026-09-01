"""
Test for parse_tradeweb_pdf (TRADEWEB_PDF).
Run: python test_tradeweb_parse.py

Texts below are real pdfplumber extractions from the three sample
confirmations received 2026-09-01 (TW trade IDs 11252, 12152, 12154).
"""
import sys
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

# Mock heavy dependencies before importing function_app
for _mod in [
    "azure", "azure.functions",
    "psycopg2", "psycopg2.extras",
    "requests", "pdfplumber", "pandas",
    "openpyxl", "openpyxl.styles", "openpyxl.utils",
]:
    sys.modules[_mod] = MagicMock()

from function_app import parse_tradeweb_pdf, SENDER_DOMAIN_FALLBACK  # noqa: E402

_RECEIVED = datetime(2026, 9, 1, 14, 23)

_T12154 = """Trade summary
Side Quantity Security Customer / TW user ID Company Trade date / time
BUY 70,000 ROMANI 6.375 30/01/34 Achraf Douggui AM Wealth 01/09/26 05:27:53 EDT
adouggui
Trade detail
ISIN XS2756521303 Yield 6.461%
Trade type Outright MMY 8.761%
Quote type Price Settle date 03/09/26
Execution type Electronic Z spread 209.30
TW composite 99.761 I spread 214.80
Trader tweucrblast ASW spread 208.30
TW user ID - Benchmark description T 4 5/8 15/08/36 10yr
Dealer TWE Benchmark price 98.750
Trade date 01/09/26 Benchmark yield 4.784%
Trade time 05:27:53 EDT Corp principal USD $69,643.00
TW trade ID 12154 Corp accrued USD $409.06
Spread 167.7 Corp total USD $70,052.06
Traded price USD $99.49 Trade Best Yes
DM - BestX 30.80
Competing quotes
Dealer Trader Quote State
CP1 99.49 Accepted
CP6 99.534 Dlr-Quote
Page 1 of 1"""

_T12152 = """Trade summary
Side Quantity Security Customer / TW user ID Company Trade date / time
BUY 200,000 ADGB 5.500 30/04/54 Achraf Douggui AM Wealth 01/09/26 05:22:16 EDT
adouggui
Trade detail
ISIN XS2811094213 Yield 6.012%
Trade type Outright MMY -
Quote type Price Settle date 03/09/26
Execution type Electronic Z spread 138.40
TW composite 93.183 I spread 141.0
Trader tweucrblast ASW spread 130.10
TW user ID - Benchmark description T 5 15/05/56
Dealer TWE Benchmark price 95.891
Trade date 01/09/26 Benchmark yield 5.275%
Trade time 05:22:16 EDT Corp principal USD $186,270.00
TW trade ID 12152 Corp accrued USD $3,758.33
Spread 73.7 Corp total USD $190,028.33
Traded price USD $93.135 Trade Best Yes
DM - BestX 52.0
Competing quotes
Dealer Trader Quote State
CP8 93.135 Accepted
Page 1 of 1"""

_T11252 = """Trade summary
Side Quantity Security Customer / TW user ID Company Trade date / time
BUY 71,000 DT 9.250 01/06/32 Achraf Douggui AM Wealth 01/09/26 07:42:12 EDT
adouggui
Trade detail
ISIN US25156PAD50 Yield 5.111%
Trade type Outright MMY 6.029%
Quote type Price Settle date 02/09/26
Execution type Electronic Z spread 82.10
TW composite 120.181 I spread 83.40
Trader tweucrblast ASW spread 91.80
TW user ID - Benchmark description T 4 3/8 31/08/31 5yr
Dealer TWE Benchmark price 99.352
Trade date 01/09/26 Benchmark yield 4.521%
Trade time 07:42:12 EDT Corp principal USD $85,467.67
TW trade ID 11252 Corp accrued USD $1,660.12
Spread 59 Corp total USD $87,127.79
Traded price USD $120.377 Trade Best Yes
DM - BestX 74.550
Competing quotes
Dealer Trader Quote State
CP1 120.377 Accepted
CP2 120.482 Dlr-Quote
Page 1 of 1"""

# Synthetic SELL (same layout, side flipped) — no real sample yet
_T_SELL = _T12154.replace("BUY 70,000", "SELL 70,000")


def _parse(text):
    trades = parse_tradeweb_pdf(
        text=text,
        internet_message_id="<test@tradeweb>",
        source_file="test.pdf",
        email_received_at=_RECEIVED,
        processing_run_id=None,
        file_id=None,
        email_id=None,
        broker_name="Tradeweb",
    )
    assert len(trades) == 1, f"expected 1 trade, got {len(trades)}"
    return trades[0]


def test_12154():
    t = _parse(_T12154)
    assert t["side"] == "BUY", t["side"]
    assert t["isin"] == "XS2756521303", t["isin"]
    assert t["security_name"] == "ROMANI 6.375 30/01/34", t["security_name"]
    assert t["nominal"] == Decimal("70000"), t["nominal"]
    assert t["price_in_percentage"] == Decimal("99.49"), t["price_in_percentage"]
    assert str(t["trade_date"]) == "2026-09-01", t["trade_date"]
    assert str(t["value_date"]) == "2026-09-03", t["value_date"]
    assert t["consideration"] == Decimal("69643.00"), t["consideration"]
    assert t["accrued_interest"] == Decimal("409.06"), t["accrued_interest"]
    assert t["net_amount"] == Decimal("70052.06"), t["net_amount"]
    assert t["counterparty_reference"] == "12154", t["counterparty_reference"]
    assert t["settlement_currency"] == "USD"
    assert t["parser_template"] == "TRADEWEB_PDF"
    assert t["validation_status"] == "PARSED", (t["validation_status"], t["validation_note"])


def test_12152():
    t = _parse(_T12152)
    assert t["side"] == "BUY"
    assert t["isin"] == "XS2811094213"
    assert t["security_name"] == "ADGB 5.500 30/04/54", t["security_name"]
    assert t["nominal"] == Decimal("200000")
    assert t["price_in_percentage"] == Decimal("93.135")
    assert t["consideration"] == Decimal("186270.00")
    assert t["accrued_interest"] == Decimal("3758.33")
    assert t["net_amount"] == Decimal("190028.33")
    assert t["counterparty_reference"] == "12152"
    assert t["validation_status"] == "PARSED", (t["validation_status"], t["validation_note"])


def test_11252():
    t = _parse(_T11252)
    assert t["side"] == "BUY"
    assert t["isin"] == "US25156PAD50"
    assert t["security_name"] == "DT 9.250 01/06/32", t["security_name"]
    assert t["nominal"] == Decimal("71000")
    assert t["price_in_percentage"] == Decimal("120.377")
    assert str(t["value_date"]) == "2026-09-02", t["value_date"]
    assert t["net_amount"] == Decimal("87127.79")
    assert t["counterparty_reference"] == "11252"
    assert t["validation_status"] == "PARSED", (t["validation_status"], t["validation_note"])


def test_sell():
    t = _parse(_T_SELL)
    assert t["side"] == "SELL", t["side"]
    assert t["validation_status"] == "PARSED", (t["validation_status"], t["validation_note"])


def test_domain_fallback():
    assert SENDER_DOMAIN_FALLBACK["eusers.tradeweb.com"]["template_code"] == "TRADEWEB_PDF"
    assert SENDER_DOMAIN_FALLBACK["tradeweb.com"]["template_code"] == "TRADEWEB_PDF"


if __name__ == "__main__":
    test_12154()
    test_12152()
    test_11252()
    test_sell()
    test_domain_fallback()
    print("ALL TRADEWEB TESTS PASSED")
