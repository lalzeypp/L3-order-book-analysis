#!/usr/bin/env python3
"""
Shared constants, helpers, and view setup for scripts.

Imported by both order_flow_overview.py and order_lifecycle.py.
"""

import duckdb
import pandas as pd

# ChangeReason 
NEW_REASON       = 6   # New 
TRADE_REASON     = 3   # Order matched/executed
NORMAL_ORDER_CAT = 1   # OrderCategory = Order (not quote, not trade report)

CANCEL_REASONS = (
    1,   # CanceledByUser
    9,   # CanceledBySystem
    10,  # CanceledOnBehalf
    13,  # IcebergRefresh
    15,  # CanceledBySystemLimitChange
    19,  # Expired
    20,  # CanceledDueToISS
    34,  # CanceledAfterAuction
    41,  # QuoteCanceledDeltaMmProtection
    42,  # QuoteCanceledAbsMmProtection
    43,  # CrossingOrderDeleted
    115, 116, 117, 118, 119, 120, 121, 122, 123, 124,  # CanceledByPtrm*
)
CANCEL_REASONS_SQL = ",".join(map(str, CANCEL_REASONS))

# OrderType (EMIR FIYAT TURU)
LIMIT_ORDER_TYPE = 1

# ExchangeOrderType (EMIR TURU) – bitmask, bit 5 = Undisclosed (iceberg)
ICEBERG_BIT = 32

# Session classification 
OPENING_SESSION_PATTERNS = ["ACILIS", "ACS_EMR"]
CLOSING_SESSION_PATTERNS = ["KAPANIS", "ESLESTIRMETEKFIYAT"]
CONTINUOUS_START_HOUR    = 10
CONTINUOUS_END_HOUR      = 18

# matplotlib style (apply v plt.rcParams.update(CHART_RCPARAMS)) 
CHART_RCPARAMS = {
    "figure.facecolor":  "white",
    "axes.facecolor":    "white",
    "axes.grid":         True,
    "grid.alpha":        0.3,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "font.size":         10,
}


# helpers 
def session_case(time_col: str) -> str:
    """
    Return a SQL CASE expression classifying a timestamp column into:
      'A_OPENING'    – opening auction
      'Z_CLOSING'    – closing auction
      'HH:MM-HH:MM'  – 30-min bucket during continuous trading
    """
    opening_filter = " OR ".join(
        f"\"SEANS\" LIKE '%{p}%'" for p in OPENING_SESSION_PATTERNS
    )
    closing_filter = " OR ".join(
        f"\"SEANS\" LIKE '%{p}%'" for p in CLOSING_SESSION_PATTERNS
    )
    return f"""
        CASE
            WHEN ({opening_filter}) OR EXTRACT(HOUR FROM {time_col}) < {CONTINUOUS_START_HOUR}
                THEN 'A_OPENING'
            WHEN ({closing_filter}) OR EXTRACT(HOUR FROM {time_col}) >= {CONTINUOUS_END_HOUR}
                THEN 'Z_CLOSING'
            ELSE
                LPAD(CAST(EXTRACT(HOUR FROM {time_col}) AS VARCHAR), 2, '0') || ':' ||
                CASE WHEN EXTRACT(MINUTE FROM {time_col}) < 30 THEN '00' ELSE '30' END
                || '-' ||
                CASE
                    WHEN EXTRACT(MINUTE FROM {time_col}) < 30
                        THEN LPAD(CAST(EXTRACT(HOUR FROM {time_col}) AS VARCHAR), 2, '0') || ':30'
                    ELSE LPAD(CAST((EXTRACT(HOUR FROM {time_col}) + 1) AS VARCHAR), 2, '0') || ':00'
                END
        END
    """


def broad_session_case(time_col: str) -> str:
    """Return a SQL CASE classifying a timestamp into '1_Opening', '2_Midday', '3_Closing'."""
    opening_filter = " OR ".join(
        f"\"SEANS\" LIKE '%{p}%'" for p in OPENING_SESSION_PATTERNS
    )
    closing_filter = " OR ".join(
        f"\"SEANS\" LIKE '%{p}%'" for p in CLOSING_SESSION_PATTERNS
    )
    return f"""
        CASE
            WHEN ({opening_filter}) OR EXTRACT(HOUR FROM {time_col}) < {CONTINUOUS_START_HOUR}
                THEN '1_Opening'
            WHEN ({closing_filter}) OR EXTRACT(HOUR FROM {time_col}) >= {CONTINUOUS_END_HOUR}
                THEN '3_Closing'
            ELSE '2_Midday'
        END
    """


def create_orders_view(con: duckdb.DuckDBPyConnection, csv_path: str) -> None:
    """
    Create the 'orders' DuckDB VIEW over the raw CSV.

    Column reference:
      entry_ts        - EMIR GIRIS TARIHI     (order entry timestamp)
      change_ts       - EMIR DEGISTIRILME TARIHI (event timestamp)
      qty             - EMIR MIKTARI          (order size in lots)
      remaining       - KALAN MIKTAR          (remaining qty after this event)
      visible_qty     - GORUNEN MIKTAR        (disclosed / visible qty for icebergs)
      price           - FIYAT                 (price in TL)
      change_reason   - EMIR DEGISIKLIK SEBEBI (event type code)
      order_type      - EMIR FIYAT TURU       (1=Limit, 2=Market, …)
      exch_order_type - EMIR TURU           (bitmask; bit 5 = Undisclosed / iceberg)
      order_cat       - EMIR KATEGORISI       (1=Order, 4=Quote, 32=TradeReport)
      order_tl        - qty * price           (notional value at submission)
      "EMIR NO"       - order identifier (constant across all events of the same order)
      "ISLEM KODU"    - stock ticker
      "ALIS_SATIS"    - A=buy / S=sell
      "SEANS"         - session name string
    """
    con.execute(f"""
        CREATE VIEW orders AS
        SELECT
            *,
            TRY_CAST("EMIR GIRIS TARIHI"        AS TIMESTAMP) AS entry_ts,
            TRY_CAST("EMIR DEGISTIRILME TARIHI"  AS TIMESTAMP) AS change_ts,
            TRY_CAST("EMIR MIKTARI"              AS DOUBLE)    AS qty,
            TRY_CAST("KALAN MIKTAR"              AS DOUBLE)    AS remaining,
            TRY_CAST("GORUNEN MIKTAR"            AS DOUBLE)    AS visible_qty,
            TRY_CAST("FIYAT"                     AS DOUBLE)    AS price,
            TRY_CAST("EMIR DEGISIKLIK SEBEBI"    AS INTEGER)   AS change_reason,
            TRY_CAST("EMIR FIYAT TURU"           AS INTEGER)   AS order_type,
            TRY_CAST("EMIR TURU"                 AS INTEGER)   AS exch_order_type,
            TRY_CAST("EMIR KATEGORISI"           AS INTEGER)   AS order_cat,
            TRY_CAST("EMIR MIKTARI"              AS DOUBLE)
                * TRY_CAST("FIYAT"               AS DOUBLE)    AS order_tl
        FROM read_csv(
            '{csv_path}',
            delim=';',
            header=true,
            ignore_errors=true,
            parallel=true
        )
    """)

def print_section(title: str) -> None:
    print(f"\n{'='*64}")
    print(f"  {title}")
    print(f"{'='*64}")


def pct(num, denom):
    return 100.0 * num / denom if denom else 0.0
