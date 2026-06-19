#!/usr/bin/env python3
"""
Borsa Istanbul - Equity Market Full Order Book analysis with 16 research questions.

Delimiter: semicolon (;), encoding: UTF-8

Key ChangeReason codes:
  6  = New (initial order entry)
  3  = Trade (order executed)
  1  = CanceledByUser
  9  = CanceledBySystem
  5  = ReplacedByUser

Usage:
  python order_book_analysis.py /path/to/TED_YYYYMMDD.csv
  python order_book_analysis.py /path/to/TED_YYYYMMDD.csv --output results.xlsx
"""

import sys
import argparse
import duckdb
import pandas as pd
from pathlib import Path

# CONSTANTS

NEW_REASON = 6          # New order (first entry)
TRADE_REASON = 3        # Order matched/executed
NORMAL_ORDER_CAT = 1    # OrderCategory = Order (not quote, not trade report)

# All ChangeReason codes that represent a cancellation
CANCEL_REASONS = (
    1,   # CanceledByUser
    9,   # CanceledBySystem
    10,  # CanceledOnBehalf
    13,  # IcebergRefresh
    15,  # CanceledBySystemLimitChange
    20,  # CanceledDueToISS
    34,  # CanceledAfterAuction
    41,  # QuoteCanceledDeltaMmProtection
    42,  # QuoteCanceledAbsMmProtection
    43,  # CrossingOrderDeleted
    115, 116, 117, 118, 119, 120, 121, 122, 123, 124,  # CanceledByPtrm*
)
CANCEL_REASONS_SQL = ",".join(map(str, CANCEL_REASONS))

# OrderType (EMIR FIYAT TURU)
LIMIT_ORDER_TYPE = 1   # Limit

# ExchangeOrderType (EMIR TURU) - bitmask, bit 5 = Undisclosed (iceberg)
ICEBERG_BIT = 32  # Undisclosed Quantity

# Session classification by SEANS name fragment
OPENING_SESSION_PATTERNS = ["ACILIS", "ACS_EMR"]
CLOSING_SESSION_PATTERNS = ["KAPANIS", "ESLESTIRMETEKFIYAT"]

# Continuous trading hours at BIST (approx)
CONTINUOUS_START_HOUR = 10
CONTINUOUS_END_HOUR = 18


# DuckDB

def session_case(time_col: str) -> str:
    """
    SQL CASE that classifies a timestamp into a display label:
      - 'A_OPENING'  for opening session
      - 'Z_CLOSING'  for closing session
      - 'HH:MM-HH:MM' 30-min bucket for continuous trading
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
    """Three-way classification: Opening / Midday / Closing."""
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


def print_section(title: str) -> None:
    print(f"\n{'='*64}")
    print(f"  {title}")
    print(f"{'='*64}")

def pct(num, denom):
    return 100.0 * num / denom if denom else 0.0


# MAIN 

def run_analysis(csv_path: str, output_path=None) -> None:
    con = duckdb.connect()
    results: dict[str, pd.DataFrame] = {}

    print(f"\nLoading CSV via DuckDB: {csv_path}")
    print("(Large file — DuckDB streams it; no full RAM load needed)\n")

    # ── Create view ──────────────────────────────────────────────────────────
    # Column names contain spaces; we reference them with double-quotes in SQL.
    # EMIR GIRIS TARIHI  = order entry timestamp
    # EMIR DEGISTIRILME TARIHI = order change/cancel timestamp
    # EMIR MIKTARI  = order quantity (lots)
    # KALAN MIKTAR  = remaining quantity after this event
    # FIYAT         = price (TL)
    # EMIR DEGISIKLIK SEBEBI = ChangeReason code
    # EMIR FIYAT TURU        = OrderType (1=Limit, 2=Market …)
    # EMIR TURU              = ExchangeOrderType (bitmask)
    # EMIR KATEGORISI        = 1=Order, 4=Quote, 32=TradeReport
    # SEANS                  = Session name string

    con.execute(f"""
        CREATE VIEW orders AS
        SELECT
            *,
            TRY_CAST("EMIR GIRIS TARIHI"      AS TIMESTAMP) AS entry_ts,
            TRY_CAST("EMIR DEGISTIRILME TARIHI" AS TIMESTAMP) AS change_ts,
            TRY_CAST("EMIR MIKTARI"            AS DOUBLE)  AS qty,
            TRY_CAST("KALAN MIKTAR"            AS DOUBLE)  AS remaining,
            TRY_CAST("GORUNEN MIKTAR"          AS DOUBLE)  AS visible_qty,
            TRY_CAST("FIYAT"                   AS DOUBLE)  AS price,
            TRY_CAST("EMIR DEGISIKLIK SEBEBI"  AS INTEGER) AS change_reason,
            TRY_CAST("EMIR FIYAT TURU"         AS INTEGER) AS order_type,
            TRY_CAST("EMIR TURU"               AS INTEGER) AS exch_order_type,
            TRY_CAST("EMIR KATEGORISI"         AS INTEGER) AS order_cat,
            TRY_CAST("EMIR MIKTARI"            AS DOUBLE)
                * TRY_CAST("FIYAT" AS DOUBLE)              AS order_tl
        FROM read_csv(
            '{csv_path}',
            delim=';',
            header=true,
            ignore_errors=true,
            parallel=true
        )
    """)

    base_filter = f"order_cat = {NORMAL_ORDER_CAT}"

    # ════════════════════════════════════════════════════════════════════════
    # Q1 — Total new orders 
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q1 · Total new orders submitted")
    df = con.execute(f"""
        SELECT
            COUNT(*)           AS new_order_count,
            SUM(qty)           AS total_lots,
            SUM(order_tl)      AS total_tl
        FROM orders
        WHERE change_reason = {NEW_REASON}
          AND {base_filter}""").fetchdf()
    print(df.to_string(index=False))
    results["Q1_new_orders"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q2 — Buy vs Sell among new orders (count + volume)
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q2 · Buy vs. Sell ratio among new orders")
    q2_totals = con.execute(f"""
        SELECT COUNT(*) AS n, SUM(qty) AS lots, SUM(order_tl) AS tl
        FROM orders WHERE change_reason={NEW_REASON} AND {base_filter}
    """).fetchone()
    df = con.execute(f"""
        SELECT
            "ALIS_SATIS"               AS side,
            COUNT(*)                   AS count,
            100.0*COUNT(*)/{q2_totals[0]}  AS pct_count,
            SUM(qty)                   AS total_lots,
            100.0*SUM(qty)/{q2_totals[1]}  AS pct_lots,
            SUM(order_tl)              AS total_tl,
            100.0*SUM(order_tl)/{q2_totals[2]} AS pct_tl
        FROM orders
        WHERE change_reason = {NEW_REASON} AND {base_filter}
        GROUP BY "ALIS_SATIS"
        ORDER BY "ALIS_SATIS"
    """).fetchdf()
    print(df.to_string(index=False))
    results["Q2_buy_sell"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q3 — Top 10 stocks share of total order volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q3 · Top 10 stocks share of total new-order volume")
    df = con.execute(f"""
        WITH sv AS (
            SELECT
                "ISLEM KODU"   AS stock,
                SUM(qty)        AS lots,
                SUM(order_tl)   AS tl
            FROM orders
            WHERE change_reason = {NEW_REASON} AND {base_filter}
            GROUP BY "ISLEM KODU"
        ),
        tot AS (SELECT SUM(lots) AS gtl, SUM(tl) AS gtl_tl FROM sv)
        SELECT
            s.stock,
            s.lots,
            100.0*s.lots/t.gtl   AS pct_lots,
            s.tl,
            100.0*s.tl/t.gtl_tl AS pct_tl
        FROM sv s, tot t
        ORDER BY s.tl DESC
        LIMIT 10
    """).fetchdf()
    top10_pct_lots = df["pct_lots"].sum()
    top10_pct_tl   = df["pct_tl"].sum()
    print(df.to_string(index=False))
    print(f"\n  → Top 10 stocks represent {top10_pct_lots:.1f}% of lot volume, "
          f"{top10_pct_tl:.1f}% of TL volume")
    results["Q3_top10_stocks"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q4 — 30-min windows: most new orders (opening/closing treated separately)
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q4 · New orders per 30-min window (opening/closing separate)")
    sc = session_case("entry_ts")
    df = con.execute(f"""
        WITH classified AS (
            SELECT {sc} AS time_window, qty, order_tl
            FROM orders
            WHERE change_reason = {NEW_REASON} AND {base_filter}
        ),
        totals AS (
            SELECT COUNT(*) AS n, SUM(qty) AS lots, SUM(order_tl) AS tl
            FROM classified
        )
        SELECT
            c.time_window,
            COUNT(*)                AS order_count,
            100.0*COUNT(*)/t.n      AS pct_count,
            SUM(c.qty)              AS total_lots,
            SUM(c.order_tl)         AS total_tl,
            100.0*SUM(c.order_tl)/t.tl AS pct_tl
        FROM classified c, totals t
        GROUP BY c.time_window, t.n, t.lots, t.tl
        ORDER BY c.time_window
    """).fetchdf()
    peak_count = df.loc[df["order_count"].idxmax()]
    peak_tl    = df.loc[df["total_tl"].idxmax()]
    print(df.to_string(index=False))
    print(f"\n  → Peak by order count : {peak_count['time_window']} "
          f"({int(peak_count['order_count']):,} orders, {peak_count['pct_count']:.1f}%)")
    print(f"  → Peak by TL volume   : {peak_tl['time_window']} "
          f"({peak_tl['pct_tl']:.1f}%)")
    results["Q4_windows_new_orders"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q5 — Limit and iceberg ratio among new orders
    # Iceberg identification: any EMIR NO that generated a change_reason=13
    # (IcebergRefresh) event is definitively an iceberg order. The EMIR TURU
    # bitmask is unreliable at submission time (change_reason=6 rows).
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q5 · Limit and iceberg order ratio among new orders")
    df = con.execute(f"""
        WITH iceberg_ids AS (
            SELECT DISTINCT "EMIR NO" AS ono
            FROM orders
            WHERE change_reason = 13 AND {base_filter}
        ),
        new_orders AS (
            SELECT
                o.qty,
                o.order_type,
                CASE WHEN i.ono IS NOT NULL THEN 1 ELSE 0 END AS is_iceberg
            FROM orders o
            LEFT JOIN iceberg_ids i ON o."EMIR NO" = i.ono
            WHERE o.change_reason = {NEW_REASON} AND o.{base_filter}
        )
        SELECT
            COUNT(*)                                                                   AS total_new,
            SUM(qty)                                                                   AS total_lots,
            SUM(CASE WHEN order_type = {LIMIT_ORDER_TYPE} THEN 1 ELSE 0 END)          AS limit_count,
            100.0*SUM(CASE WHEN order_type = {LIMIT_ORDER_TYPE} THEN 1 ELSE 0 END)/COUNT(*) AS limit_pct_count,
            SUM(CASE WHEN order_type = {LIMIT_ORDER_TYPE} THEN qty ELSE 0 END)        AS limit_lots,
            100.0*SUM(CASE WHEN order_type = {LIMIT_ORDER_TYPE} THEN qty ELSE 0 END)/SUM(qty) AS limit_pct_lots,
            SUM(is_iceberg)                                                            AS iceberg_count,
            100.0*SUM(is_iceberg)/COUNT(*)                                            AS iceberg_pct_count,
            SUM(is_iceberg * qty)                                                      AS iceberg_lots,
            100.0*SUM(is_iceberg * qty)/SUM(qty)                                      AS iceberg_pct_lots
        FROM new_orders
    """).fetchdf()
    print(df.T.to_string())
    results["Q5_order_types"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q6 — Ratio: busiest 30-min window / calmest 30-min window
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q6 · Busiest vs. calmest 30-min window ratio (new orders)")
    sc = session_case("entry_ts")
    df_windows = con.execute(f"""
        SELECT {sc} AS time_window, COUNT(*) AS order_count
        FROM orders
        WHERE change_reason = {NEW_REASON} AND {base_filter}
        GROUP BY 1
        ORDER BY order_count DESC
    """).fetchdf()
    busiest = df_windows.iloc[0]
    calmest = df_windows.iloc[-1]
    ratio   = busiest["order_count"] / calmest["order_count"]
    print(f"  Busiest window : {busiest['time_window']}  ({int(busiest['order_count']):,} orders)")
    print(f"  Calmest window : {calmest['time_window']}  ({int(calmest['order_count']):,} orders)")
    print(f"  Ratio          : {ratio:.1f}×")
    results["Q6_window_ratio"] = df_windows

    # ════════════════════════════════════════════════════════════════════════
    # Q7 — Total cancelled orders (count + volume)
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q7 · Total cancelled orders (full day)")
    df = con.execute(f"""
        SELECT
            COUNT(*)                   AS cancelled_events,
            SUM(remaining)             AS cancelled_lots,
            SUM(remaining * price)     AS cancelled_tl
        FROM orders
        WHERE change_reason IN ({CANCEL_REASONS_SQL})
          AND {base_filter}
    """).fetchdf()
    print(df.to_string(index=False))
    results["Q7_cancellations"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q8 — 30-min window with most cancellations
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q8 · Cancellations per 30-min window (opening/closing separate)")
    sc = session_case("change_ts")
    df = con.execute(f"""
        SELECT
            {sc}                       AS time_window,
            COUNT(*)                   AS cancelled_count,
            SUM(remaining)             AS cancelled_lots,
            SUM(remaining * price)     AS cancelled_tl
        FROM orders
        WHERE change_reason IN ({CANCEL_REASONS_SQL})
          AND {base_filter}
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    if not df.empty:
        peak = df.loc[df["cancelled_count"].idxmax()]
        print(f"\n  → Peak cancellation window: {peak['time_window']}  "
              f"({int(peak['cancelled_count']):,} events)")
    else:
        print("\n  → No cancellations found in this dataset.")
    results["Q8_cancel_windows"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q9 — Top/Bottom 10 stocks by cancelled-volume / executed-volume ratio
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q9 · Stocks by ratio of cancelled volume to executed volume")
    # Trade events: change_reason=3; executed qty = qty - remaining for that event
    df = con.execute(f"""
        WITH stock_stats AS (
            SELECT
                "ISLEM KODU" AS stock,
                SUM(CASE WHEN change_reason IN ({CANCEL_REASONS_SQL})
                         THEN remaining ELSE 0 END)          AS cancelled_lots,
                SUM(CASE WHEN change_reason = {TRADE_REASON}
                         THEN (qty - remaining) ELSE 0 END)  AS traded_lots
            FROM orders
            WHERE {base_filter}
            GROUP BY "ISLEM KODU"
            HAVING traded_lots > 0
        )
        SELECT
            stock,
            cancelled_lots,
            traded_lots,
            ROUND(cancelled_lots / traded_lots, 4)           AS cancel_to_trade_ratio
        FROM stock_stats
        ORDER BY cancel_to_trade_ratio DESC
    """).fetchdf()
    print("\n  ▲ Top 10 (highest cancel/trade ratio):")
    print(df.head(10).to_string(index=False))
    print("\n  ▼ Bottom 10 (lowest cancel/trade ratio):")
    print(df.tail(10).to_string(index=False))
    results["Q9_cancel_trade_ratio"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q10 — Cancelled volume as % of initially submitted volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q10 · Cancelled volume as % of total initial order volume")
    row = con.execute(f"""
        SELECT
            SUM(CASE WHEN change_reason = {NEW_REASON}
                     THEN qty ELSE 0 END)                    AS new_lots,
            SUM(CASE WHEN change_reason = {NEW_REASON}
                     THEN order_tl ELSE 0 END)               AS new_tl,
            SUM(CASE WHEN change_reason IN ({CANCEL_REASONS_SQL})
                     THEN remaining ELSE 0 END)              AS cancel_lots,
            SUM(CASE WHEN change_reason IN ({CANCEL_REASONS_SQL})
                     THEN remaining * price ELSE 0 END)      AS cancel_tl
        FROM orders
        WHERE {base_filter}
    """).fetchone()
    nl, nt, cl, ct = row
    print(f"  Total new-order volume  : {nl:>18,.0f} lots   /  {nt:>20,.0f} TL")
    print(f"  Total cancelled volume  : {cl:>18,.0f} lots   /  {ct:>20,.0f} TL")
    print(f"  Cancel rate (lots)      : {pct(cl, nl):>8.2f}%")
    print(f"  Cancel rate (TL)        : {pct(ct, nt):>8.2f}%")
    results["Q10_cancel_rate"] = pd.DataFrame([{
        "new_lots": nl, "new_tl": nt, "cancel_lots": cl, "cancel_tl": ct,
        "pct_lots": pct(cl, nl), "pct_tl": pct(ct, nt)
    }])

    # ════════════════════════════════════════════════════════════════════════
    # Q11 — Overlap: top 10 by order volume vs top 10 by trade volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q11 · Overlap: top 10 by order volume vs. top 10 by trade volume")
    top10_order = set(con.execute(f"""
        SELECT "ISLEM KODU" FROM orders
        WHERE change_reason = {NEW_REASON} AND {base_filter}
        GROUP BY "ISLEM KODU"
        ORDER BY SUM(order_tl) DESC LIMIT 10
    """).fetchdf()["ISLEM KODU"].tolist())

    top10_trade = set(con.execute(f"""
        SELECT "ISLEM KODU" FROM orders
        WHERE change_reason = {TRADE_REASON} AND {base_filter}
        GROUP BY "ISLEM KODU"
        ORDER BY SUM((qty - remaining) * price) DESC LIMIT 10
    """).fetchdf()["ISLEM KODU"].tolist())

    overlap = top10_order & top10_trade
    only_order = top10_order - top10_trade
    only_trade = top10_trade - top10_order
    print(f"  Top 10 by ORDER volume  : {sorted(top10_order)}")
    print(f"  Top 10 by TRADE volume  : {sorted(top10_trade)}")
    print(f"  Overlap ({len(overlap)}/10)           : {sorted(overlap)}")
    print(f"  Only in order top 10    : {sorted(only_order)}")
    print(f"  Only in trade top 10    : {sorted(only_trade)}")
    results["Q11_overlap"] = pd.DataFrame({
        "category": ["order_only", "both", "trade_only"],
        "stocks": [sorted(only_order), sorted(overlap), sorted(only_trade)]
    })

    # ════════════════════════════════════════════════════════════════════════
    # Q12 — Same window for highest order volume vs highest trade volume?
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q12 · Highest order-volume window = highest trade-volume window?")
    sc_entry  = session_case("entry_ts")
    sc_change = session_case("change_ts")

    peak_order_win = con.execute(f"""
        SELECT {sc_entry} AS time_window, SUM(order_tl) AS tl
        FROM orders WHERE change_reason={NEW_REASON} AND {base_filter}
        GROUP BY 1 ORDER BY tl DESC LIMIT 1
    """).fetchone()

    peak_trade_win = con.execute(f"""
        SELECT {sc_change} AS time_window, SUM((qty - remaining)*price) AS tl
        FROM orders WHERE change_reason={TRADE_REASON} AND {base_filter}
        GROUP BY 1 ORDER BY tl DESC LIMIT 1
    """).fetchone()

    if peak_order_win and peak_trade_win:
        same = peak_order_win[0] == peak_trade_win[0]
        print(f"  Peak ORDER volume  : window={peak_order_win[0]}  TL={peak_order_win[1]:,.0f}")
        print(f"  Peak TRADE volume  : window={peak_trade_win[0]}  TL={peak_trade_win[1]:,.0f}")
        print(f"  Same window?       : {'YES ✓' if same else 'NO ✗'}")
        results["Q12_peak_windows"] = pd.DataFrame([{
            "peak_order_window": peak_order_win[0], "peak_order_tl": peak_order_win[1],
            "peak_trade_window": peak_trade_win[0], "peak_trade_tl": peak_trade_win[1],
            "same_window": same
        }])
    else:
        print("  → Insufficient data (no trade events found).")
        results["Q12_peak_windows"] = pd.DataFrame()

    # ════════════════════════════════════════════════════════════════════════
    # Q13 — How many stocks concentrate 50% and 80% of order volume?
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q13 · Stocks concentrating 50% and 80% of total order volume")
    df = con.execute(f"""
        WITH sv AS (
            SELECT "ISLEM KODU" AS stock, SUM(order_tl) AS tl
            FROM orders WHERE change_reason={NEW_REASON} AND {base_filter}
            GROUP BY "ISLEM KODU"
            ORDER BY tl DESC
        ),
        cumulative AS (
            SELECT
                stock,
                tl,
                SUM(tl) OVER (ORDER BY tl DESC ROWS UNBOUNDED PRECEDING) AS cum_tl,
                SUM(tl) OVER ()                                            AS grand_total
            FROM sv
        )
        SELECT
            COUNT(*) AS total_stocks,
            COUNT(CASE WHEN cum_tl - tl < 0.50 * grand_total THEN 1 END) + 1 AS stocks_50pct,
            COUNT(CASE WHEN cum_tl - tl < 0.80 * grand_total THEN 1 END) + 1 AS stocks_80pct
        FROM cumulative
    """).fetchone()
    print(f"  Total unique stocks              : {df[0]:,}")
    print(f"  Stocks to reach 50% of volume   : {df[1]}")
    print(f"  Stocks to reach 80% of volume   : {df[2]}")
    results["Q13_concentration"] = pd.DataFrame([{
        "total_stocks": df[0], "stocks_for_50pct": df[1], "stocks_for_80pct": df[2]
    }])

    # ════════════════════════════════════════════════════════════════════════
    # Q14 — 30-min window with highest average lot size per order
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q14 · 30-min window with highest avg lot size per order")
    sc = session_case("entry_ts")
    df = con.execute(f"""
        SELECT
            {sc}             AS time_window,
            COUNT(*)          AS order_count,
            AVG(qty)          AS avg_lots_per_order,
            SUM(qty)          AS total_lots
        FROM orders
        WHERE change_reason={NEW_REASON} AND {base_filter} AND qty > 0
        GROUP BY 1
        ORDER BY avg_lots_per_order DESC
    """).fetchdf()
    print(df.to_string(index=False))
    print(f"\n  → Window with highest avg lot size: {df.iloc[0]['time_window']}  "
          f"(avg {df.iloc[0]['avg_lots_per_order']:,.0f} lots/order)")
    results["Q14_avg_lot_size"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q15 — Average order size (TL) in opening / midday / closing
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q15 · Average order size (TL) by period: opening / midday / closing")
    bsc = broad_session_case("entry_ts")
    df = con.execute(f"""
        SELECT
            {bsc}            AS period,
            COUNT(*)          AS order_count,
            AVG(order_tl)     AS avg_order_tl,
            MEDIAN(order_tl)  AS median_order_tl,
            MIN(order_tl)     AS min_order_tl,
            MAX(order_tl)     AS max_order_tl
        FROM orders
        WHERE change_reason={NEW_REASON} AND {base_filter} AND order_tl > 0
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    results["Q15_avg_order_size"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Q16 — Top 100 orders by volume: opening vs closing session distribution
    # ════════════════════════════════════════════════════════════════════════
    print_section("Q16 · Top 100 orders by lot volume — session distribution")
    sc = session_case("entry_ts")
    df = con.execute(f"""
        WITH top100 AS (
            SELECT
                "ISLEM KODU",
                qty,
                order_tl,
                "SEANS",
                {sc} AS time_window
            FROM orders
            WHERE change_reason={NEW_REASON} AND {base_filter} AND qty > 0
            ORDER BY qty DESC
            LIMIT 100
        )
        SELECT
            time_window,
            COUNT(*) AS count,
            SUM(qty) AS total_lots
        FROM top100
        GROUP BY time_window
        ORDER BY time_window
    """).fetchdf()
    print(df.to_string(index=False))
    results["Q16_top100_sessions"] = df

    # ════════════════════════════════════════════════════════════════════════
    # Optional: export to Excel
    # ════════════════════════════════════════════════════════════════════════
    if output_path:
        print(f"\n\nWriting results to {output_path} ...")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, frame in results.items():
                frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        print("Done.")

    con.close()
    print("\n" + "="*64)
    print("  Analysis complete.")
    print("="*64 + "\n")


# ────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BIST full order book analysis"
    )
    parser.add_argument("csv_file", help="Path to TED CSV file (semicolon-delimited)")
    parser.add_argument(
        "--output", "-o",
        help="Optional path for Excel output (e.g. results.xlsx)",
        default=None,
    )
    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"ERROR: File not found: {args.csv_file}")
        sys.exit(1)

    run_analysis(args.csv_file, args.output)
