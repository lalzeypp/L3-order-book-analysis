#!/usr/bin/env python3
"""
Borsa Istanbul - Equity Market Full Order Book analysis; 16 overview questions.

Delimiter: semicolon (;), encoding: UTF-8

Usage:
  python3 order_flow_overview.py /path/to/TED_YYYYMMDD.csv
  python3 order_flow_overview.py /path/to/TED_YYYYMMDD.csv --output overview_results.xlsx

Notes:
  - The output file specified with --output must have a .xlsx extension.
  - The Excel output will contain multiple sheets, one for each research question.
  - The input CSV file must be semicolon-delimited (;) and match the column format.
  - Constants, SQL helpers, and the view setup live in ted_common.py.
"""

import sys
import argparse
import duckdb
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

from ted_common import (
    NEW_REASON, TRADE_REASON, NORMAL_ORDER_CAT,
    CANCEL_REASONS, CANCEL_REASONS_SQL,
    LIMIT_ORDER_TYPE, ICEBERG_BIT,
    session_case, broad_session_case,
    create_orders_view, print_section, pct,
    CHART_RCPARAMS, timestamped_path,
)

plt.rcParams.update(CHART_RCPARAMS)


def compute_rolling_intensity(con, base_filter: str, window_minutes: int = 30) -> pd.DataFrame:
    """Compute rolling order intensity at 1-minute granularity using a trailing window."""
    return con.execute(f"""
        WITH minute_counts AS (
            SELECT
                DATE_TRUNC('minute', entry_ts) AS minute_ts,
                COUNT(*)       AS order_count,
                SUM(qty)       AS lots,
                SUM(order_tl)  AS tl
            FROM orders
            WHERE change_reason = {NEW_REASON}
              AND {base_filter}
              AND entry_ts IS NOT NULL
            GROUP BY DATE_TRUNC('minute', entry_ts)
        )
        SELECT
            minute_ts,
            SUM(order_count) OVER (
                ORDER BY minute_ts
                RANGE BETWEEN INTERVAL '{window_minutes} minutes' PRECEDING AND CURRENT ROW
            ) AS rolling_order_count,
            SUM(lots) OVER (
                ORDER BY minute_ts
                RANGE BETWEEN INTERVAL '{window_minutes} minutes' PRECEDING AND CURRENT ROW
            ) AS rolling_lots,
            SUM(tl) OVER (
                ORDER BY minute_ts
                RANGE BETWEEN INTERVAL '{window_minutes} minutes' PRECEDING AND CURRENT ROW
            ) AS rolling_tl
        FROM minute_counts
        ORDER BY minute_ts
    """).fetchdf()


# MAIN
def run_analysis(csv_path: str, output_path=None) -> None:
    con = duckdb.connect()
    results: dict[str, pd.DataFrame] = {}

    if output_path:
        output_path = timestamped_path(output_path)

    print(f"\nLoading CSV via DuckDB (no full RAM load needed): {csv_path}")
    create_orders_view(con, csv_path)

    base_filter = f"order_cat = {NORMAL_ORDER_CAT}"

    # 1 — Total new orders
    # ════════════════════════════════════════════════════════════════════════
    print_section("Total new orders submitted:")
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

    # 2 — Buy vs Sell among new orders (count + volume)
    # ════════════════════════════════════════════════════════════════════════
    print_section("Buy vs. Sell ratio among new orders:")
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

    # 3 — Top 10 stocks share of total order volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Top 10 stocks share of total new-order volume:")
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

    # 4 — 30-min windows: most new orders (opening/closing treated separately)
    # ════════════════════════════════════════════════════════════════════════
    print_section("New orders per 30-min window (opening/closing separate:)")
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

    # Rolling 30-min intensity chart
    # ════════════════════════════════════════════════════════════════════════
    print_section("Rolling 30-min order intensity (Q4b):")
    rolling_df = compute_rolling_intensity(con, base_filter, window_minutes=30)
    results["Q4b_rolling_intensity"] = rolling_df

    if not rolling_df.empty:
        out_dir = Path("output")
        out_dir.mkdir(exist_ok=True)

        peak_idx = rolling_df["rolling_order_count"].idxmax()
        peak_row = rolling_df.loc[peak_idx]
        peak_ts  = pd.Timestamp(peak_row["minute_ts"])

        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(rolling_df["minute_ts"], rolling_df["rolling_order_count"],
                color="#4c72b0", linewidth=1)
        ax.annotate(
            f"{peak_ts.strftime('%H:%M')}\n{int(peak_row['rolling_order_count']):,} orders",
            xy=(peak_row["minute_ts"], peak_row["rolling_order_count"]),
            xytext=(20, -30), textcoords="offset points",
            arrowprops=dict(arrowstyle="->", color="crimson"),
            color="crimson", fontsize=9,
        )
        ax.set_title("Order Submission Intensity — Rolling 30-Minute Window")
        ax.set_xlabel("Time of Day (end of rolling window)")
        ax.set_ylabel("Orders in Window")
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        fig.autofmt_xdate()
        plt.tight_layout()
        chart_path = out_dir / "order_flow_intensity.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        print(f"  Chart saved: {chart_path}")
        print(f"  Peak 30-min window ending {peak_ts.strftime('%H:%M')}: "
              f"{int(peak_row['rolling_order_count']):,} orders, "
              f"{int(peak_row['rolling_lots']):,} lots")
    else:
        print("  No data for rolling intensity chart.")

    # 5 — Limit and iceberg ratio among new orders
    # ════════════════════════════════════════════════════════════════════════
    print_section("Limit and iceberg order ratio among new orders:")
    df = con.execute(f"""
        WITH new_orders AS (
            SELECT
                qty,
                order_type,
                CASE WHEN visible_qty IS NOT NULL AND visible_qty != qty
                     THEN 1 ELSE 0 END AS is_iceberg
            FROM orders
            WHERE change_reason = {NEW_REASON} AND {base_filter}
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

    # 6 — Ratio: busiest 30-min window / calmest 30-min window
    # ════════════════════════════════════════════════════════════════════════
    print_section("Busiest vs. calmest 30-min window ratio (new orders):")
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

    # 7 — Total cancelled orders (count + volume)
    # ════════════════════════════════════════════════════════════════════════
    print_section("Total cancelled orders (full day):")
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

    # Expiry vs other cancellations
    df_q7b = con.execute(f"""
        SELECT
            CASE WHEN change_reason = 19
                 THEN 'Expired (reason 19)'
                 ELSE 'Other cancellations'
            END                    AS cancel_type,
            COUNT(*)               AS events,
            SUM(remaining)         AS lots,
            SUM(remaining * price) AS tl
        FROM orders
        WHERE change_reason IN ({CANCEL_REASONS_SQL})
          AND {base_filter}
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print("\n  Expiry vs. other cancellations:")
    print(df_q7b.to_string(index=False))
    results["Q7b_expiry_breakdown"] = df_q7b

    # 8 — 30-min window with most cancellations
    # ════════════════════════════════════════════════════════════════════════
    print_section("Cancellations per 30-min window (opening/closing separate):")
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

    # 9 — Top/Bottom 10 stocks by cancelled-volume / executed-volume ratio
    # ════════════════════════════════════════════════════════════════════════
    print_section("Stocks by ratio of cancelled volume to executed volume:")
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

    # 10 — Cancelled volume as % of initially submitted volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Cancelled volume as % of total initial order volume:")
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

    # 11 — Overlap: top 10 by order volume vs top 10 by trade volume
    # ════════════════════════════════════════════════════════════════════════
    print_section("Overlap: top 10 by order volume vs. top 10 by trade volume:")
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

    # 12 — Same window for highest order volume vs highest trade volume?
    # ════════════════════════════════════════════════════════════════════════
    print_section("Highest order-volume window = highest trade-volume window?")
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

    # 13 — How many stocks concentrate 50% and 80% of order volume? tickers ending in '.E'
    # ════════════════════════════════════════════════════════════════════════
    print_section("Stocks concentrating 50% and 80% of total order volume (equities only):")
    df = con.execute(f"""
        WITH sv AS (
            SELECT "ISLEM KODU" AS stock, SUM(order_tl) AS tl
            FROM orders
            WHERE change_reason = {NEW_REASON}
              AND {base_filter}
              AND "ISLEM KODU" LIKE '%.E'
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
    print(f"  Total unique equity stocks (.E)      : {df[0]:,}")
    print(f"  Stocks to reach 50% of volume        : {df[1]}")
    print(f"  Stocks to reach 80% of volume        : {df[2]}")
    results["Q13_concentration"] = pd.DataFrame([{
        "total_stocks": df[0], "stocks_for_50pct": df[1], "stocks_for_80pct": df[2]
    }])

    # 14 — 30-min window with highest average lot size per order
    # ════════════════════════════════════════════════════════════════════════
    print_section("30-min window with highest avg lot size per order:")
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

    # 15 — Average order size (TL) in opening / midday / closing; bar chart with mean bars and median markers
    # ════════════════════════════════════════════════════════════════════════
    print_section("Average order size (TL) by period: opening / midday / closing")
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

    # 15 chart
    if not df.empty:
        out_dir = Path("output")
        out_dir.mkdir(exist_ok=True)

        period_order = {'1_Opening': 0, '2_Midday': 1, '3_Closing': 2}
        df_plot = df[df['period'].isin(period_order)].copy()
        df_plot = df_plot.sort_values('period', key=lambda x: x.map(period_order))
        labels = [p.split('_', 1)[1] for p in df_plot['period']]

        fig, ax = plt.subplots(figsize=(8, 5))
        x = list(range(len(df_plot)))
        colors = ['#4c72b0', '#dd8452', '#55a868']
        ax.bar(x, df_plot['avg_order_tl'], color=colors, alpha=0.85, label='Mean')
        ax.scatter(x, df_plot['median_order_tl'], color='crimson', zorder=5,
                   s=120, marker='D', label='Median')
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=12)
        ax.set_title('Average vs Median Order Size by Session')
        ax.set_ylabel('Order Size (TL)')
        ax.legend()
        plt.tight_layout()
        chart_path = out_dir / "avg_order_size_by_session.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        print(f"\n  Q15 chart saved: {chart_path}")

    # 16 — Top 100 orders by volume: opening vs closing session distribution
    # ════════════════════════════════════════════════════════════════════════
    print_section("Top 100 orders by lot volume — session distribution:")
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

    # closing-session callout
    z_rows = df[df["time_window"] == "Z_CLOSING"]
    if not z_rows.empty:
        z = z_rows.iloc[0]
        print(f"\n  -> Of the 100 largest orders of the day, {int(z['count'])} "
              f"landed in the closing auction (Z_CLOSING), "
              f"totaling {int(z['total_lots']):,} lots.")
    else:
        print("\n  -> 0 of the top 100 orders landed in the closing auction (Z_CLOSING).")

    # export to excel
    # ════════════════════════════════════════════════════════════════════════
    if output_path:
        print(f"\n\nWriting results to {output_path} ...")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            row = 0
            for q_name, frame in results.items():
                # section title row
                pd.DataFrame([[q_name.replace("_", " · ")]]).to_excel(
                    writer, sheet_name="Results", startrow=row, index=False, header=False
                )
                row += 1
                # data table (includes column headers)
                frame.to_excel(writer, sheet_name="Results", startrow=row, index=False)
                row += 1 + len(frame) + 2  # header + data rows + 2 blank rows
        print("Done.")

    con.close()
    print("\n" + "="*64)
    print("  Analysis complete.")
    print("="*64 + "\n")


# entry point
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BIST full order book analysis (16 overview questions)"
    )
    parser.add_argument("csv_file", help="Path to TED CSV file (semicolon-delimited)")
    parser.add_argument(
        "--output", "-o",
        help="Path for Excel output (default: overview_results.xlsx)",
        default="overview_results.xlsx",
    )
    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"ERROR: File not found: {args.csv_file}")
        sys.exit(1)

    run_analysis(args.csv_file, args.output)