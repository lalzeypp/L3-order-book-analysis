#!/usr/bin/env python3
"""
Borsa Istanbul - Order Lifecycle & Microstructure Analysis

Reconstructs a per-order lifecycle from EMIR NO grouping and answers:
  Q-L1  Order lifetime distribution (emir bekleme süresi)
  Q-L2  HFT-like behavioral fingerprint share
  Q-L3  Passive vs aggressive liquidity
  Q-L4  Order conversion funnel
  Q-L5  Resting time vs outcome cross-analysis (E1 vs E1_1)
  Q-L6  HFT-like share by time of day

Usage:
  # Validate lifecycle table on a 1% sample, then stop:
  python order_lifecycle_microstructure.py /path/to/CSV --validate-only

  # Full analysis on complete dataset:
  python order_lifecycle_microstructure.py /path/to/CSV --full --output lifecycle_results.xlsx

  # Dev: full analysis on smaller sample (faster):
  python order_lifecycle_microstructure.py /path/to/CSV --sample 0.05

IMPORTANT - HFT CAVEAT (printed at runtime too):
  'likely_hft' identifies HFT-LIKE BEHAVIORAL PATTERNS inferred from order timing
  and lifecycle data. This dataset contains no trader IDs. All behavioral
  classifications are proxies only — NOT confirmed participant identity.
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
    CONTINUOUS_START_HOUR, CONTINUOUS_END_HOUR,
    create_orders_view, print_section, pct,
    CHART_RCPARAMS, timestamped_path,
)

plt.rcParams.update(CHART_RCPARAMS)

OUTPUT_DIR = Path("output")

HFT_CAVEAT = (
    "  *** BEHAVIORAL PROXY — NOT CONFIRMED IDENTITY ***\n"
    "  'likely_hft' flags orders whose TIMING matches HFT patterns (sub-second\n"
    "  lifetime, cancelled without fill). No trader IDs exist in this data;\n"
    "  these are behavioral inferences only. Sanity-check small-denominator\n"
    "  stocks before drawing conclusions."
)

# ── SQL fragment helpers ──────────────────────────────────────────────────────

def _lifetime_bucket_sql(col: str = "lifetime_seconds") -> str:
    """EOD-expired orders get their own bucket regardless of lifetime_seconds."""
    return f"""
        CASE
            WHEN ever_expired = 1                THEN '8_eod'
            WHEN {col} IS NULL OR {col} < 0     THEN '0_unknown'
            WHEN {col} < 0.1                    THEN '1_sub_0.1s'
            WHEN {col} < 1.0                    THEN '2_0.1-1s'
            WHEN {col} < 5.0                    THEN '3_1-5s'
            WHEN {col} < 30.0                   THEN '4_5-30s'
            WHEN {col} < 300.0                  THEN '5_30s-5min'
            WHEN {col} < 1800.0                 THEN '6_5-30min'
            ELSE                                     '7_gt30min'
        END
    """


def _terminal_state_sql() -> str:
    return """
        CASE
            WHEN ever_traded = 1 AND traded_lots >= original_qty * 0.99
                THEN '1_fully_filled'
            WHEN ever_traded = 1 AND (ever_cancelled = 1 OR ever_expired = 1)
                THEN '2_partially_filled'
            WHEN ever_traded = 0 AND ever_expired = 1
                THEN '4_expired_no_fill'
            WHEN ever_traded = 0 AND ever_cancelled = 1
                THEN '3_cancelled_no_fill'
            ELSE '5_other_open'
        END
    """


def _hft_flag_sql() -> str:
    return "(lifetime_seconds < 1.0 AND ever_cancelled = 1 AND ever_traded = 0)"


def _behavior_class_sql() -> str:
    return f"""
        CASE
            WHEN time_to_first_fill_seconds IS NOT NULL
                 AND time_to_first_fill_seconds < 0.1
                THEN 'aggressive'
            WHEN {_hft_flag_sql()}
                THEN 'likely_hft'
            WHEN (lifetime_seconds > 60.0 AND time_to_first_fill_seconds IS NOT NULL)
                 OR (ever_expired = 1 AND ever_traded = 0)
                THEN 'likely_passive'
            ELSE 'mixed'
        END
    """


def _born_time_window_sql() -> str:
    """30-min bucket using only born_ts (lifecycle table has no SEANS column)."""
    h = "EXTRACT(HOUR FROM born_ts)"
    m = "EXTRACT(MINUTE FROM born_ts)"
    return f"""
        CASE
            WHEN {h} < {CONTINUOUS_START_HOUR}  THEN 'A_OPENING'
            WHEN {h} >= {CONTINUOUS_END_HOUR}   THEN 'Z_CLOSING'
            ELSE
                LPAD(CAST({h} AS VARCHAR), 2, '0') || ':' ||
                CASE WHEN {m} < 30 THEN '00' ELSE '30' END || '-' ||
                CASE
                    WHEN {m} < 30
                        THEN LPAD(CAST({h} AS VARCHAR), 2, '0') || ':30'
                    ELSE LPAD(CAST(({h} + 1) AS VARCHAR), 2, '0') || ':00'
                END
        END
    """


# ── Lifecycle table ───────────────────────────────────────────────────────────

def build_lifecycle_table(con: duckdb.DuckDBPyConnection, sample_pct: float = 1.0) -> None:
    """
    Build the per-order lifecycle TEMP TABLE from the 'orders' view.

    sample_pct = 1.0  → full dataset
    sample_pct = 0.01 → 1% hash-based sample (all events of sampled orders included)

    traded_lots uses MAX(qty-remaining) across trade events, which equals the net
    filled quantity. SUM would double-count because qty-remaining is cumulative.

    n_qualifying_improvements = MAX(sub_order_number): counts how many times the
    order was improved (price improvement or lot increase via change_reason=5).
    Each improvement resets BIST time-priority. Computed via LAG window functions.
    """
    sample_clause = ""
    if sample_pct < 1.0:
        n = max(1, round(1.0 / sample_pct))
        sample_clause = f'AND hash("EMIR NO") % {n} = 0'

    con.execute(f"""
        CREATE TEMP TABLE order_lifecycle AS
        WITH base AS (
            SELECT
                "EMIR NO", "ALIS_SATIS", "ISLEM KODU",
                change_reason, entry_ts, change_ts, price, qty, remaining
            FROM orders
            WHERE order_cat = {NORMAL_ORDER_CAT}
            {sample_clause}
        ),
        with_lag AS (
            SELECT *,
                LAG(price) OVER (PARTITION BY "EMIR NO" ORDER BY change_ts, entry_ts) AS prev_price,
                LAG(qty)   OVER (PARTITION BY "EMIR NO" ORDER BY change_ts, entry_ts) AS prev_qty
            FROM base
        ),
        with_qualifies AS (
            SELECT *,
                CASE
                    WHEN change_reason = 5
                         AND prev_price IS NOT NULL
                         AND (
                             ("ALIS_SATIS" = 'A' AND price > prev_price) OR
                             ("ALIS_SATIS" = 'S' AND price < prev_price) OR
                             (qty > prev_qty)
                         )
                    THEN 1 ELSE 0
                END AS qualifies
            FROM with_lag
        ),
        with_sub AS (
            SELECT *,
                SUM(qualifies) OVER (
                    PARTITION BY "EMIR NO"
                    ORDER BY change_ts, entry_ts
                    ROWS UNBOUNDED PRECEDING
                ) AS sub_order_number
            FROM with_qualifies
        )
        SELECT
            "EMIR NO"                                                             AS order_id,
            MAX("ISLEM KODU")                                                     AS stock,
            MAX("ALIS_SATIS")                                                     AS side,
            MIN(CASE WHEN change_reason = {NEW_REASON}   THEN entry_ts  END)     AS born_ts,
            MAX(change_ts)                                                         AS last_event_ts,
            MIN(CASE WHEN change_reason = {TRADE_REASON} THEN change_ts END)     AS first_fill_ts,
            MAX(CASE WHEN change_reason = {NEW_REASON}   THEN qty       END)     AS original_qty,
            COUNT(*)                                                               AS event_count,
            MAX(CASE WHEN change_reason = {TRADE_REASON} THEN 1 ELSE 0 END)      AS ever_traded,
            MAX(CASE WHEN change_reason IN ({CANCEL_REASONS_SQL})
                     THEN 1 ELSE 0 END)                                           AS ever_cancelled,
            MAX(CASE WHEN change_reason = 19             THEN 1 ELSE 0 END)      AS ever_expired,
            COALESCE(MAX(CASE WHEN change_reason = {TRADE_REASON}
                              THEN qty - remaining END), 0)                       AS traded_lots,
            EXTRACT(EPOCH FROM (
                MAX(change_ts) -
                MIN(CASE WHEN change_reason = {NEW_REASON} THEN entry_ts END)
            ))                                                                     AS lifetime_seconds,
            EXTRACT(EPOCH FROM (
                MIN(CASE WHEN change_reason = {TRADE_REASON} THEN change_ts END) -
                MIN(CASE WHEN change_reason = {NEW_REASON}   THEN entry_ts  END)
            ))                                                                     AS time_to_first_fill_seconds,
            COUNT(CASE WHEN change_reason = 5 THEN 1 END)                         AS n_modifications,
            MAX(sub_order_number)                                                  AS n_qualifying_improvements
        FROM with_sub
        GROUP BY "EMIR NO"
        HAVING MIN(CASE WHEN change_reason = {NEW_REASON} THEN entry_ts END) IS NOT NULL
    """)


# ── Validation ────────────────────────────────────────────────────────────────

def _show_sample_with_raw_events(
    con: duckdb.DuckDBPyConnection,
    csv_path: str,
    label: str,
    condition: str,
    n: int = 3,
) -> None:
    print_section(f"Sample orders — {label}")
    lc = con.execute(f"""
        SELECT
            order_id, stock, side,
            born_ts, first_fill_ts, last_event_ts,
            original_qty, traded_lots, event_count,
            ROUND(lifetime_seconds, 3)            AS lifetime_s,
            ROUND(time_to_first_fill_seconds, 3)  AS fill_lag_s,
            ever_traded, ever_cancelled, ever_expired
        FROM order_lifecycle
        WHERE {condition}
        LIMIT {n}
    """).fetchdf()
    if lc.empty:
        print("  (no orders matching this condition in sample)")
        return
    print("\nLifecycle rows:")
    print(lc.to_string(index=False))

    order_ids = lc["order_id"].tolist()
    ids_sql = ",".join(f"'{oid}'" for oid in order_ids)
    raw_con = duckdb.connect()
    create_orders_view(raw_con, csv_path)
    raw = raw_con.execute(f"""
        SELECT
            "EMIR NO" AS order_id, change_reason,
            entry_ts, change_ts, qty, remaining, ROUND(price, 4) AS price, "SEANS"
        FROM orders
        WHERE "EMIR NO" IN ({ids_sql}) AND order_cat = {NORMAL_ORDER_CAT}
        ORDER BY "EMIR NO", change_ts
    """).fetchdf()
    raw_con.close()

    print("\nRaw event sequences:")
    for oid in order_ids:
        evts = raw[raw["order_id"] == oid]
        print(f"\n  Order {oid}  ({len(evts)} events):")
        print(evts.drop(columns=["order_id"]).to_string(index=False))


def validate_lifecycle(con: duckdb.DuckDBPyConnection, csv_path: str) -> None:
    print_section("Lifecycle table — summary statistics")
    summary = con.execute("""
        SELECT
            COUNT(*)                                                          AS total_orders,
            SUM(ever_traded)                                                  AS n_ever_traded,
            SUM(ever_cancelled)                                               AS n_ever_cancelled,
            SUM(ever_expired)                                                 AS n_ever_expired,
            SUM(CASE WHEN ever_traded=0 AND ever_cancelled=0
                          AND ever_expired=0 THEN 1 ELSE 0 END)              AS n_no_terminal_event,
            ROUND(AVG(lifetime_seconds), 3)                                   AS avg_lifetime_s,
            ROUND(MEDIAN(lifetime_seconds), 3)                                AS median_lifetime_s,
            ROUND(AVG(event_count), 2)                                        AS avg_events_per_order,
            SUM(CASE WHEN lifetime_seconds < 0 THEN 1 ELSE 0 END)            AS sanity_neg_lifetime,
            SUM(CASE WHEN time_to_first_fill_seconds < 0 THEN 1 ELSE 0 END)  AS sanity_neg_fill_lag,
            SUM(CASE WHEN traded_lots > original_qty * 1.05
                         AND ever_traded = 1 THEN 1 ELSE 0 END)              AS sanity_overfilled
        FROM order_lifecycle
    """).fetchdf()
    print(summary.T.to_string())

    neg_lt = int(summary["sanity_neg_lifetime"].iloc[0])
    neg_fl = int(summary["sanity_neg_fill_lag"].iloc[0])
    overf  = int(summary["sanity_overfilled"].iloc[0])
    n_tot  = int(summary["total_orders"].iloc[0])
    print(f"\n  Sanity: neg_lifetime={neg_lt}, neg_fill_lag={neg_fl}, "
          f"overfilled={overf} ({100*overf/n_tot:.2f}% — modified orders expected)")
    if neg_lt == 0 and neg_fl == 0:
        print("  ✓ Negative-time checks pass.")

    print_section("Flag distribution (ever_traded / ever_cancelled / ever_expired)")
    flags = con.execute("""
        SELECT
            ever_traded, ever_cancelled, ever_expired,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM order_lifecycle
        GROUP BY 1, 2, 3
        ORDER BY ever_traded DESC, ever_cancelled DESC, ever_expired DESC
    """).fetchdf()
    print(flags.to_string(index=False))

    for label, cond in [
        ("TRADED",               "ever_traded = 1 AND original_qty > 0"),
        ("CANCELLED, no fill",   "ever_cancelled = 1 AND ever_traded = 0 AND ever_expired = 0"),
        ("EXPIRED, no fill",     "ever_expired = 1 AND ever_traded = 0"),
    ]:
        _show_sample_with_raw_events(con, csv_path, label, cond)


# ── Q-L1: Lifetime distribution ───────────────────────────────────────────────

def question_l1(con: duckdb.DuckDBPyConnection, out_dir: Path, results: dict) -> None:
    print_section("Q-L1: Order Lifetime Distribution (emir bekleme süresi)")

    lb = _lifetime_bucket_sql("lifetime_seconds")
    df = con.execute(f"""
        SELECT
            {lb} AS lifetime_bucket,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_orders,
            ROUND(SUM(original_qty), 0) AS total_lots,
            ROUND(100.0 * SUM(original_qty) / SUM(SUM(original_qty)) OVER (), 2) AS pct_lots
        FROM order_lifecycle
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    results["QL1_lifetime_buckets"] = df

    stats = con.execute("""
        SELECT
            ROUND(AVG(lifetime_seconds), 2)    AS mean_lifetime_s,
            ROUND(MEDIAN(lifetime_seconds), 2) AS median_lifetime_s
        FROM order_lifecycle
    """).fetchone()
    print(f"\n  Mean lifetime   : {stats[0]:>10,.2f}s  ({stats[0]/60:.1f} min)")
    print(f"  Median lifetime : {stats[1]:>10,.2f}s  — this is the honest 'typical' order")
    print(f"\n  The large mean/median gap ({stats[0]/max(stats[1], 0.001):.0f}x) confirms the"
          " bimodal structure:")
    print("  a large population of very short-lived orders (algorithmic) sits alongside")
    print("  a long tail of patient, resting orders (passive liquidity).")
    results["QL1_lifetime_stats"] = pd.DataFrame([{
        "mean_lifetime_s": stats[0], "median_lifetime_s": stats[1]
    }])

    # Chart: dual bar chart (% orders, % lots) by bucket
    BUCKET_LABELS = {
        '0_unknown': 'unknown', '1_sub_0.1s': '<0.1s', '2_0.1-1s': '0.1–1s',
        '3_1-5s': '1–5s', '4_5-30s': '5–30s', '5_30s-5min': '30s–5min',
        '6_5-30min': '5–30min', '7_gt30min': '>30min', '8_eod': 'EOD',
    }
    df_plot = df[df["lifetime_bucket"] != "0_unknown"].copy()
    df_plot["label"] = df_plot["lifetime_bucket"].map(BUCKET_LABELS)
    colors = ['#4c72b0'] * len(df_plot)
    if '8_eod' in df_plot["lifetime_bucket"].values:
        colors[-1] = '#c44e52'

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for ax, col, title, ylabel in [
        (axes[0], "pct_orders", "% of Order Count", "% of Orders"),
        (axes[1], "pct_lots",   "% of Submitted Lots", "% of Lots"),
    ]:
        ax.bar(df_plot["label"], df_plot[col], color=colors, alpha=0.85)
        ax.set_title(title)
        ax.set_xlabel("Order Lifetime")
        ax.set_ylabel(ylabel)
        ax.tick_params(axis='x', rotation=35)
    fig.suptitle("Order Lifetime Distribution", fontsize=12, fontweight='bold')
    plt.tight_layout()
    out_dir.mkdir(exist_ok=True)
    path = out_dir / "lifetime_distribution.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"\n  Chart saved: {path}")


# ── Q-L2: HFT-like behavioral fingerprint ────────────────────────────────────

def question_l2(con: duckdb.DuckDBPyConnection, out_dir: Path, results: dict) -> None:
    print_section("Q-L2: HFT-like Behavioral Fingerprint Share")
    print(HFT_CAVEAT)

    bc = _behavior_class_sql()
    df = con.execute(f"""
        SELECT
            {bc} AS behavior_class,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_orders,
            ROUND(SUM(original_qty), 0) AS total_lots,
            ROUND(100.0 * SUM(original_qty) / SUM(SUM(original_qty)) OVER (), 2) AS pct_lots
        FROM order_lifecycle
        GROUP BY 1
        ORDER BY pct_orders DESC
    """).fetchdf()
    print("\n  Behavioral class breakdown (all orders):")
    print(df.to_string(index=False))
    results["QL2_behavior_classes"] = df

    hft_flag = _hft_flag_sql()
    n_hft = int(con.execute(f"SELECT COUNT(*) FROM order_lifecycle WHERE {hft_flag}").fetchone()[0])
    if n_hft > 0:
        print(f"\n  Top 15 stocks by likely_hft order count ({n_hft:,} total hft-like orders):")
        df_stocks = con.execute(f"""
            WITH hft AS (
                SELECT stock, COUNT(*) AS hft_orders
                FROM order_lifecycle
                WHERE {hft_flag}
                GROUP BY stock
            ),
            tot AS (
                SELECT stock, COUNT(*) AS total_orders
                FROM order_lifecycle
                GROUP BY stock
            )
            SELECT
                h.stock,
                h.hft_orders,
                t.total_orders,
                ROUND(100.0 * h.hft_orders / t.total_orders, 2) AS hft_pct_of_stock,
                ROUND(100.0 * h.hft_orders / {n_hft}, 2)        AS pct_of_all_hft
            FROM hft h JOIN tot t ON h.stock = t.stock
            ORDER BY h.hft_orders DESC
            LIMIT 15
        """).fetchdf()
        print(df_stocks.to_string(index=False))
        print("\n  Sanity: check total_orders before treating high hft_pct_of_stock as meaningful.")
        results["QL2_hft_by_stock"] = df_stocks
    else:
        print("  (No likely_hft orders found in this sample.)")

    # Chart: grouped bar, % orders vs % lots by class
    CLASS_ORDER  = ['likely_hft', 'aggressive', 'likely_passive', 'mixed']
    CLASS_COLORS = {'likely_hft': '#c44e52', 'aggressive': '#dd8452',
                    'likely_passive': '#55a868', 'mixed': '#8172b2'}
    df_plot = df.set_index("behavior_class").reindex(
        [c for c in CLASS_ORDER if c in df["behavior_class"].values]
    ).reset_index()

    x = list(range(len(df_plot)))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 5))
    bars1 = ax.bar([i - width/2 for i in x], df_plot["pct_orders"], width,
                   label="% of Orders",
                   color=[CLASS_COLORS.get(c, '#9e9e9e') for c in df_plot["behavior_class"]],
                   alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], df_plot["pct_lots"], width,
                   label="% of Lots", alpha=0.55,
                   color=[CLASS_COLORS.get(c, '#9e9e9e') for c in df_plot["behavior_class"]])
    ax.set_xticks(x)
    ax.set_xticklabels(df_plot["behavior_class"], fontsize=10)
    ax.set_ylabel("Percentage")
    ax.set_title("Behavioral Order Classification (% of Orders vs % of Lots)\n"
                 "NOTE: Behavioral proxy — not confirmed participant identity")
    ax.legend()
    plt.tight_layout()
    out_dir.mkdir(exist_ok=True)
    path = out_dir / "behavioral_classification.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"\n  Chart saved: {path}")


# ── Q-L3: Passive vs aggressive liquidity ────────────────────────────────────

def question_l3(con: duckdb.DuckDBPyConnection, results: dict) -> None:
    print_section("Q-L3: Passive vs Aggressive Liquidity")

    df = con.execute("""
        SELECT
            CASE
                WHEN time_to_first_fill_seconds IS NOT NULL
                     AND time_to_first_fill_seconds < 0.1
                    THEN 'aggressive_took_liquidity'
                WHEN time_to_first_fill_seconds IS NOT NULL
                    THEN 'passive_provided_liquidity'
                ELSE 'never_filled'
            END AS liquidity_class,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_orders,
            ROUND(SUM(original_qty), 0) AS total_submitted_lots,
            ROUND(SUM(traded_lots), 0) AS total_traded_lots,
            ROUND(100.0 * SUM(original_qty) / SUM(SUM(original_qty)) OVER (), 2) AS pct_submitted_lots
        FROM order_lifecycle
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    results["QL3_liquidity"] = df

    # Among traded orders only: % of TRADED LOTS from passive vs aggressive
    df_tl = con.execute("""
        SELECT
            CASE
                WHEN time_to_first_fill_seconds < 0.1 THEN 'aggressive'
                ELSE 'passive'
            END AS source,
            ROUND(SUM(traded_lots), 0) AS traded_lots,
            ROUND(100.0 * SUM(traded_lots) / SUM(SUM(traded_lots)) OVER (), 2) AS pct_traded_lots
        FROM order_lifecycle
        WHERE ever_traded = 1 AND time_to_first_fill_seconds IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print("\n  Share of TRADED LOTS by source (among filled orders only):")
    print(df_tl.to_string(index=False))
    results["QL3_traded_lots_source"] = df_tl

    if not df_tl.empty and 'passive' in df_tl["source"].values:
        passive_pct = float(df_tl[df_tl["source"] == "passive"]["pct_traded_lots"].iloc[0])
        aggr_pct    = 100 - passive_pct
        print(f"\n  Interpretation: {passive_pct:.1f}% of traded lots were provided by passive orders")
        print(f"  (rested in the book before getting hit); {aggr_pct:.1f}% came from aggressive")
        print(f"  orders (crossed the spread on arrival). A high passive share indicates deep")
        print(f"  resting liquidity; a high aggressive share signals momentum/liquidity-taking flow.")


# ── Q-L4: Conversion funnel ───────────────────────────────────────────────────

def question_l4(con: duckdb.DuckDBPyConnection, out_dir: Path, results: dict) -> None:
    print_section("Q-L4: Order Conversion Funnel")

    ts = _terminal_state_sql()
    df = con.execute(f"""
        SELECT
            {ts} AS terminal_state,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_orders,
            ROUND(SUM(original_qty), 0) AS total_submitted_lots,
            ROUND(100.0 * SUM(original_qty) / SUM(SUM(original_qty)) OVER (), 2) AS pct_lots
        FROM order_lifecycle
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    results["QL4_conversion_funnel"] = df

    filled = df[df["terminal_state"].isin(["1_fully_filled", "2_partially_filled"])]
    if not filled.empty:
        pct_ord  = filled["pct_orders"].sum()
        pct_lots = filled["pct_lots"].sum()
        print(f"\n  Headline: {pct_ord:.1f}% of submitted orders ({pct_lots:.1f}% of submitted lots)")
        print("  converted into at least partial trades.")
        print("  Fully filled + partially filled = the orders that found a counterparty.")
        print("  Everything else evaporated via cancellation, expiry, or remained open.")

    # Chart: horizontal bar funnel
    FUNNEL_LABELS = {
        '1_fully_filled':     'Fully Filled',
        '2_partially_filled': 'Partially Filled',
        '3_cancelled_no_fill':'Cancelled (no fill)',
        '4_expired_no_fill':  'Expired (no fill)',
        '5_other_open':       'Other / Open',
    }
    FUNNEL_COLORS = {
        '1_fully_filled':     '#55a868',
        '2_partially_filled': '#8fd8a0',
        '3_cancelled_no_fill':'#c44e52',
        '4_expired_no_fill':  '#dd8452',
        '5_other_open':       '#9e9e9e',
    }
    df_plot = df.set_index("terminal_state").reindex(
        [k for k in FUNNEL_LABELS if k in df["terminal_state"].values]
    ).reset_index()
    df_plot["label"] = df_plot["terminal_state"].map(FUNNEL_LABELS)

    fig, ax = plt.subplots(figsize=(9, 4))
    y = list(range(len(df_plot)))
    ax.barh(y, df_plot["pct_orders"],
            color=[FUNNEL_COLORS.get(c, '#9e9e9e') for c in df_plot["terminal_state"]],
            alpha=0.87)
    for i, (val, label) in enumerate(zip(df_plot["pct_orders"], df_plot["pct_lots"])):
        ax.text(val + 0.3, i, f"{val:.1f}%  ({label:.1f}% lots)", va='center', fontsize=9)
    ax.set_yticks(y)
    ax.set_yticklabels(df_plot["label"])
    ax.set_xlabel("% of Orders")
    ax.set_title("Order Conversion Funnel (% of Orders; annotation shows % of lots)")
    ax.set_xlim(0, df_plot["pct_orders"].max() * 1.35)
    plt.tight_layout()
    out_dir.mkdir(exist_ok=True)
    path = out_dir / "conversion_funnel.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"\n  Chart saved: {path}")


# ── Q-L5: Resting time vs outcome cross-analysis ──────────────────────────────

def question_l5(con: duckdb.DuckDBPyConnection, results: dict) -> None:
    print_section("Q-L5: Resting Time vs Outcome Cross-Analysis")

    lb = _lifetime_bucket_sql("lifetime_seconds")
    ts = _terminal_state_sql()
    df = con.execute(f"""
        WITH classified AS (
            SELECT
                {lb} AS bucket,
                {ts} AS terminal
            FROM order_lifecycle
        )
        SELECT
            bucket,
            COUNT(*) AS n_orders,
            ROUND(100.0 * COUNT(CASE WHEN terminal = '1_fully_filled'     THEN 1 END) / COUNT(*), 1) AS pct_fully_filled,
            ROUND(100.0 * COUNT(CASE WHEN terminal = '2_partially_filled' THEN 1 END) / COUNT(*), 1) AS pct_partial_fill,
            ROUND(100.0 * COUNT(CASE WHEN terminal = '3_cancelled_no_fill' THEN 1 END) / COUNT(*), 1) AS pct_cancelled,
            ROUND(100.0 * COUNT(CASE WHEN terminal = '4_expired_no_fill'  THEN 1 END) / COUNT(*), 1) AS pct_expired,
            ROUND(100.0 * COUNT(CASE WHEN terminal = '5_other_open'       THEN 1 END) / COUNT(*), 1) AS pct_open
        FROM classified
        WHERE bucket != '0_unknown'
        GROUP BY bucket
        ORDER BY bucket
    """).fetchdf()
    print(df.to_string(index=False))
    results["QL5_resting_vs_outcome"] = df

    # Print key observations
    if not df.empty:
        sub1s = df[df["bucket"].isin(["1_sub_0.1s", "2_0.1-1s"])]
        if not sub1s.empty:
            avg_fill = sub1s["pct_fully_filled"].mean()
            avg_canc = sub1s["pct_cancelled"].mean()
            print(f"\n  Sub-second orders (buckets 1–2):  {avg_fill:.1f}% fully filled, "
                  f"{avg_canc:.1f}% cancelled — low fill rate consistent with HFT-like probing.")
        long_resting = df[df["bucket"].isin(["6_5-30min", "7_gt30min"])]
        if not long_resting.empty:
            avg_fill = long_resting["pct_fully_filled"].mean()
            print(f"  Long-resting orders (30min+):      {avg_fill:.1f}% fully filled — "
                  "patient orders have higher fill rates (or expire waiting).")


# ── Q-L6: HFT-like share by time of day (optional) ───────────────────────────

def question_l6(con: duckdb.DuckDBPyConnection, out_dir: Path, results: dict) -> None:
    print_section("Q-L6: HFT-like Share by Time of Day")
    print(HFT_CAVEAT)

    tw = _born_time_window_sql()
    hft = _hft_flag_sql()
    df = con.execute(f"""
        SELECT
            {tw} AS time_window,
            COUNT(*) AS total_orders,
            SUM(CASE WHEN {hft} THEN 1 ELSE 0 END) AS hft_orders,
            ROUND(100.0 * SUM(CASE WHEN {hft} THEN 1 ELSE 0 END) / COUNT(*), 2) AS hft_pct
        FROM order_lifecycle
        WHERE born_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()
    print(df.to_string(index=False))
    results["QL6_hft_intraday"] = df

    # Chart: bar chart of hft_pct across windows (exclude opening/closing special buckets)
    df_cont = df[~df["time_window"].isin(["A_OPENING", "Z_CLOSING"])].copy()
    if not df_cont.empty:
        fig, ax = plt.subplots(figsize=(14, 4))
        ax.bar(df_cont["time_window"], df_cont["hft_pct"], color='#c44e52', alpha=0.80)
        ax.set_xlabel("30-min Window (born_ts)")
        ax.set_ylabel("Likely HFT-like Order %")
        ax.set_title("HFT-like Order Share by Time of Day — Continuous Session\n"
                     "NOTE: Behavioral proxy — not confirmed participant identity")
        ax.tick_params(axis='x', rotation=45)
        plt.tight_layout()
        out_dir.mkdir(exist_ok=True)
        path = out_dir / "algorithmic_activity_intraday.png"
        plt.savefig(path, dpi=300)
        plt.close()
        print(f"\n  Chart saved: {path}")

    if not df.empty:
        peak_hft = df.loc[df["hft_pct"].idxmax()]
        print(f"\n  Peak HFT-like share: {peak_hft['time_window']}  "
              f"({peak_hft['hft_pct']:.1f}%, {int(peak_hft['hft_orders']):,} orders)")
        print("  Compare with the peak order-submission window from order_flow_overview.py.")
        print("  Algorithmic activity typically concentrates near the open when price")
        print("  discovery uncertainty is highest.")


# ── Order version distribution ────────────────────────────────────────────────

def question_versions(con: duckdb.DuckDBPyConnection, results: dict) -> None:
    print_section("Order Version Distribution (qualifying improvements per order)")

    df = con.execute("""
        SELECT
            n_qualifying_improvements                                              AS versions_created,
            COUNT(*)                                                               AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)                   AS pct_of_orders
        FROM order_lifecycle
        GROUP BY 1
        ORDER BY 1
        LIMIT 15
    """).fetchdf()
    print(df.to_string(index=False))
    results["Order_Versions"] = df

    total    = int(df["n_orders"].sum())
    improved = int(df[df["versions_created"] > 0]["n_orders"].sum()) if not df.empty else 0
    print(f"\n  Orders with at least one qualifying improvement: {improved:,}  "
          f"({100.0 * improved / total:.1f}% of all orders)")
    print(f"  Each qualifying improvement = price improvement (buy raises / sell lowers)")
    print(f"  or lot increase via change_reason=5. Causes time-priority reset in BIST queue.")
    print(f"  The composite_id column in enrich_ted.py tracks this as EMIRNO_0, EMIRNO_1, etc.")


# ── Full analysis runner ──────────────────────────────────────────────────────

def run_questions(
    con: duckdb.DuckDBPyConnection,
    results: dict,
    out_dir: Path,
) -> None:
    question_l1(con, out_dir, results)
    question_l2(con, out_dir, results)
    question_l3(con, results)
    question_l4(con, out_dir, results)
    question_l5(con, results)
    question_l6(con, out_dir, results)
    question_versions(con, results)


def _write_excel(results: dict, output_path: str) -> None:
    print(f"\nWriting results to {output_path} ...")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        row = 0
        for name, frame in results.items():
            pd.DataFrame([[name.replace("_", " · ")]]).to_excel(
                writer, sheet_name="Results", startrow=row, index=False, header=False
            )
            row += 1
            frame.to_excel(writer, sheet_name="Results", startrow=row, index=False)
            row += 1 + len(frame) + 2
    print("Done.")


# ── Entry point ───────────────────────────────────────────────────────────────

def run_analysis(
    csv_path: str,
    validate_only: bool = False,
    sample_pct: float = 0.01,
    output_path: str = "lifecycle_results.xlsx",
    out_dir: Path = OUTPUT_DIR,
    use_timestamp: bool = True,
) -> None:
    con = duckdb.connect()
    results: dict[str, pd.DataFrame] = {}

    if use_timestamp:
        output_path = timestamped_path(output_path)

    print(f"\nLoading CSV: {csv_path}")
    create_orders_view(con, csv_path)

    mode = f"{sample_pct * 100:.1f}% hash-based sample" if sample_pct < 1.0 else "full dataset"
    print(f"Building per-order lifecycle table ({mode}) ...")
    build_lifecycle_table(con, sample_pct=sample_pct)

    n_orders = con.execute("SELECT COUNT(*) FROM order_lifecycle").fetchone()[0]
    print(f"  → {n_orders:,} orders in lifecycle table.")

    validate_lifecycle(con, csv_path)

    if validate_only:
        print("\n" + "=" * 64)
        print("  VALIDATION COMPLETE.")
        print("  Re-run without --validate-only (or add --full) for Q-L1 → Q-L6.")
        print("=" * 64 + "\n")
        con.close()
        return

    run_questions(con, results, out_dir)

    if output_path:
        _write_excel(results, output_path)

    con.close()
    print("\n" + "=" * 64)
    print("  Analysis complete.")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BIST order lifecycle & microstructure analysis (Q-L1 through Q-L6)"
    )
    parser.add_argument("csv_file", help="Path to semicolon-delimited order file")
    parser.add_argument("--output", "-o", default="lifecycle_results.xlsx",
                        help="Excel output base name — timestamp is added automatically (default: lifecycle_results.xlsx)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Build lifecycle table on sample, show validation, stop")
    parser.add_argument("--sample", type=float, default=0.01, metavar="FRAC",
                        help="Fraction of orders to include (hash-based; default 0.01 = 1%%)")
    parser.add_argument("--full", action="store_true",
                        help="Run on full dataset — overrides --sample")
    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"ERROR: File not found: {args.csv_file}")
        sys.exit(1)

    sample_pct = 1.0 if args.full else args.sample
    run_analysis(
        args.csv_file,
        validate_only=args.validate_only,
        sample_pct=sample_pct,
        output_path=args.output,
    )
