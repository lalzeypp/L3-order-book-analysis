#!/usr/bin/env python3
"""
Order modification validator — sub_order_number + composite ID logic check.

For each EMIR NO, qualifying modifications (change_reason=5 / ReplacedByUser) that
represent a price improvement OR lot increase reset the order's BIST time-priority
and are treated as a new version of the order.

Each version gets a composite ID: EMIRNO_0 (original), EMIRNO_1 (first improvement),
EMIRNO_2 (second improvement), etc.

Qualification rules (either condition alone is sufficient):
  PRICE IMPROVEMENT:
    buy  order (ALIS_SATIS='A'): new price > prev price  (willing to pay more)
    sell order (ALIS_SATIS='S'): new price < prev price  (willing to accept less)
  LOT INCREASE:
    new qty > prev qty

Comparison is always against the IMMEDIATELY PRECEDING row of the same EMIR NO.
Non-qualifying reason=5 rows do NOT create a new version.

Usage:
    python3 order_modification_validator.py /path/to/PP_GUNICIEMIR_...csv
    python3 order_modification_validator.py /path/to/PP_GUNICIEMIR_...csv --output modification_validation.xlsx
    python3 order_modification_validator.py /path/to/PP_GUNICIEMIR_...csv --sample 0.05
"""

import sys
import argparse
import duckdb
import pandas as pd
from pathlib import Path
from ted_common import create_orders_view, print_section, NORMAL_ORDER_CAT, timestamped_path

REPLACED_BY_USER = 5

REASON_LABELS = {
    3:  "TRADE",
    5:  "REPLACED",
    6:  "NEW",
    1:  "CANCEL_USER",
    9:  "CANCEL_SYS",
    10: "CANCEL_BEHALF",
    13: "ICE_REFRESH",
    15: "CANCEL_LIMIT",
    19: "EXPIRED",
    20: "CANCEL_ISS",
    34: "CANCEL_AUCTION",
    41: "CANCEL_MM_DELTA",
    42: "CANCEL_MM_ABS",
    43: "CROSSING_DEL",
}


def _reason_label(code) -> str:
    try:
        return REASON_LABELS.get(int(code), f"reason_{int(code)}")
    except (TypeError, ValueError):
        return str(code)


def _improvement_label(row) -> str:
    """Describe what changed on this row vs the previous row."""
    if int(row["change_reason"]) != REPLACED_BY_USER:
        return "-"
    if pd.isna(row["prev_price"]):
        return "first row — nothing to compare"

    side = str(row["side"])
    price      = float(row["price"])      if pd.notna(row["price"])      else None
    prev_price = float(row["prev_price"]) if pd.notna(row["prev_price"]) else None
    qty        = float(row["qty"])        if pd.notna(row["qty"])        else None
    prev_qty   = float(row["prev_qty"])   if pd.notna(row["prev_qty"])   else None

    good, bad = [], []

    if price is not None and prev_price is not None:
        diff = price - prev_price
        if abs(diff) > 1e-9:
            sign = "+" if diff > 0 else ""
            if (side == "A" and diff > 0) or (side == "S" and diff < 0):
                good.append(f"price {sign}{diff:.4f} (better)")
            else:
                bad.append(f"price {sign}{diff:.4f} (worse)")

    if qty is not None and prev_qty is not None:
        diff = qty - prev_qty
        if abs(diff) > 1e-9:
            sign = "+" if diff > 0 else ""
            if diff > 0:
                good.append(f"qty {sign}{int(diff)}")
            else:
                bad.append(f"qty {int(diff)} (reduced)")

    if good:
        result = "IMPROVED: " + ", ".join(good)
        if bad:
            result += "  |  also: " + ", ".join(bad)
        return result
    elif bad:
        return "no improvement — " + ", ".join(bad)
    return "no change"


# ── Core CTE chain ────────────────────────────────────────────────────────────

def _sub_order_cte_sql(filter_clause: str = "") -> str:
    """
    CTE chain: base → with_lag → with_qualifies → with_sub_order.
    with_sub_order adds sub_order_number and composite_id to every row.
    """
    extra = f"AND {filter_clause}" if filter_clause else ""
    return f"""
        WITH base AS (
            SELECT
                "EMIR NO",
                "ALIS_SATIS",
                "ISLEM KODU",
                change_reason,
                entry_ts,
                change_ts,
                price,
                qty,
                remaining
            FROM orders
            WHERE order_cat = {NORMAL_ORDER_CAT}
            {extra}
        ),
        with_lag AS (
            SELECT
                *,
                LAG(price) OVER (PARTITION BY "EMIR NO" ORDER BY change_ts, entry_ts) AS prev_price,
                LAG(qty)   OVER (PARTITION BY "EMIR NO" ORDER BY change_ts, entry_ts) AS prev_qty
            FROM base
        ),
        with_qualifies AS (
            SELECT
                *,
                CASE
                    WHEN change_reason = {REPLACED_BY_USER}
                         AND prev_price IS NOT NULL
                         AND (
                             ("ALIS_SATIS" = 'A' AND price > prev_price) OR
                             ("ALIS_SATIS" = 'S' AND price < prev_price) OR
                             (qty > prev_qty)
                         )
                    THEN 1
                    ELSE 0
                END AS qualifies
            FROM with_lag
        ),
        with_sub_order AS (
            SELECT
                *,
                SUM(qualifies) OVER (
                    PARTITION BY "EMIR NO"
                    ORDER BY change_ts, entry_ts
                    ROWS UNBOUNDED PRECEDING
                ) AS sub_order_number
            FROM with_qualifies
        )
    """


# ── Step 1: find sample orders ────────────────────────────────────────────────

def find_sample_orders(con: duckdb.DuckDBPyConnection, min_replacements: int = 3) -> list:
    df = con.execute(f"""
        SELECT "EMIR NO"
        FROM orders
        WHERE order_cat = {NORMAL_ORDER_CAT}
          AND change_reason = {REPLACED_BY_USER}
        GROUP BY "EMIR NO"
        HAVING COUNT(*) >= {min_replacements}
        LIMIT 5
    """).fetchdf()
    return df["EMIR NO"].tolist() if not df.empty else []


# ── Step 2a: event-by-event detail ───────────────────────────────────────────

def get_order_detail(con: duckdb.DuckDBPyConnection, order_ids: list) -> pd.DataFrame:
    ids_sql = ", ".join(f"'{oid}'" for oid in order_ids)
    cte = _sub_order_cte_sql(f'"EMIR NO" IN ({ids_sql})')

    raw = con.execute(f"""
        {cte}
        SELECT
            "EMIR NO"                                                           AS order_id,
            "ALIS_SATIS"                                                        AS side,
            "ISLEM KODU"                                                        AS stock,
            change_reason,
            change_ts,
            ROUND(price, 4)                                                     AS price,
            ROUND(prev_price, 4)                                                AS prev_price,
            ROUND(qty, 0)                                                       AS qty,
            ROUND(prev_qty, 0)                                                  AS prev_qty,
            remaining,
            qualifies,
            sub_order_number,
            "EMIR NO" || '_' || CAST(sub_order_number AS INTEGER)              AS composite_id
        FROM with_sub_order
        ORDER BY "EMIR NO", change_ts, entry_ts
    """).fetchdf()

    if raw.empty:
        return raw

    raw["event"]       = raw["change_reason"].apply(_reason_label)
    raw["improvement"] = [_improvement_label(row) for _, row in raw.iterrows()]
    raw["qualified"]   = raw["qualifies"].apply(lambda x: "YES" if x == 1 else "")
    raw["side_full"]   = raw["side"].apply(lambda s: "BUY (alis)" if s == "A" else "SELL (satis)")
    return raw


def print_order_detail(df: pd.DataFrame, order_ids: list) -> None:
    display_cols = [
        "composite_id", "event", "change_ts",
        "price", "prev_price", "qty", "prev_qty",
        "qualified", "improvement",
    ]
    for oid in order_ids:
        subset = df[df["order_id"] == oid].reset_index(drop=True)
        if subset.empty:
            continue
        side_full  = subset["side_full"].iloc[0]
        stock      = subset["stock"].iloc[0]
        n_versions = int(subset["sub_order_number"].max()) + 1
        n_improved = int(subset["qualifies"].sum())

        print(f"\n{'─' * 84}")
        print(f"  Order    : {oid}   Stock: {stock}   Direction: {side_full}")
        print(f"  Versions : {n_versions}  (original + {n_improved} qualifying improvement(s))")
        print()
        print(subset[display_cols].to_string(index=False))

    print()
    print("  How to read the composite_id column:")
    print("    EMIRNO_0  — original order as submitted (change_reason=6)")
    print("    EMIRNO_1  — first version after a qualifying improvement (loses time priority)")
    print("    EMIRNO_2  — second improvement, etc.")
    print("    Non-qualifying REPLACED rows keep the SAME composite_id (no priority loss,")
    print("    no increment) — e.g. a price worsening or size reduction.")


# ── Step 2b: version summary ──────────────────────────────────────────────────

def get_version_summary(con: duckdb.DuckDBPyConnection, order_ids: list) -> pd.DataFrame:
    """
    One row per composite version per order.
    Shows: price at version start, duration until next version (or end of order),
    and what triggered each version boundary.
    """
    ids_sql = ", ".join(f"'{oid}'" for oid in order_ids)
    cte = _sub_order_cte_sql(f'"EMIR NO" IN ({ids_sql})')

    raw = con.execute(f"""
        {cte},
        -- keep only the row that STARTS each version:
        --   sub_order_number=0 → the NEW row (reason=6)
        --   sub_order_number>0 → the qualifying REPLACED row (qualifies=1)
        version_starts AS (
            SELECT
                "EMIR NO"                                                  AS order_id,
                "ISLEM KODU"                                               AS stock,
                "ALIS_SATIS"                                               AS side,
                sub_order_number,
                "EMIR NO" || '_' || CAST(sub_order_number AS INTEGER)     AS composite_id,
                change_ts                                                   AS version_start_ts,
                ROUND(price, 4)                                             AS version_price,
                ROUND(qty, 0)                                               AS version_qty
            FROM with_sub_order
            WHERE qualifies = 1
               OR (change_reason = 6 AND sub_order_number = 0)
        )
        SELECT
            composite_id,
            stock,
            side,
            version_start_ts,
            version_price,
            version_qty,
            LEAD(version_start_ts) OVER (
                PARTITION BY order_id ORDER BY sub_order_number
            )                                                               AS version_end_ts,
            ROUND(EXTRACT(EPOCH FROM (
                LEAD(version_start_ts) OVER (
                    PARTITION BY order_id ORDER BY sub_order_number
                ) - version_start_ts
            )), 2)                                                          AS version_duration_s
        FROM version_starts
        ORDER BY order_id, sub_order_number
    """).fetchdf()

    raw["version_end_ts"]    = raw["version_end_ts"].fillna("(last version — see order events)")
    raw["version_duration_s"] = raw["version_duration_s"].apply(
        lambda x: f"{x:.2f}s" if pd.notna(x) and isinstance(x, float) else "(ongoing)"
    )
    return raw


def print_version_summary(df: pd.DataFrame) -> None:
    print_section("Step 2b: Version summary — one row per composite order ID")
    print("  Each row = one 'life' of the order at a specific price/qty.")
    print("  version_duration_s = how long this version sat in the book before")
    print("  the next improvement (or end of order).")
    print()

    display_cols = [
        "composite_id", "stock", "side",
        "version_start_ts", "version_price", "version_qty",
        "version_end_ts", "version_duration_s",
    ]
    print(df[display_cols].to_string(index=False))


# ── Step 3: distribution + per-stock breakdown ────────────────────────────────

def get_distribution(con: duckdb.DuckDBPyConnection, sample_n: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    cte = _sub_order_cte_sql(f'hash("EMIR NO") % {sample_n} = 0')

    dist = con.execute(f"""
        {cte}
        , per_order AS (
            SELECT
                "EMIR NO",
                MAX("ISLEM KODU") AS stock,
                MAX(sub_order_number) AS max_version
            FROM with_sub_order
            GROUP BY "EMIR NO"
        )
        SELECT
            max_version                                                    AS times_improved,
            COUNT(*)                                                       AS n_orders,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)            AS pct_of_orders
        FROM per_order
        GROUP BY max_version
        ORDER BY max_version
        LIMIT 15
    """).fetchdf()

    by_stock = con.execute(f"""
        {cte}
        , per_order AS (
            SELECT
                "EMIR NO",
                MAX("ISLEM KODU") AS stock,
                MAX(sub_order_number) AS max_version
            FROM with_sub_order
            GROUP BY "EMIR NO"
        )
        SELECT
            stock,
            COUNT(*)                                                                    AS total_orders,
            SUM(CASE WHEN max_version > 0 THEN 1 ELSE 0 END)                          AS orders_with_improvements,
            ROUND(100.0 * SUM(CASE WHEN max_version > 0 THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                                        AS pct_improved,
            ROUND(AVG(CASE WHEN max_version > 0 THEN max_version END), 2)             AS avg_versions_when_improved
        FROM per_order
        GROUP BY stock
        HAVING COUNT(*) >= 10
        ORDER BY orders_with_improvements DESC
        LIMIT 20
    """).fetchdf()

    return dist, by_stock


def print_distribution(dist: pd.DataFrame, by_stock: pd.DataFrame, sample_n: int) -> None:
    pct_label = f"~{100/sample_n:.0f}%" if sample_n > 1 else "100%"
    print_section(f"Step 3: Version count distribution ({pct_label} hash-based sample)")

    total    = int(dist["n_orders"].sum())
    improved = int(dist[dist["times_improved"] > 0]["n_orders"].sum()) if not dist.empty else 0
    pct_impr = 100.0 * improved / total if total else 0.0

    print(dist.to_string(index=False))
    print(f"\n  Orders in sample                  : {total:,}")
    print(f"  Orders with >= 1 improvement       : {improved:,}  ({pct_impr:.2f}%)")
    print(f"\n  Stocks with most actively repriced orders:")
    print(by_stock.to_string(index=False))
    print(f"\n  Interpretation:")
    print(f"    Each 'improvement' = trader re-competed by posting a better price")
    print(f"    or larger size. In BIST's price-time priority system, this loses")
    print(f"    the order's queue position and restarts it as a new submission.")
    print(f"    A stock with high pct_improved has participants who actively manage")
    print(f"    their resting orders rather than placing and waiting.")


# ── Excel export ──────────────────────────────────────────────────────────────

def write_excel(
    detail_df: pd.DataFrame,
    version_df: pd.DataFrame,
    dist_df: pd.DataFrame,
    by_stock_df: pd.DataFrame,
    output_path: str,
) -> None:
    print(f"\nWriting results to {output_path} ...")

    # Sheet 1: event-by-event detail
    event_export = detail_df[[
        "order_id", "composite_id", "stock", "side_full",
        "event", "change_ts", "price", "prev_price",
        "qty", "prev_qty", "remaining", "qualified", "improvement",
    ]].rename(columns={
        "order_id":    "EMIR NO",
        "composite_id":"Composite ID",
        "stock":       "Stock",
        "side_full":   "Direction",
        "event":       "Event",
        "change_ts":   "Timestamp",
        "price":       "Price",
        "prev_price":  "Prev Price",
        "qty":         "Qty (lots)",
        "prev_qty":    "Prev Qty",
        "remaining":   "Remaining",
        "qualified":   "Qualified?",
        "improvement": "What Changed",
    })

    # Sheet 2: version summary
    version_export = version_df[[
        "composite_id", "stock", "side",
        "version_start_ts", "version_price", "version_qty",
        "version_end_ts", "version_duration_s",
    ]].rename(columns={
        "composite_id":      "Composite ID",
        "stock":             "Stock",
        "side":              "Side",
        "version_start_ts":  "Version Start",
        "version_price":     "Price at Start",
        "version_qty":       "Qty at Start",
        "version_end_ts":    "Version End (next improvement)",
        "version_duration_s":"Duration in Book",
    })

    # Sheet 3: distribution with meaning
    def _meaning(n):
        if n == 0:
            return "Order was never improved — placed and left as-is, then cancelled/filled/expired"
        if n == 1:
            return "One improvement: trader re-competed once, lost time priority once"
        if n == 2:
            return "Two improvements: trader actively managed this order twice"
        return f"{int(n)} improvements — highly active order management"

    dist_export = dist_df.rename(columns={
        "times_improved": "Versions Created",
        "n_orders":       "Order Count",
        "pct_of_orders":  "% of Orders",
    }).copy()
    dist_export["Meaning"] = dist_df["times_improved"].apply(_meaning)

    # Sheet 4: per-stock
    stock_export = by_stock_df.rename(columns={
        "stock":                      "Stock",
        "total_orders":               "Total Orders (sample)",
        "orders_with_improvements":   "Orders Improved",
        "pct_improved":               "% Improved",
        "avg_versions_when_improved": "Avg Versions (when >0)",
    })

    # Sheet 5: legend
    legend_rows = [
        ("Composite ID",       "EMIRNO_0 = original order. EMIRNO_1 = after first qualifying improvement. Each increment = lost time priority in BIST queue."),
        ("Qualified? = YES",   "This REPLACED event was a price improvement OR lot increase. Sub-order counter incremented. New time-priority position."),
        ("Qualified? = (blank)","REPLACED event that worsened price or reduced size. Counter stays the same. Time priority NOT affected."),
        ("Event: NEW(6)",      "Order first placed. Always Composite ID _0."),
        ("Event: REPLACED(5)", "Trader modified the order. May or may not be qualifying."),
        ("Event: TRADE(3)",    "Partial or full fill. Composite ID unchanged."),
        ("Event: CANCEL_*(1/9/etc)", "Order cancelled. Composite ID unchanged."),
        ("Event: EXPIRED(19)", "Time-in-force exhausted. Composite ID unchanged."),
        ("Price improvement",  "BUY: new price > prev (willing to pay more). SELL: new price < prev (willing to accept less)."),
        ("Lot increase",       "new qty > prev qty — trader increased their order size."),
        ("Version Duration",   "Time this version of the order spent in the book before the next improvement replaced it."),
        ("Hash sample",        "Distribution and By_Stock sheets use ~1% of orders for speed. All events of sampled orders are included."),
    ]
    legend_export = pd.DataFrame(legend_rows, columns=["Term / Column", "Explanation"])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        event_export.to_excel(writer,   sheet_name="Event_Detail",   index=False)
        version_export.to_excel(writer, sheet_name="Version_Summary", index=False)
        dist_export.to_excel(writer,    sheet_name="Distribution",    index=False)
        stock_export.to_excel(writer,   sheet_name="By_Stock",        index=False)
        legend_export.to_excel(writer,  sheet_name="Legend",          index=False)

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max((len(str(cell.value)) for cell in col if cell.value), default=10)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 70)

    print(f"  Sheets: Event_Detail | Version_Summary | Distribution | By_Stock | Legend")
    print(f"  Done.")


# ── Lookup mode (terminal browse without opening Excel) ───────────────────────

def run_lookup(csv_path: str, emir_no: str) -> None:
    """Print the full version history for one specific EMIR NO."""
    con = duckdb.connect()
    create_orders_view(con, csv_path)

    detail_df = get_order_detail(con, [emir_no])
    if detail_df.empty:
        print(f"\n  Order '{emir_no}' not found in the file.")
        con.close()
        return

    print_order_detail(detail_df, [emir_no])
    version_df = get_version_summary(con, [emir_no])
    print_version_summary(version_df)
    con.close()


# ── Full validation run ───────────────────────────────────────────────────────

def run_validation(csv_path: str, output_path: str, sample_frac: float) -> None:
    con = duckdb.connect()
    print(f"\nConnecting to: {csv_path}")
    create_orders_view(con, csv_path)

    print_section("Step 1: Finding orders with multiple ReplacedByUser (change_reason=5) events")
    order_ids = find_sample_orders(con, min_replacements=3)
    if not order_ids:
        print("  No orders with >= 3 replacements found. Trying >= 2 ...")
        order_ids = find_sample_orders(con, min_replacements=2)
    if not order_ids:
        print("  No orders with multiple change_reason=5 rows found.")
        print("  The full CSV is required — a truncated file will only have NEW (reason=6) rows.")
        con.close()
        return
    print(f"  Found {len(order_ids)} sample orders: {order_ids}")

    print_section("Step 2: Event-by-event history with composite order IDs")
    detail_df = get_order_detail(con, order_ids)
    print_order_detail(detail_df, order_ids)

    version_df = get_version_summary(con, order_ids)
    print_version_summary(version_df)

    sample_n = max(1, round(1.0 / sample_frac))
    dist_df, by_stock_df = get_distribution(con, sample_n)
    print_distribution(dist_df, by_stock_df, sample_n)

    output_path = timestamped_path(output_path)
    write_excel(detail_df, version_df, dist_df, by_stock_df, output_path)

    con.close()
    print("\n  Validation complete.")
    print("  To look up any specific order later:")
    print("    python3 order_modification_validator.py <CSV> --lookup <EMIR_NO>\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate sub_order_number and composite order ID (EMIRNO_0, EMIRNO_1, ...) logic"
    )
    parser.add_argument("csv_file", help="Path to PP_GUNICIEMIR CSV file (full file recommended)")
    parser.add_argument(
        "--output", "-o",
        default="modification_validation.xlsx",
        help="Excel output base name — timestamp added automatically",
    )
    parser.add_argument(
        "--sample", type=float, default=0.01, metavar="FRAC",
        help="Fraction for distribution stats (default: 0.01 = 1%%)",
    )
    parser.add_argument(
        "--lookup", metavar="EMIR_NO",
        help="Print full version history for a specific order ID and exit (no Excel written)",
    )
    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"ERROR: File not found: {args.csv_file}")
        sys.exit(1)

    if args.lookup:
        run_lookup(args.csv_file, args.lookup)
    else:
        run_validation(args.csv_file, args.output, args.sample)
