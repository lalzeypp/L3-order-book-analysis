#!/usr/bin/env python3
"""
Enrich the raw TED CSV with composite order ID columns.

Adds two columns to every regular order row (order_cat=1):
  sub_order_number  — 0 = original submission, 1 after first qualifying improvement, etc.
  composite_id      — EMIRNO_0, EMIRNO_1, ... string identifier

A qualifying improvement is change_reason=5 where the trader raised a buy price,
lowered a sell price, or increased lot size. Each one causes a time-priority reset
in the BIST queue, making that version effectively a new order submission.

HOW IT WORKS (to stay within RAM):
  The window functions that compute sub_order_number have to sort every row by
  EMIR NO + timestamp. Sorting all 59M rows at once exceeds available RAM.
  Instead, this script splits orders into N hash-based partitions and processes
  one at a time. Each partition has ~1/N of the orders and fits easily in RAM.
  The partitions are written as temporary Parquet files then combined at the end.

Usage:
    python3 enrich_ted.py /path/to/PP_GUNICIEMIR_20260506_E.csv
    python3 enrich_ted.py /path/to/PP_GUNICIEMIR_20260506_E.csv --output my_output.csv
    python3 enrich_ted.py /path/to/PP_GUNICIEMIR_20260506_E.csv --partitions 20
"""

import os
import sys
import argparse
import duckdb
from pathlib import Path

from ted_common import NORMAL_ORDER_CAT

PARTS_DIR = "/private/tmp/duckdb_enrich_parts"
SPILL_DIR = "/private/tmp/duckdb_enrich_spill"


def _connect() -> duckdb.DuckDBPyConnection:
    os.makedirs(SPILL_DIR, exist_ok=True)
    return duckdb.connect(config={
        "memory_limit":             "6GB",
        "temp_directory":           SPILL_DIR,
        "preserve_insertion_order": False,
        "threads":                  4,
    })


def _process_partition(con: duckdb.DuckDBPyConnection, csv_path: str,
                        part_idx: int, n_parts: int, part_path: str) -> None:
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    *,
                    TRY_CAST("EMIR KATEGORISI"           AS INTEGER)   AS _order_cat,
                    TRY_CAST("EMIR DEGISIKLIK SEBEBI"    AS INTEGER)   AS _change_reason,
                    TRY_CAST("EMIR GIRIS TARIHI"         AS TIMESTAMP) AS _entry_ts,
                    TRY_CAST("EMIR DEGISTIRILME TARIHI"  AS TIMESTAMP) AS _change_ts,
                    TRY_CAST("FIYAT"                     AS DOUBLE)    AS _price,
                    TRY_CAST("EMIR MIKTARI"              AS DOUBLE)    AS _qty
                FROM read_csv(
                    '{csv_path}',
                    delim=';', header=true, ignore_errors=true, parallel=true
                )
                WHERE hash("EMIR NO") % {n_parts} = {part_idx}
                  AND TRY_CAST("EMIR KATEGORISI" AS INTEGER) = {NORMAL_ORDER_CAT}
            ),
            with_lag AS (
                SELECT *,
                    LAG(_price) OVER (
                        PARTITION BY "EMIR NO" ORDER BY _change_ts, _entry_ts
                    ) AS _prev_price,
                    LAG(_qty) OVER (
                        PARTITION BY "EMIR NO" ORDER BY _change_ts, _entry_ts
                    ) AS _prev_qty
                FROM raw
            ),
            with_qualifies AS (
                SELECT *,
                    CASE
                        WHEN _change_reason = 5 AND _prev_price IS NOT NULL
                         AND (
                             ("ALIS_SATIS" = 'A' AND _price > _prev_price) OR
                             ("ALIS_SATIS" = 'S' AND _price < _prev_price) OR
                             (_qty > _prev_qty)
                         )
                        THEN 1 ELSE 0
                    END AS _qualifies
                FROM with_lag
            ),
            with_sub AS (
                SELECT *,
                    SUM(_qualifies) OVER (
                        PARTITION BY "EMIR NO"
                        ORDER BY _change_ts, _entry_ts
                        ROWS UNBOUNDED PRECEDING
                    ) AS sub_order_number
                FROM with_qualifies
            )
            SELECT
                * EXCLUDE (
                    _order_cat, _change_reason, _entry_ts, _change_ts,
                    _price, _qty, _prev_price, _prev_qty, _qualifies
                ),
                sub_order_number,
                "EMIR NO" || '_' || CAST(sub_order_number AS INTEGER) AS composite_id
            FROM with_sub
        ) TO '{part_path}' (FORMAT PARQUET)
    """)


def enrich(csv_path: str, output_path: str, n_partitions: int = 10) -> None:
    os.makedirs(PARTS_DIR, exist_ok=True)

    print(f"\nSource     : {csv_path}")
    print(f"Output     : {output_path}")
    print(f"Partitions : {n_partitions}  (each processes ~1/{n_partitions} of all orders)")
    print(f"\nEach partition reads the full CSV but only sorts its share of orders.")
    print(f"Expect 2-4 minutes per partition + a final combine step.\n")

    con = _connect()
    part_files = []

    for i in range(n_partitions):
        part_path = f"{PARTS_DIR}/part_{i:02d}.parquet"
        part_files.append(part_path)
        print(f"  Partition {i+1:>2}/{n_partitions} ...", end=" ", flush=True)
        _process_partition(con, csv_path, i, n_partitions, part_path)
        size_mb = Path(part_path).stat().st_size / 1_048_576
        print(f"done  ({size_mb:.0f} MB)")

    print(f"\nCombining {n_partitions} partitions into final CSV ...")
    glob = f"{PARTS_DIR}/part_*.parquet"
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob}')
        ) TO '{output_path}' (FORMAT CSV, HEADER true, DELIMITER ';')
    """)

    for f in part_files:
        try:
            os.remove(f)
        except OSError:
            pass

    con.close()

    size_gb = Path(output_path).stat().st_size / 1_073_741_824
    print(f"\nDone.")
    print(f"Enriched file : {output_path}  ({size_gb:.1f} GB)")
    print(f"\nOpen in Excel: Data → From Text/CSV → set delimiter to semicolon.")
    print(f"Note: Excel shows only the first ~1 million rows. The file has ~45M rows.")
    print(f"For full-file analysis, use DuckDB to query it directly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add sub_order_number and composite_id to TED CSV (hash-partitioned, RAM-safe)"
    )
    parser.add_argument("csv_file", help="Path to PP_GUNICIEMIR CSV file")
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output CSV path (default: same folder as input, _enriched.csv)",
    )
    parser.add_argument(
        "--partitions", type=int, default=10, metavar="N",
        help="Number of hash partitions (default: 10). Increase to 20 if still OOM.",
    )
    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"ERROR: File not found: {args.csv_file}")
        sys.exit(1)

    if args.output:
        output_path = args.output
    else:
        p = Path(args.csv_file)
        output_path = str(p.parent / f"{p.stem}_enriched.csv")

    enrich(args.csv_file, output_path, n_partitions=args.partitions)
