"""CLI interface for IBD RS Rating."""

import argparse
import logging

from .config import DATA_DIR
from . import db
from . import tickers as tickers_mod
from . import prices
from . import splits
from . import rs


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_init(args):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = db.get_connection()
    db.init_db(conn)

    print("Step 1/4: Fetching ticker list from Finviz...")
    ticker_list = tickers_mod.fetch_ticker_list(conn, force_refresh=True)
    print(f"  Found {len(ticker_list)} tickers")

    print(f"Step 2/4: Downloading 2-year price history ({len(ticker_list)} tickers)...")
    print("  This may take 20-30 minutes.")
    failed = prices.download_initial(ticker_list, conn)
    if failed:
        print(f"  Warning: {len(failed)} tickers failed to download")

    print("Step 3/4: Checking for stock splits...")
    flagged = splits.detect_anomalous_changes(conn)
    if flagged:
        repaired = splits.verify_and_repair(conn, flagged)
        print(f"  Repaired {len(repaired)} split-affected tickers")
    else:
        print("  No anomalies detected")

    print("Step 4/4: Calculating RS ratings...")
    count = rs.calculate_and_store(conn, recalc_all=True)
    print(f"  Computed {count} RS records")

    conn.close()
    print("\nInit complete!")
    cmd_status(args)


def cmd_update(args):
    conn = db.get_connection()
    db.init_db(conn)

    print("Step 1/4: Fetching ticker list...")
    ticker_list = tickers_mod.fetch_ticker_list(conn)
    print(f"  {len(ticker_list)} tickers")

    print("Step 2/4: Downloading new price data...")
    failed = prices.download_update(ticker_list, conn)
    if failed:
        print(f"  Warning: {len(failed)} tickers failed")

    print("Step 3/4: Checking for stock splits...")
    flagged = splits.detect_anomalous_changes(conn)
    if flagged:
        repaired = splits.verify_and_repair(conn, flagged)
        print(f"  Repaired {len(repaired)} tickers")
    else:
        print("  No anomalies detected")

    print("Step 4/4: Calculating RS ratings...")
    count = rs.calculate_and_store(conn, recalc_all=False)
    print(f"  Computed {count} RS records")

    conn.close()
    print("\nUpdate complete!")


def cmd_recalc(args):
    conn = db.get_connection()
    print("Recalculating all RS ratings...")
    count = rs.calculate_and_store(conn, recalc_all=True)
    print(f"Computed {count} RS records")
    conn.close()


def cmd_top(args):
    conn = db.get_connection()
    latest_date = db.get_latest_rs_date(conn)
    if not latest_date:
        print("No RS data available. Run 'init' first.")
        conn.close()
        return

    n = args.n or 20
    rows = db.get_top_rs(conn, latest_date, n)
    refs = db.get_reference_rs(conn, latest_date)

    print(f"\nIBD RS Ratings — {latest_date}")
    print("=" * 50)
    print(f"{'Rank':>4}  {'Ticker':<8}  {'RS Rating':>9}  {'RS Raw':>8}")
    print("-" * 50)
    for i, (ticker, rating, raw) in enumerate(rows, 1):
        print(f"{i:>4}  {ticker:<8}  {rating:>9}  {raw:>8.4f}")

    if refs:
        print("-" * 50)
        print("Reference:")
        for ticker, raw, rating in refs:
            print(f"      {ticker:<8}  {rating or '—':>9}  {raw:>8.4f}")

    conn.close()


def cmd_lookup(args):
    conn = db.get_connection()
    days = args.days or 30
    rows = db.get_rs_history(conn, args.ticker.upper(), days)

    if not rows:
        print(f"No RS data found for {args.ticker.upper()}")
        conn.close()
        return

    print(f"\nRS History for {args.ticker.upper()}")
    print("=" * 42)
    print(f"{'Date':<12}  {'RS Raw':>8}  {'RS Rating':>9}")
    print("-" * 42)
    for date, raw, rating in rows:
        rating_str = str(rating) if rating is not None else "—"
        print(f"{date:<12}  {raw:>8.4f}  {rating_str:>9}")

    conn.close()


def cmd_status(args):
    conn = db.get_connection()
    try:
        stats = db.get_price_stats(conn)
    except Exception:
        print("No database found. Run 'init' first.")
        conn.close()
        return

    print(f"\nDatabase Status")
    print("=" * 40)
    print(f"Price records:   {stats['price_rows']:>10,}")
    print(f"Price tickers:   {stats['price_tickers']:>10,}")
    print(f"Price range:     {stats['price_min_date']} — {stats['price_max_date']}")
    print(f"RS records:      {stats['rs_rows']:>10,}")
    print(f"RS tickers:      {stats['rs_tickers']:>10,}")
    print(f"RS range:        {stats['rs_min_date']} — {stats['rs_max_date']}")
    print(f"Last update:     {stats['last_update']}")
    conn.close()


def cmd_export(args):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = db.get_connection()
    latest_date = db.get_latest_rs_date(conn)
    if not latest_date:
        print("No RS data available.")
        conn.close()
        return

    df = db.get_rs_for_export(conn, latest_date)

    outpath = DATA_DIR / f"rs_ratings_{latest_date}.csv"
    df.to_csv(outpath, index=False)
    print(f"Exported {len(df)} records to {outpath}")

    tickers_path = DATA_DIR / "tickers.csv"
    df.to_csv(tickers_path, index=False)
    print(f"Updated {tickers_path} ({len(df)} tickers)")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        prog="ibd-rs",
        description="IBD-style Relative Strength Rating calculator",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")
    subparsers = parser.add_subparsers(dest="command", help="command")

    subparsers.add_parser("init", help="Initial setup: download data and compute RS")
    subparsers.add_parser("update", help="Daily update: new prices + RS recalc")
    subparsers.add_parser("recalc", help="Recalculate RS from existing prices")

    p_top = subparsers.add_parser("top", help="Show top stocks by RS Rating")
    p_top.add_argument("n", nargs="?", type=int, default=20, help="number of stocks (default: 20)")

    p_lookup = subparsers.add_parser("lookup", help="Show RS history for a ticker")
    p_lookup.add_argument("ticker", help="ticker symbol")
    p_lookup.add_argument("--days", type=int, default=30, help="days of history (default: 30)")

    subparsers.add_parser("status", help="Show database statistics")
    subparsers.add_parser("export", help="Export latest RS ratings to CSV")

    args = parser.parse_args()
    setup_logging(args.verbose)

    commands = {
        "init": cmd_init, "update": cmd_update, "recalc": cmd_recalc,
        "top": cmd_top, "lookup": cmd_lookup, "status": cmd_status, "export": cmd_export,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
