"""CLI interface for IBD RS Rating."""

import argparse
import logging
from datetime import datetime

from .config import DATA_DIR, UNIVERSE_FLOOR
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
    if db.get_oldest_rs_date(conn) is not None and not getattr(args, "force_full", False):
        print(
            "Refusing init: database already has RS history; pass --force-full to "
            "rebuild from scratch (only safe with full price history), or use "
            "`recalc --from/--to` to fix ratings."
        )
        conn.close()
        raise SystemExit(1)

    print("Step 1/4: Fetching ticker list from Finviz...")
    universe, conn = tickers_mod.refresh_universe(conn)
    print(f"  Found {len(universe.tickers)} tickers")
    if not universe.trusted:
        print(f"  ERROR: {universe.reason}")
        print("  Refusing to init from an untrusted universe.")
        conn.close()
        raise SystemExit(1)
    ticker_list = universe.tickers

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
    count = rs.calculate_and_store(
        conn,
        recalc_all=True,
        active_universe=ticker_list,
        force_full=args.force_full,
    )
    print(f"  Computed {count} RS records")

    conn.close()
    print("\nInit complete!")
    cmd_status(args)


def _format_ratio(ratio):
    if ratio is None:
        return "unknown"
    return f"{ratio:.1%}"


def _format_count(count):
    if count is None:
        return "unknown"
    return str(count)


def _format_coverage(coverage, universe_size):
    if not universe_size:
        return f"{coverage}/unknown"
    return f"{coverage}/{universe_size}"


def _print_completeness_report(report):
    universe_size = report["universe_size"]
    coverage_ratio = _format_ratio(report["coverage_ratio"])
    rating_ratio = (
        report["rating_coverage"] / universe_size
        if universe_size
        else None
    )
    result = "PASS" if report["is_complete"] else "FAIL"

    print(f"  Latest trading day: {report['latest_date'] or 'none'}")
    print(f"  Universe size: {universe_size or 'unknown'}")
    print(f"  Close coverage: {_format_coverage(report['close_coverage'], universe_size)} ({coverage_ratio})")
    print(f"  Missing close count: {_format_count(report['missing_close_count'])}")
    print(f"  RS rating coverage: {_format_coverage(report['rating_coverage'], universe_size)} ({_format_ratio(rating_ratio)})")
    print(f"  Missing RS rating count: {_format_count(report['missing_rating_count'])}")
    print(f"  Threshold: {report['threshold']:.1%}")
    print(f"  Result: {result} ({report['reason']})")


def cmd_update(args):
    conn = db.get_connection()
    db.init_db(conn)

    print("Step 1/6: Loading cached ticker list...")
    universe, conn = tickers_mod.get_cached_universe(conn)
    ticker_list = universe.tickers
    print(f"  {len(ticker_list)} tickers")
    if not universe.trusted:
        print(f"  WARNING: universe fetch degraded: {universe.reason}")

    print("Step 2/6: Downloading new price data...")
    conn = db.reconnect(conn)
    failed = prices.download_update(ticker_list, conn)
    if failed:
        print(f"  Warning: {len(failed)} tickers failed")

    print("Step 3/6: Checking for stock splits...")
    flagged = splits.detect_anomalous_changes(conn)
    if flagged:
        repaired = splits.verify_and_repair(conn, flagged)
        print(f"  Repaired {len(repaired)} tickers")
    else:
        print("  No anomalies detected")

    print("Step 4/6: Calculating RS ratings...")
    conn = db.reconnect(conn)
    count = rs.calculate_and_store(conn, recalc_all=False, active_universe=ticker_list)
    print(f"  Computed {count} RS records")

    print("Step 5/6: Pruning old close prices...")
    pruned = db.prune_old_close(conn)
    print(f"  Pruned {pruned} old close records")

    print("Step 6/6: Latest trading day completeness...")
    completeness = db.check_latest_trading_day_completeness(
        conn, ticker_list, min_universe_size=UNIVERSE_FLOOR,
    )
    _print_completeness_report(completeness)

    conn.close()
    if not universe.trusted or not completeness["is_complete"]:
        raise SystemExit(1)
    print("\nUpdate complete!")


def cmd_refresh_universe(args):
    conn = db.get_connection()
    db.init_db(conn)

    universe, conn = tickers_mod.refresh_universe(conn)
    print(f"Universe: {len(universe.tickers)} tickers (trusted={universe.trusted})")
    if not universe.trusted:
        print(f"  ERROR: {universe.reason}")

    conn.close()
    if not universe.trusted:
        raise SystemExit(1)


def cmd_recalc(args):
    from_date = args.from_date
    to_date = args.to_date
    force_full = args.force_full
    conn = db.get_connection()
    try:
        if force_full and (from_date or to_date):
            print("ERROR: --from/--to and --force-full are mutually exclusive.")
            raise SystemExit(1)

        if from_date or to_date:
            if not from_date or not to_date:
                print("ERROR: --from and --to must be provided together.")
                raise SystemExit(1)
            try:
                parsed_from = datetime.strptime(from_date, "%Y-%m-%d")
                parsed_to = datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                print("ERROR: --from and --to must be valid dates in YYYY-MM-DD format.")
                raise SystemExit(1)
            if parsed_from > parsed_to:
                print("ERROR: --from date must be on or before --to date.")
                raise SystemExit(1)

            print(f"Backfilling RS ratings from {from_date} to {to_date}...")
            count = rs.backfill_ratings(conn, from_date, to_date)
            if count == 0:
                print("No stored rs_raw in [from,to] to re-rank.")
                raise SystemExit(1)
            print(f"Updated {count} RS rating records")
        elif force_full:
            print("Recalculating all RS ratings...")
            count = rs.calculate_and_store(
                conn,
                recalc_all=True,
                force_full=True,
            )
            print(f"Computed {count} RS records")
        else:
            print(
                "ERROR: choose safe rating-only `recalc --from <date> --to <date>` "
                "or guarded `recalc --force-full`."
            )
            raise SystemExit(1)
    finally:
        conn.close()


def cmd_top(args):
    conn = db.get_connection()
    latest_date = db.get_latest_rated_date(conn)
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
    latest_date = db.get_latest_rated_date(conn)
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

    p_init = subparsers.add_parser("init", help="Initial setup: download data and compute RS")
    p_init.add_argument(
        "--force-full",
        dest="force_full",
        action="store_true",
        help="allow a guarded full RS recompute",
    )
    subparsers.add_parser("update", help="Daily update: new prices + RS recalc")
    subparsers.add_parser(
        "refresh-universe",
        help="Refresh the validated weekly Finviz universe cache",
    )
    p_recalc = subparsers.add_parser("recalc", help="Recalculate RS from existing prices")
    p_recalc.add_argument("--from", dest="from_date", help="first date to re-rank (YYYY-MM-DD)")
    p_recalc.add_argument("--to", dest="to_date", help="last date to re-rank (YYYY-MM-DD)")
    p_recalc.add_argument(
        "--force-full",
        dest="force_full",
        action="store_true",
        help="allow a guarded full RS recompute",
    )

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
        "init": cmd_init,
        "update": cmd_update,
        "refresh-universe": cmd_refresh_universe,
        "recalc": cmd_recalc,
        "top": cmd_top,
        "lookup": cmd_lookup,
        "status": cmd_status,
        "export": cmd_export,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
