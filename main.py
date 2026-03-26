#!/usr/bin/env python3
"""
Polymarket Analyzer — CLI Tool
================================
Commands:
  search <query>             Search for markets
  watch <query|slug>         Add a market to your watchlist
  iran                       Watch & analyze the 'US forces enter Iran by Dec 31' market
  analyze                    Run analysis on all watched markets
  predict <query|slug>       Analyze one market and store prediction
  orderbook [query] [--range 0.10]  Show bid/ask depth ±cents from mid
  resolve                    Check pending predictions against live prices
  report                     Show prediction performance report
  refine                     Refine algorithm weights from resolved predictions
  watchlist                  List your watched markets
  demo                       Run demo with sample market data (no API key needed)

  --- Multi-Market Research ---
  discover                   Auto-find ~20 US politics/Israel markets
  collect                    Snapshot prices for all watched markets
  volatility [market_id]     Show volatility metrics
  opportunities              Show mean reversion buy/sell opportunities
  signals                    Show active trading signals
  signal-report              Show trading signal performance

Usage:
  python main.py discover
  python main.py collect
  python main.py volatility
  python main.py opportunities
  python main.py signals
  python main.py signal-report
"""
import sys
import json
from datetime import datetime, timezone
import api as polyapi
import tracker
import analyzer as analyzermod
from models import Prediction
from intelligence.signals import init_db


def _separator(char="─", width=65):
    print(char * width)


def cmd_search(query: str):
    print(f"\nSearching Polymarket for: '{query}'")
    _separator()
    markets = polyapi.search_markets(query, limit=15)
    if not markets:
        print("No markets found.")
        return
    for m in markets:
        status = "CLOSED" if m.closed else ("ACTIVE" if m.active else "inactive")
        print(f"[{status}] {m.question}")
        print(f"  YES: {m.yes_price:.1%}  |  Volume: ${m.volume:,.0f}  |  Ends: {m.end_date[:10] if m.end_date else 'N/A'}")
        print(f"  Slug: {m.slug}")
        print()


def cmd_watch(query: str):
    print(f"\nFinding market: '{query}'")
    market = _find_market(query)
    if not market:
        print("Market not found. Try 'search' to see available markets.")
        return
    print(f"Found: {market.question}")
    tracker.add_to_watchlist(market)


def cmd_analyze():
    """Analyze all watched markets."""
    watchlist = tracker.load_watchlist()
    if not watchlist:
        print("\nWatchlist is empty. Use 'watch <query>' to add markets.")
        return

    weights = tracker.load_weights()
    print(f"\nAnalyzing {len(watchlist)} watched market(s)...")
    _separator()

    for item in watchlist:
        market = polyapi.get_market_by_id(item["id"])
        if not market:
            print(f"Could not fetch: {item['question']}")
            continue
        pred = analyzermod.analyze_market(market, weights)
        print(pred.reasoning)
        _separator()


def cmd_predict(query: str):
    """Analyze one market and store the prediction."""
    print(f"\nPredicting: '{query}'")
    market = _find_market(query)
    if not market:
        print("Market not found.")
        return

    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)

    print()
    print(pred.reasoning)
    _separator()

    tracker.store_prediction(pred)
    print(f"Prediction stored. YES probability: {pred.predicted_yes_prob:.1%}")
    print("Run 'resolve' later to check outcomes and score predictions.")


def cmd_resolve():
    """Check all pending predictions and resolve if market has settled."""
    print("\nChecking pending predictions...")
    _separator()
    resolved = tracker.check_and_resolve_pending()
    pending = tracker.get_pending_predictions()
    print(f"Still pending: {len(pending)}")


def cmd_report():
    """Print performance report."""
    print("\nPrediction Performance Report")
    _separator()
    report = tracker.generate_performance_report()

    print(f"Total predictions:   {report.total_predictions}")
    print(f"Resolved:            {report.resolved}")
    print(f"Pending:             {report.pending}")
    if report.resolved:
        print()
        print(f"Avg Brier Score:     {report.avg_brier_score:.4f}  (lower=better; random=0.25, perfect=0.00)")
        print(f"Calibration Error:   {report.calibration_error:.4f}  (mean |predicted - actual|)")
        print(f"Binary Accuracy:     {report.accuracy:.1%}  (correct direction)")
        print(f"Hypothetical P&L:    {report.profit_loss_units:+.2f} units (betting $1/prediction)")

        if report.best_predictions:
            print()
            print("Best predictions (lowest Brier score):")
            for p in report.best_predictions:
                print(f"  [{p['outcome']}] {p['market_question'][:55]}...")
                print(f"    Predicted: {p['predicted_yes_prob']:.1%}  Score: {p['score']:.4f}")

        if report.worst_predictions:
            print()
            print("Worst predictions:")
            for p in report.worst_predictions:
                print(f"  [{p['outcome']}] {p['market_question'][:55]}...")
                print(f"    Predicted: {p['predicted_yes_prob']:.1%}  Score: {p['score']:.4f}")


def cmd_refine():
    """Refine algorithm weights based on resolved predictions."""
    print("\nRefining algorithm weights...")
    _separator()
    predictions = tracker.load_predictions()
    current_weights = tracker.load_weights()

    print("Current weights:")
    for k, v in current_weights.items():
        print(f"  {k}: {v:.4f}")

    new_weights = analyzermod.refine_weights(predictions, current_weights)

    print("\nUpdated weights:")
    for k in new_weights:
        delta = new_weights[k] - current_weights[k]
        arrow = "↑" if delta > 0.001 else "↓" if delta < -0.001 else "→"
        print(f"  {k}: {new_weights[k]:.4f}  {arrow} ({delta:+.4f})")

    tracker.save_weights(new_weights)
    print("\nWeights saved.")


def cmd_watchlist():
    """Show current watchlist."""
    watchlist = tracker.load_watchlist()
    if not watchlist:
        print("\nWatchlist is empty.")
        return
    print(f"\nWatched markets ({len(watchlist)}):")
    _separator()
    for item in watchlist:
        print(f"  • {item['question']}")
        print(f"    ID: {item['id']}  |  Added: {item['added'][:10]}")


def cmd_orderbook(args: list):
    """
    Show detailed order book depth for a market.
    Usage: orderbook [query] [--range 0.10]
    Example: orderbook "iran december" --range 0.15
    """
    # Parse optional --range flag
    cents_range = 0.10
    query_parts = []
    i = 0
    while i < len(args):
        if args[i] == "--range" and i + 1 < len(args):
            try:
                cents_range = float(args[i + 1])
            except ValueError:
                pass
            i += 2
        else:
            query_parts.append(args[i])
            i += 1

    query = " ".join(query_parts) if query_parts else "iran december"

    print(f"\nFetching order book for: '{query}'")
    market = _find_market(query)
    if not market:
        print("Market not found.")
        return

    print(f"\n{analyzermod.analyze_orderbook_depth(market, cents_range=cents_range)}")


def cmd_watch_iran():
    """Fetch and watch the 'US forces enter Iran by December 31' market."""
    print("\nFetching 'US forces enter Iran by December 31, 2026' market...")
    _separator()

    market = polyapi.find_iran_boots_market()

    if not market:
        print("Market not found via direct ID. Trying event browse...")
        # Fallback: fetch from event listing
        try:
            import requests
            r = requests.get("https://gamma-api.polymarket.com/events/158299",
                             headers={"User-Agent": "PolymarketAnalyzer/1.0"}, timeout=15)
            event = r.json()
            for raw in event.get("markets", []):
                if "december" in raw.get("question", "").lower():
                    market = polyapi._parse_market(raw)
                    break
        except Exception as e:
            print(f"Error: {e}")

    if not market:
        print("Could not locate the market. It may have been removed or renamed.")
        return

    status = "CLOSED" if market.closed else "ACTIVE"
    print(f"[{status}] {market.question}")
    print(f"  YES: {market.yes_price:.1%}  |  Vol: ${market.volume:,.0f}  |  Ends: {market.end_date[:10] if market.end_date else 'N/A'}")
    print(f"  Liquidity: ${market.liquidity:,.0f}")
    print()

    tracker.add_to_watchlist(market)

    print("\nRunning full analysis...")
    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(market, weights)
    print()
    print(pred.reasoning)
    _separator()

    # Show order book depth
    print("\nOrder Book Depth (±10¢ from mid):")
    print(analyzermod.analyze_orderbook_depth(market, cents_range=0.10))
    _separator()

    tracker.store_prediction(pred)
    print(f"\nPrediction stored. Run 'report' to track performance over time.")


def cmd_demo():
    """Run with synthetic market data — no live API needed."""
    from models import Market
    import random

    print("\nDEMO MODE — Synthetic Market Data")
    _separator()

    demo_market = Market(
        id="demo-iran-001",
        question="Will there be US boots on the ground in Iran by December 31, 2025?",
        slug="us-boots-iran-2025",
        outcomes=["Yes", "No"],
        outcome_prices=[0.07, 0.93],
        volume=850_000,
        liquidity=45_000,
        end_date="2025-12-31T23:59:00Z",
        active=True,
        closed=False,
        description="Resolves YES if US military ground forces are confirmed in Iran before Dec 31 2025.",
        category="Politics",
        volume_24hr=12_000,
        clob_token_ids=[],
    )

    weights = tracker.load_weights()
    pred = analyzermod.analyze_market(demo_market, weights)

    print(pred.reasoning)
    _separator()

    # Simulate a historical track record
    print("\nSimulating 5 past demo predictions for performance testing...")
    demo_history = [
        ("Will Iran nuclear deal be signed by Dec 2024?", 0.25, "NO", 0.03),
        ("Will Iran launch missile strike on US base in 2024?", 0.15, "YES", 0.72),
        ("Will US impose new sanctions on Iran in Q1 2025?", 0.72, "YES", 0.85),
        ("Will Iran nuclear enrichment reach 90% in 2024?", 0.30, "NO", 0.08),
        ("Will US and Iran hold direct talks in 2025?", 0.40, "NO", 0.12),
    ]

    demo_predictions = []
    for q, pred_prob, actual_outcome, actual_price in demo_history:
        p = Prediction(
            market_id=f"demo-{hash(q) % 10000}",
            market_question=q,
            timestamp=datetime.now(timezone.utc).isoformat(),
            yes_price_at_prediction=actual_price * 0.9 + 0.05,
            predicted_yes_prob=pred_prob,
            confidence=0.6,
            reasoning="Demo prediction",
            signals={"market_price": actual_price * 0.9 + 0.05},
            outcome=actual_outcome,
            resolved_price=actual_price,
        )
        from tracker import _brier_score
        p.score = _brier_score(pred_prob, actual_outcome)
        demo_predictions.append(p)

    # Show mini report
    scores = [p.score for p in demo_predictions]
    avg = sum(scores) / len(scores)
    correct = sum(1 for p in demo_predictions
                  if (p.predicted_yes_prob > 0.5) == (p.outcome == "YES"))

    print(f"\nDemo Performance:")
    print(f"  Avg Brier Score: {avg:.4f}  (random baseline: 0.25)")
    print(f"  Accuracy:        {correct}/{len(demo_predictions)} = {correct/len(demo_predictions):.0%}")
    print(f"\nRun  python main.py refine  after accumulating real predictions to tune the model.")


def cmd_discover():
    """Auto-discover US politics and Israel markets."""
    import discovery
    init_db()
    print("\nDiscovering US politics & Israel markets on Polymarket...")
    _separator()
    result = discovery.refresh_watchlist()
    print(f"\nResult: {result['added']} added, {result.get('removed', 0)} removed, {result['total']} total")
    _separator()

    from intelligence.signals import get_managed_watchlist
    watchlist = get_managed_watchlist()
    print(f"\nActive watchlist ({len(watchlist)} markets):")
    for i, w in enumerate(watchlist, 1):
        vol_str = f"${w.get('volume', 0):,.0f}" if w.get('volume') else "N/A"
        print(f"  {i:2d}. {w['question'][:65]}")
        print(f"      Volume: {vol_str}  |  ID: {w['market_id']}")

    # Backfill historical data
    import collector
    print("\nBackfilling historical price data...")
    bf = collector.backfill_all_markets()
    print(f"  Backfilled {bf['markets_filled']} markets with {bf['total_points']} data points")


def cmd_collect():
    """Run one cycle of price collection."""
    import collector
    init_db()
    print("\nCollecting price snapshots for all watched markets...")
    _separator()
    result = collector.collect_all_snapshots()
    print(f"  Collected: {result['collected']}")
    print(f"  Failed:    {result['failed']}")
    print(f"  Total:     {result['total']}")


def cmd_volatility(args: list):
    """Show volatility metrics for one or all markets."""
    import volatility as volmod
    init_db()

    if args:
        market_id = args[0]
        print(f"\nVolatility for market {market_id}")
        _separator()
        summary = volmod.get_market_volatility_summary(market_id)
        if not summary:
            print("No volatility data yet. Run 'collect' first to gather price data.")
            return
        print(f"  Current price: {summary['current_price']:.4f}" if summary.get('current_price') else "  Current price: N/A")
        for window, data in summary.get("windows", {}).items():
            print(f"\n  {window}-minute window:")
            print(f"    Mean: {data['mean_price']:.4f}  |  Std: {data['std_dev']:.4f}")
            print(f"    Bollinger: [{data['bollinger_lower']:.4f} — {data['bollinger_upper']:.4f}]")
            print(f"    Z-Score: {data['z_score']:+.3f}  |  Signal: {data['signal_label']}")
    else:
        print("\nVolatility Summary — All Watched Markets")
        _separator()
        from intelligence.signals import get_all_latest_volatility, get_managed_watchlist
        all_vol = get_all_latest_volatility()
        market_names = {w["market_id"]: w["question"] for w in get_managed_watchlist()}

        if not all_vol:
            print("No volatility data yet. Run 'collect' first to gather price data.")
            return

        # Sort by absolute z-score
        all_vol.sort(key=lambda v: abs(v.get("z_score", 0)), reverse=True)
        for v in all_vol:
            name = market_names.get(v["market_id"], v["market_id"])[:55]
            z = v.get("z_score", 0)
            price = v.get("current_price", 0)
            std = v.get("std_dev", 0)
            label = _z_label(z)
            print(f"  {name}")
            print(f"    Price: {price:.4f}  |  Z: {z:+.2f} {label}  |  StdDev: {std:.4f}")


def cmd_opportunities():
    """Show mean reversion buy/sell opportunities."""
    import volatility as volmod
    init_db()
    print("\nMean Reversion Opportunities (|z-score| > 1.5)")
    _separator()
    opps = volmod.detect_opportunities(threshold_z=1.5)
    if not opps:
        print("  No significant opportunities detected.")
        print("  Markets are trading near fair value, or not enough data yet.")
        return
    for o in opps:
        direction = o["direction"]
        marker = "🟢" if direction == "BUY" else "🔴"
        print(f"  {marker} {direction} — {o['question'][:55]}")
        print(f"    Price: {o['current_price']:.4f}  |  Mean: {o['mean_price']:.4f}  |  Z: {o['z_score']:+.2f}")
        print(f"    Bollinger: [{o['bollinger_lower']:.4f} — {o['bollinger_upper']:.4f}]")
        print()


def cmd_signals():
    """Show active trading signals."""
    from intelligence.signals import get_active_trading_signals
    init_db()
    print("\nActive Trading Signals (last 24h)")
    _separator()
    signals = get_active_trading_signals(hours=24)
    if not signals:
        print("  No active signals.")
        return
    for s in signals:
        stype = s["signal_type"].upper()
        marker = "🟢 BUY " if stype == "BUY" else "🔴 SELL"
        print(f"  {marker} (strength: {s['strength']:.2f}) — {s.get('market_question', s['market_id'])[:55]}")
        print(f"    Price: {s['price_at_signal']:.4f}  |  {s['reasoning']}")
        print(f"    Time: {s['timestamp'][:19]}")
        if s.get("evaluated") and s.get("profit_loss") is not None:
            pl = s["profit_loss"]
            print(f"    P&L: {pl:+.4f} {'✓' if pl > 0 else '✗'}")
        print()


def cmd_signal_report():
    """Show trading signal performance report."""
    import signal_generator as siggen
    init_db()
    print("\nTrading Signal Performance Report")
    _separator()
    report = siggen.get_signal_performance_report()
    print(f"  Total signals:   {report['total_signals']}")
    print(f"  Evaluated:       {report['evaluated']}")
    print(f"  Win rate:        {report['win_rate']:.1%}")
    print(f"  Avg profit:      {report['avg_profit']:+.6f}")
    print(f"  Total profit:    {report['total_profit']:+.6f}")

    if report.get("by_type"):
        print("\n  By signal type:")
        for stype, data in report["by_type"].items():
            if data["count"] > 0:
                print(f"    {stype.upper()}: {data['count']} signals, "
                      f"win rate {data['win_rate']:.1%}, avg P&L {data['avg_pl']:+.6f}")

    if report.get("by_market"):
        print("\n  By market (top 5):")
        sorted_markets = sorted(report["by_market"].items(),
                                key=lambda x: x[1]["count"], reverse=True)[:5]
        for mid, data in sorted_markets:
            name = data.get("question", mid)[:45]
            print(f"    {name}")
            print(f"      Signals: {data['count']}  |  Win: {data['win_rate']:.0%}  |  P&L: {data['total_pl']:+.4f}")


def _z_label(z):
    if z > 2.0: return "(STRONG SELL)"
    elif z > 1.5: return "(SELL)"
    elif z < -2.0: return "(STRONG BUY)"
    elif z < -1.5: return "(BUY)"
    else: return ""


def _find_market(query: str):
    """Find market by slug, ID, or keyword search."""
    # Try slug
    market = polyapi.get_market_by_slug(query)
    if market:
        return market
    # Try ID
    if query.isdigit() or (len(query) > 20 and "-" in query):
        market = polyapi.get_market_by_id(query)
        if market:
            return market
    # Keyword search
    results = polyapi.search_markets(query, limit=10)
    if results:
        print(f"  Found {len(results)} result(s). Using top match: {results[0].question}")
        return results[0]
    return None


def print_help():
    print(__doc__)


COMMANDS = {
    "search": lambda args: cmd_search(" ".join(args)),
    "watch": lambda args: cmd_watch(" ".join(args)),
    "iran": lambda args: cmd_watch_iran(),
    "analyze": lambda args: cmd_analyze(),
    "predict": lambda args: cmd_predict(" ".join(args)),
    "orderbook": cmd_orderbook,
    "ob": cmd_orderbook,    # alias
    "resolve": lambda args: cmd_resolve(),
    "report": lambda args: cmd_report(),
    "refine": lambda args: cmd_refine(),
    "watchlist": lambda args: cmd_watchlist(),
    "demo": lambda args: cmd_demo(),
    "help": lambda args: print_help(),
    # Multi-market research
    "discover": lambda args: cmd_discover(),
    "collect": lambda args: cmd_collect(),
    "volatility": cmd_volatility,
    "vol": cmd_volatility,  # alias
    "opportunities": lambda args: cmd_opportunities(),
    "opps": lambda args: cmd_opportunities(),  # alias
    "signals": lambda args: cmd_signals(),
    "signal-report": lambda args: cmd_signal_report(),
}

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print_help()
        sys.exit(0)

    cmd = args[0].lower()
    rest = args[1:]

    if cmd in COMMANDS:
        COMMANDS[cmd](rest)
    else:
        print(f"Unknown command: {cmd}")
        print_help()
        sys.exit(1)
