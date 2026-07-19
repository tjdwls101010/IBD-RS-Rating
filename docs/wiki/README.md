# IBD RS Rating — Documentation

This is the full documentation for IBD RS Rating, a system that computes percentile-ranked relative strength ratings (1–99) for roughly 4,600 US stocks every trading day, and a zero-dependency Python client for reading them.

The [repository README](../../README.md) is the short version — what the project is and how to get a first result. These pages are the long version: what the numbers mean, how they are produced, how to run the pipeline yourself, and what to do when something looks wrong.

**New here? Start with [Overview](Overview.md)**, then [Getting Started](Getting-Started.md).

## Contents

| Page | What it covers |
|---|---|
| **[Overview](Overview.md)** | The problem this solves, the approach it takes, who it's for, and what it deliberately does not do |
| **[Getting Started](Getting-Started.md)** | Installing the client and getting a real result; installing the engine and building your own database |
| **[Concepts](Concepts.md)** | The vocabulary: RS Raw, RS Rating, the universe, warm-up, the trailing window, silent stalls |
| **[Architecture](Architecture.md)** | The two packages, how data flows through them, the database schema, and the design decisions behind each |
| **[Data Pipeline](Data-Pipeline.md)** | Ticker sourcing from Finviz, price download from yfinance, split repair, retention — the daily job in detail |
| **[API Reference](API-Reference.md)** | All 15 methods on the `RS` client, with parameters, return shapes, and errors |
| **[CLI Reference](CLI-Reference.md)** | Every `ibd-rs` command, its steps, its exit codes, and when to reach for it |
| **[Operations](Operations.md)** | Running the pipeline yourself: database setup, scheduling, the reliability guards, and monitoring |
| **[Troubleshooting](Troubleshooting.md)** | Symptoms mapped to causes and fixes, for both client users and self-hosters |
| **[FAQ](FAQ.md)** | How accurate is it really, why does a stock have no rating, why is a date missing, and other recurring questions |

## Reading paths

**"I want to use the ratings in my own analysis."**
[Overview](Overview.md) → [Getting Started](Getting-Started.md) → [API Reference](API-Reference.md) → [Concepts](Concepts.md) when a number surprises you.

**"I want to run this myself."**
[Architecture](Architecture.md) → [Getting Started](Getting-Started.md) (self-hosting section) → [Operations](Operations.md) → [CLI Reference](CLI-Reference.md).

**"I want to understand or change how ratings are computed."**
[Concepts](Concepts.md) → [Architecture](Architecture.md) → [Data Pipeline](Data-Pipeline.md) → [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Elsewhere in the repository

- [CHANGELOG.md](../../CHANGELOG.md) — release history, including the breaking changes in 0.4.0
- [CONTRIBUTING.md](../../CONTRIBUTING.md) — development setup and how to submit a change
- [SECURITY.md](../../SECURITY.md) — reporting a vulnerability privately
- [`data/tickers.csv`](../../data/tickers.csv) — a committed snapshot of the latest ratings
