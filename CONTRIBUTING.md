# Contributing to IBD RS Rating

Thanks for your interest. This is a small project maintained by one person, so the process here is deliberately light — there is no CLA, no required issue template, and no review board.

## What's wanted

Especially welcome:

- **Data-correctness fixes.** Anything where a rating is wrong, missing, or computed against the wrong denominator. These matter most — a plausible-but-wrong rating is worse than no rating.
- **Pipeline reliability.** The daily job runs unattended, and its failure modes are the interesting ones. See [Operations](docs/wiki/Operations.md) for the guards already in place.
- **Bug reports with a reproduction.** A failing test is the ideal bug report.
- **Documentation corrections**, including in `docs/wiki/`.

Please open an issue to discuss before starting on:

- **New client dependencies.** `rs_rating` is deliberately zero-dependency — standard library only. A PR adding `requests` to the client will be declined regardless of how much cleaner it reads. The engine (`ibd_rs`) may take dependencies, but each one has to earn its place in the daily job.
- **Changing the RS formula.** The 0.4/0.2/0.2/0.2 weighting over 63/126/189/252 trading days is kept for continuity with historical data. Changing it silently rewrites the meaning of every stored rating.
- **New data sources.** Replacing Finviz or yfinance is a real discussion, not a drive-by patch.

## Development setup

```bash
git clone https://github.com/tjdwls101010/IBD-RS-Rating.git
cd IBD-RS-Rating

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -e ".[engine,pg,dev]"
```

The extras map to the two halves of the project:

| Extra | Pulls in | Needed for |
|---|---|---|
| *(none)* | nothing | The `rs_rating` client alone |
| `engine` | pandas, yfinance, finvizfinance, requests | The `ibd_rs` calculation engine |
| `pg` | psycopg2-binary | Running the engine against Postgres |
| `dev` | pytest, build, twine | Tests and packaging |

Python 3.11 or newer. CI runs on 3.11 and 3.12.

## Running tests

```bash
pytest
```

The whole suite is deterministic and runs offline — network boundaries (`yfinance`, Finviz, the Neon Data API) are mocked, and database tests use in-memory SQLite. If a test you write needs the network, it belongs at a boundary that gets mocked instead.

Useful subsets while iterating:

```bash
pytest tests/test_rs.py          # RS computation
pytest tests/test_prices.py      # download orchestration and failure handling
pytest tests/test_client.py      # the public client
pytest -k trailing_window        # by name
```

`tests/test_ci_dependencies.py` is worth knowing about: it asserts that `requirements.lock` pins the versions actually installed, that the workflows install from that lock, and that the distributed package keeps its dependency ranges loose. If you change dependencies or a workflow, that test is what tells you the two stayed consistent.

## Dependency changes

There are two dependency surfaces and they are pinned differently on purpose:

- **`requirements.lock`** — exact pins (`==`). This is what CI installs, so the daily job runs the same versions today as yesterday. An unpinned upstream release breaking the pipeline is not hypothetical; it happened, and it is why this file exists.
- **`pyproject.toml`** — loose ranges (`>=`). This is what users get from PyPI, and hard pins there would collide with whatever else is in their environment.

To upgrade something, change both intentionally in the same PR, run the suite, and say in the PR description what you verified. Never regenerate the lock as a side effect of an unrelated change.

## Making a change

1. Branch from `main`. Any clear name is fine — `fix/split-detection-false-positive`.
2. Write a test that fails for the reason you're fixing, then make it pass. For data-correctness bugs this is not optional; a fix without a test that pins the behaviour tends to come back.
3. Keep the change focused. Unrelated refactors and reformatting make review harder and are likely to be asked out of the PR.
4. Update [`CHANGELOG.md`](CHANGELOG.md) under an `Unreleased` heading if the change affects behaviour, data meaning, or the public API.
5. Open the PR describing **what broke and why your fix addresses the cause**, not just what you changed.

Commit messages: a short imperative subject line, and a body explaining the reasoning when it isn't obvious. The existing history is a reasonable guide.

## Code style

There is no linter or formatter configured — match the surrounding code. In practice that means: standard library imports first, module-level `logger = logging.getLogger(__name__)`, docstrings on public functions explaining *why* where the *what* isn't self-evident, and lazy `%`-style logging arguments rather than f-strings.

Constants that tune the pipeline belong in `ibd_rs/config.py` with a comment explaining the value, not inlined at the call site. Several of those comments record why a specific number was chosen — keep that habit; the numbers are load-bearing.

## Reporting bugs

Open a [GitHub issue](https://github.com/tjdwls101010/IBD-RS-Rating/issues) with:

- What you ran and what you expected
- What happened instead, with the actual output
- Your Python version, the package version, and whether you're using the hosted endpoint or self-hosting

For data problems, include the ticker and date — that makes it checkable directly against the database.

**Security issues do not go in the issue tracker.** See [SECURITY.md](SECURITY.md).

## Code of Conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
