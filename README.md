# Car Scraping Pipeline

Scrapes used car listings from Cars24 and Spinny, stores them in SQLite, and finds the best value deals using heuristic scoring + LLM analysis.

## Quick Start!

```bash
# 1. Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Add auth tokens (see Auth section below)
cp tokens.yaml.example tokens.yaml   # edit with your tokens

# 3. Find the best cars (scrape + analyze in one shot)
python run.py findcars --dry-run     # no LLM cost, heuristic only
python run.py findcars               # full pipeline with LLM analysis
```

## Requirements

- Python 3.12+
- Dependencies: `requests`, `pyyaml`, `pandas`, `openai`
- Auth tokens for Cars24 and/or Spinny (see [Auth](#auth-tokens))
- OpenAI API key for LLM analysis (set `OPENAI_API_KEY` env var)

## Commands

### `findcars` — Full Pipeline (Recommended)

Runs scrape + export + analyze in one command.

```bash
python run.py findcars                          # Full pipeline
python run.py findcars --dry-run                # Scrape + heuristic only, no LLM cost
python run.py findcars --skip-scrape            # Reuse existing data, just re-analyze
python run.py findcars --platform spinny        # Single platform only
python run.py findcars --prefer automatic suv   # Preference overrides
python run.py findcars --top 20                 # Return top 20 deals instead of 15
```

| Flag | Description |
|------|-------------|
| `--platform {cars24,spinny,all}` | Which platform to scrape/analyze (default: all) |
| `--dry-run` | Heuristic scoring only, skips LLM (no API cost) |
| `--skip-scrape` | Skip scraping, reuse existing `cars_latest.csv` |
| `--prefer PREF [...]` | Override preferences (e.g. `automatic`, `suv`, `petrol`, `diesel`) |
| `--top N` | Number of top deals to return (default: 15) |

### `scrape` — Scrape Only

```bash
python run.py scrape                    # Scrape all enabled platforms
python run.py scrape --platform spinny  # Scrape one platform
python run.py scrape --dry-run          # Fetch first page only, don't store
```

### `analyze` — Analyze Only

```bash
python run.py analyze                           # Full LLM analysis
python run.py analyze --dry-run                 # Heuristic scoring only
python run.py analyze --platform cars24         # Single platform
python run.py analyze --prefer automatic suv    # Preference overrides
python run.py analyze --top 20                  # More deals
```

### `report` — View Summary

```bash
python run.py report                        # Summary of active listings
python run.py report --type changes         # Price changes for today
python run.py report --date 2026-03-01      # Specific date
```

### `export` — Export CSV

```bash
python run.py export                        # Export active listings
python run.py export --output my_cars.csv   # Custom output path
```

### `cleanup` — Maintenance

Purges old data, trims the database, rotates logs.

```bash
python run.py cleanup                   # Clean data older than 30 days
python run.py cleanup --older-than 14   # Override retention to 14 days
python run.py cleanup --dry-run         # Preview what would be cleaned
```

**What it cleans:**
- Delisted cars (`is_active=0`) older than N days
- Price history rows for inactive cars
- Old scrape run records
- Daily CSV snapshots (`cars_YYYY-MM-DD.csv`)
- Old analysis reports and ranked CSVs
- Rotates `scraper.log` if > 5MB
- VACUUMs the SQLite database

## Auth Tokens

Create a `tokens.yaml` file in the project root (gitignored):

```yaml
cars24:
  jwt: "your-cars24-jwt-token"

spinny:
  cookie: "your-spinny-session-cookie"
```

Tokens expire periodically. If you see `Auth expired` errors, refresh them from your browser's developer tools (Network tab > request headers).

## Configuration

All settings are in `config.yaml`. Key sections:

### Filters (Central Preferences)

Single source of truth used by both scraper and analyzer:

```yaml
filters:
  min_price: 50000          # Rs 50K floor
  max_price: 500000         # Rs 5L ceiling
  min_year: 2018
  max_owners: 1             # First owner only
  fuel_type: ["petrol", "cng"]
  max_odometer_km: 50000
  # transmission: "automatic"
  # body_type: ["suv", "hatchback"]
```

### Analyzer (LLM Settings)

```yaml
analyze:
  model: "gpt-5.2"
  reasoning_effort: "medium"
  max_response_tokens: 16384
  max_candidates: 80        # Candidates sent to LLM
  top_deals_count: 15
  output_dir: "data/analysis"
  # custom_note: "Looking for a reliable family car"
```

Set your API key via environment variable:
```bash
export OPENAI_API_KEY="sk-..."
```

### Cleanup

```yaml
cleanup:
  retention_days: 30
  log_max_size_mb: 5
```

### Rate Limiting

```yaml
rate_limit:
  delay_between_requests: 2.0
  max_retries: 3
  backoff_base: 2.0
  request_timeout: 30
```

## Project Structure

```
cars/
├── run.py                  # CLI entry point (all commands)
├── config.yaml             # All configuration
├── tokens.yaml             # Auth tokens (gitignored)
├── requirements.txt
├── cars/
│   ├── analyzer.py         # Orchestrator: scoring -> LLM -> output
│   ├── scoring.py          # Heuristic scoring (6-component value score)
│   ├── llm_client.py       # OpenAI API wrapper
│   ├── report_writer.py    # Terminal, markdown, and CSV output
│   ├── db.py               # SQLite database layer
│   ├── base_scraper.py     # Shared scraper logic (pagination, rate limiting)
│   ├── cars24_scraper.py   # Cars24 API scraper
│   ├── spinny_scraper.py   # Spinny API scraper
│   ├── filters.py          # Post-scrape filters
│   ├── tracker.py          # Detects delistings and price changes
│   ├── cleanup.py          # Maintenance (file + DB cleanup)
│   ├── models.py           # NormalizedCar data model
│   ├── config.py           # Config/token loading
│   └── utils.py            # Logging setup, shared utilities
├── data/                   # All generated data (gitignored)
│   ├── cars.db             # SQLite database
│   ├── cars_latest.csv     # Current active listings
│   ├── cars_YYYY-MM-DD.csv # Daily snapshots
│   ├── price_changes.csv   # Today's price changes
│   ├── scraper.log         # Log file
│   └── analysis/
│       ├── analysis_YYYY-MM-DD.md          # Markdown report
│       └── analysis_YYYY-MM-DD_ranked.csv  # All candidates with scores
└── docs/
    └── analyzer-design.md  # Detailed analyzer architecture docs
```

## How It Works

### Scraping

1. Fetches listings from Cars24/Spinny APIs with server-side price filtering
2. Post-filters by year, owners, fuel type, odometer (from `config.yaml` filters)
3. Upserts into SQLite (tracks first/last seen dates, price history)
4. Detects delistings (cars not seen this run) and price changes
5. Auto-exports `cars_latest.csv` and daily snapshot

### Analysis (Two-Stage)

**Stage 1 — Heuristic Scoring (free, ~1 second):**

Computes a 0-100 `value_score` from 6 weighted components:

| Component | Weight | What it measures |
|-----------|--------|------------------|
| Segment price percentile | 40% | Cheapest within same make/model/year |
| Age-price ratio | 20% | Newer + cheaper = better |
| KM-price product | 15% | Low km + cheap = better |
| Owner score | 10% | Fewer owners = better |
| Brand reliability | 10% | Static reliability lookup (Toyota > Tata > etc.) |
| Preference bonus | 5% | Matches your filter preferences |

Selects ~65 diverse candidates (top by score + top per body/fuel/transmission type).

**Stage 2 — LLM Analysis (~$0.05, ~60 seconds):**

Sends candidates to GPT with segment averages and your preferences. Returns:
- Top 15 deals with reasoning, risk factors, and fair price estimates
- Segment picks (best hatchback, best SUV, best automatic, best first-owner)
- Market insights paragraph

### Database Schema

Three tables in `data/cars.db`:
- **cars** — all listings (active + delisted), keyed by `(platform, platform_id)`
- **price_history** — one price entry per car per scrape day
- **scrape_runs** — metadata for each scrape execution

## Output Files

| File | When Created |
|------|-------------|
| `data/cars_latest.csv` | After every scrape |
| `data/cars_YYYY-MM-DD.csv` | After every scrape (daily snapshot) |
| `data/analysis/analysis_YYYY-MM-DD.md` | After full analysis |
| `data/analysis/analysis_YYYY-MM-DD_ranked.csv` | After any analysis (including dry-run) |

## Data Reset

To start completely fresh, delete the `data/` folder:

```bash
rm -rf data/
```

It will be recreated automatically on the next scrape.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Auth expired` | Platform tokens expired | Refresh tokens in `tokens.yaml` from browser dev tools |
| `FileNotFoundError: cars_latest.csv` | No scrape data exists | Run `python run.py scrape` first |
| `ValueError: OpenAI API key not found` | Missing API key | Set `OPENAI_API_KEY` env var |
| `ValueError: LLM response was truncated` | Output hit token limit | Increase `max_response_tokens` in config |
| Deal filtered (warning in log) | LLM hallucinated a listing URL | Normal — invalid deals are excluded, valid ones still shown |
