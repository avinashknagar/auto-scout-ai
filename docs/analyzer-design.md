# Car Value Analyzer — Design & Data Flow

## Overview

Two-stage funnel that finds best value-for-money used cars from scraped listings.
Stage 1 (heuristic scoring) is free and fast. Stage 2 (LLM) costs ~$0.05/run.

```
Scraped data (CSV)
    │
    ▼ Stage 1: Heuristic Scoring (free, ~1s)
   ~65 candidates
    │
    ▼ Stage 2: GPT-5.2 Analysis (~$0.05, ~60s)
   15 top deals with reasoning
    │
    ▼ Output
   Terminal + Markdown report + Ranked CSV
```

---

## End-to-End Data Flow

### Phase 1 — Scraping (upstream, not part of analyzer)

```
run.py:cmd_scrape()
  │
  ├─ cars/cars24_scraper.py    Fetches from Cars24 API
  ├─ cars/spinny_scraper.py    Fetches from Spinny API
  │      │
  │      ▼
  │  cars/base_scraper.py      Pagination loop, yields normalized cars
  │      │
  │      ▼
  │  cars/filters.py           Post-scrape filters (min_year, max_price, etc.)
  │      │
  │      ▼
  │  cars/db.py                Upserts into SQLite — each row has `platform` column
  │
  ▼
run.py:auto_export_csvs()
  │
  ▼
data/cars_latest.csv           ALL platforms mixed into one file
                               Column `platform` = "cars24" | "spinny"
```

**Key point:** The CSV is platform-mixed. The `platform` column is set during
scraping and carried through as a label, but no separation happens at this stage.

### Phase 2 — Analysis (the analyzer)

```
run.py:cmd_analyze(args, config)
  │
  │  Parses CLI args: --platform, --dry-run, --prefer, --top
  │
  ▼
cars/analyzer.py:run_analysis()          ← ORCHESTRATOR (main entry point)
  │
  │  1. Load data/cars_latest.csv into pandas DataFrame
  │  2. Filter by --platform flag       ← PLATFORM SEGREGATION HAPPENS HERE
  │  3. Filter by max_price from config
  │
  ├──────────────────────────────────────────────────────────────┐
  │  STAGE 1                                                     │
  │                                                              │
  │  cars/scoring.py:score_cars(df, preferences)                 │
  │    │                                                         │
  │    │  For each car, compute 6 sub-scores:                    │
  │    │    score_segment   (40%) — cheapest in make/model/year  │
  │    │    score_age_price (20%) — cheap + new = good           │
  │    │    score_km_price  (15%) — low km + cheap = good        │
  │    │    score_owners    (10%) — fewer owners = better         │
  │    │    score_brand     (10%) — static reliability lookup     │
  │    │    score_pref       (5%) — matches user preferences     │
  │    │                                                         │
  │    │  Composite: weighted sum → value_score (0–100)          │
  │    ▼                                                         │
  │  cars/scoring.py:select_candidates(scored_df, max=80)        │
  │    │                                                         │
  │    │  Top 60 overall by value_score                          │
  │    │  + Top 3 per body_type (diversity)                      │
  │    │  + Top 3 per fuel_type (diversity)                      │
  │    │  + Top 3 per transmission (diversity)                   │
  │    │  Dedup → cap at 80 → ~65 unique candidates              │
  │    ▼                                                         │
  │  cars/scoring.py:compute_segment_averages(scored_df)         │
  │    │                                                         │
  │    │  Group by (make, model, year)                           │
  │    │  Returns avg/min/max price + count per segment          │
  │    │  (Sent to LLM so it can judge if a price is low)       │
  │                                                              │
  ├──────────────────────────────────────────────────────────────┘
  │
  │  If --dry-run: print top 15, write ranked CSV, STOP here.
  │
  ├──────────────────────────────────────────────────────────────┐
  │  STAGE 2                                                     │
  │                                                              │
  │  cars/llm_client.py:analyze_with_llm(candidates, ...)       │
  │    │                                                         │
  │    │  Builds prompt with:                                    │
  │    │    - System: "You are an expert car market analyst..."  │
  │    │    - Market context (total listings, price range)       │
  │    │    - Segment averages (pipe-delimited compact table)    │
  │    │    - ~65 candidates (pipe-delimited, ~10K tokens)       │
  │    │    - User preferences from config                       │
  │    │    - JSON schema for expected response                  │
  │    │                                                         │
  │    │  Calls OpenAI Chat Completions API:                     │
  │    │    model: gpt-5.2                                       │
  │    │    reasoning_effort: medium                             │
  │    │    response_format: json_object                         │
  │    │    max_completion_tokens: 16384                         │
  │    │                                                         │
  │    │  Parses JSON response → returns dict with:              │
  │    │    top_deals[15]: rank, reasoning, value_rating,        │
  │    │                   risk_factors, estimated_fair_price,    │
  │    │                   savings_vs_market, listing_url         │
  │    │    segment_picks: best hatchback/SUV/automatic/first    │
  │    │    market_insights: paragraph of analysis               │
  │    │    _usage: token counts + estimated cost                │
  │    │                                                         │
  │    │  Safety: if finish_reason == "length" → raise error     │
  │    │          (response was truncated, need more tokens)     │
  │                                                              │
  ├──────────────────────────────────────────────────────────────┘
  │
  ├──────────────────────────────────────────────────────────────┐
  │  VALIDATION                                                  │
  │                                                              │
  │  cars/analyzer.py (post-LLM, pre-output)                    │
  │    │  For each deal in top_deals:                            │
  │    │    Check listing_url exists in candidates DataFrame     │
  │    │    If not found → log warning, exclude from results     │
  │    │  Validated deals replace raw top_deals in llm_result    │
  │    │                                                         │
  │  cars/report_writer.py (secondary check)                    │
  │    │  write_markdown_report also filters deals by URL match  │
  │    │  All LLM fields accessed via .get() with safe defaults  │
  │                                                              │
  ├──────────────────────────────────────────────────────────────┘
  │
  ├──────────────────────────────────────────────────────────────┐
  │  OUTPUT                                                      │
  │                                                              │
  │  cars/report_writer.py:print_terminal_summary(llm_result)   │
  │    │  Prints top 15 deals + segment picks + market insights  │
  │    │  to stdout                                              │
  │                                                              │
  │  cars/report_writer.py:write_markdown_report(...)            │
  │    │  Writes data/analysis/analysis_{date}_{platform}.md     │
  │    │  Contains: summary table, deal details with links,      │
  │    │  segment picks, market insights                         │
  │                                                              │
  │  cars/report_writer.py:write_ranked_csv(...)                 │
  │    │  Writes data/analysis/analysis_{date}_{platform}_ranked.csv │
  │    │  All ~65 candidates with value_score + all 6 sub-scores │
  │                                                              │
  ├──────────────────────────────────────────────────────────────┘
```

---

## File Responsibilities

| File | Role | Inputs | Outputs |
|------|------|--------|---------|
| `run.py` | CLI entry point | CLI args | Calls `run_analysis()` |
| `cars/analyzer.py` | Orchestrator | config, CLI flags | Coordinates all steps, returns summary dict |
| `cars/scoring.py` | Heuristic scoring | DataFrame, preferences | Scored DataFrame, candidates, segment averages |
| `cars/llm_client.py` | LLM wrapper | Candidates DF, config | Parsed JSON dict (deals, picks, insights) |
| `cars/report_writer.py` | Output | LLM result, candidates | Terminal output, .md file, .csv file |
| `config.yaml` | Config | — | model, preferences, thresholds |

---

## Scoring Logic Detail

### score_cars() — 6 components

**1. Segment Price Percentile (40% weight)**
- Groups cars by `(make, model, year)` — e.g., all "Maruti Baleno 2018"
- Ranks each car's price within its group using percentile rank
- Cheapest in group → score 100, most expensive → score 0
- **Single-car segments:** When a segment has only 1 car, `rank(pct=True)` returns 1.0 which would give score 0. This is corrected to 100 — the only car is the cheapest by definition.
- Why 40%: being the cheapest for the exact same car is the strongest value signal

**2. Age-Price Ratio (20% weight)**
- Formula: `price / car_age` (where `car_age = current_year - year`, min 1)
- Converts to percentile: lower ratio (newer + cheaper) → higher score
- A 2020 car at Rs 3L scores better than a 2018 car at Rs 3L

**3. KM-Price Product (15% weight)**
- Formula: `odometer_km * price`
- Converts to percentile: lower product (low km AND cheap) → higher score
- Penalizes high-km expensive cars, rewards low-km cheap ones

**4. Owner Score (10% weight)**
- Static mapping: 1 owner → 100, 2 owners → 60, 3 owners → 30, 4+ → 10
- Missing data defaults to 2 owners (score 60)

**5. Brand Reliability (10% weight)**
- Static lookup table (Toyota=92, Maruti=90, Hyundai=85, Tata=75, etc.)
- Unknown brands default to 60
- Based on general Indian market reputation for after-sales and resale

**6. Preference Bonus (5% weight)**
- Matches against user preferences from config (transmission, body_type, fuel_type, max_owners)
- Points: transmission match +40, body match +30, fuel match +20, owner threshold +10
- Normalized to 0-100 within the dataset

**Composite:**
```
value_score = segment*0.40 + age_price*0.20 + km_price*0.15
            + owners*0.10 + brand*0.10 + pref*0.05
```

### select_candidates() — diversity selection

1. Take top 60 by value_score
2. Add top 3 per body_type (ensures SUVs, sedans not drowned out by hatchbacks)
3. Add top 3 per fuel_type (ensures diesel/CNG/electric representation)
4. Add top 3 per transmission (ensures automatic cars appear)
5. Dedup by platform_id, cap at 80

Typical result: ~63-70 unique candidates.

---

## LLM Prompt Structure

```
┌─────────────────────────────────────────────┐
│ SYSTEM: Expert Indian used car analyst      │
├─────────────────────────────────────────────┤
│ USER:                                       │
│                                             │
│ ## Market Context                           │
│   Total listings, price range, platforms    │
│                                             │
│ ## Segment Averages                         │
│   make|model|year|avg|min|max|count         │
│   (pipe-delimited, one row per segment)     │
│                                             │
│ ## Candidates                               │
│   platform|make|model|variant|year|price|...|
│   (pipe-delimited, ~65 rows, ~10K tokens)   │
│                                             │
│ ## Buyer Preferences                        │
│   From config or --prefer flag              │
│                                             │
│ ## Instructions                             │
│   Return JSON with exact schema:            │
│   - top_deals[15]                           │
│   - segment_picks (4 categories)            │
│   - market_insights paragraph               │
└─────────────────────────────────────────────┘
```

**API settings:**
- `response_format: {"type": "json_object"}` — forces valid JSON output
- `reasoning_effort: "medium"` — balances quality vs speed/cost
- `max_completion_tokens: 16384` — enough for 15 detailed deals + extras

**Cost control:**
- Compact pipe-delimited format (not JSON) saves ~50% input tokens
- Only ~65 candidates sent (not all 1000+)
- Typical usage: ~10K input + ~4K output = ~$0.05/run

---

## CLI Usage

```bash
# Full analysis (both platforms)
python run.py analyze

# Heuristic only, no API cost
python run.py analyze --dry-run

# Single platform
python run.py analyze --platform spinny
python run.py analyze --platform cars24

# Preference overrides
python run.py analyze --prefer automatic suv

# More deals
python run.py analyze --top 20

# Combined
python run.py analyze --platform spinny --dry-run --prefer automatic
```

---

## Config Reference (analyze section)

```yaml
analyze:
  model: "gpt-5.2"              # OpenAI model ID
  reasoning_effort: "medium"     # none | low | medium | high | xhigh
  max_response_tokens: 16384     # Max output tokens (raise if truncation errors)
  max_candidates: 80             # Cap on candidates sent to LLM
  max_price: 500000              # Analysis-level price filter (Rs)
  top_deals_count: 15            # How many deals in final output
  output_dir: "data/analysis"    # Where reports are written
  # api_key: "sk-..."           # Or set OPENAI_API_KEY env var
  preferences:
    # transmission: "automatic"
    # body_type: "suv"
    # fuel_type: "petrol"
    # max_owners: 1
    # custom_note: "Looking for a reliable family car"
```

---

## Output Files

| File | Content |
|------|---------|
| `data/analysis/analysis_2026-03-04.md` | Full markdown report (all platforms) |
| `data/analysis/analysis_2026-03-04_spinny.md` | Spinny-only report |
| `data/analysis/analysis_2026-03-04_cars24.md` | Cars24-only report |
| `data/analysis/analysis_2026-03-04_ranked.csv` | All candidates with scores |

---

## Error Handling

| Error | Cause | Fix |
|-------|-------|-----|
| `FileNotFoundError: cars_latest.csv` | No scrape data | Run `python run.py scrape` first |
| `ValueError: OpenAI API key not found` | No API key | Set `OPENAI_API_KEY` env var |
| `ValueError: LLM response was truncated` | Output hit token limit | Increase `max_response_tokens` in config |
| `openai.BadRequestError` | Unsupported API param | Check model name and SDK version |
| Deal filtered (warning) | LLM returned listing_url not in candidates | Logged + excluded from report; remaining valid deals still shown |

---

## Potential Review Points

1. **Scoring weights** — Are 40/20/15/10/10/5 the right weights? Segment percentile dominates. Should brand reliability matter more?
2. **Brand reliability table** — Static and subjective. Missing brands default to 60. Should this be data-driven?
3. **Preference normalization** — Preference score is normalized to 0-100 relative to best match in dataset. If no car matches any preference, all get 0 (no distortion). But the 5% weight may be too low to meaningfully rerank.
4. **Segment grouping** — Groups by exact (make, model, year). A "Maruti Baleno 2018" and "Maruti Baleno 2019" are separate segments. Variants within same year are in the same group — a Delta and Alpha variant compete directly on price.
5. **Candidate diversity** — Top-3-per-segment approach ensures minority categories appear. But if there's only 1 automatic car in the data, it'll be included regardless of score.
6. **LLM prompt** — No few-shot examples. The JSON schema is described but not enforced beyond `response_format: json_object`. If the LLM returns unexpected keys or missing fields, report_writer uses `.get()` with defaults.
7. **Cost estimation** — Hardcoded at $2/M input, $8/M output. May drift as OpenAI updates pricing.
