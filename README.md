# directory-harvester-etl
A production-grade ETL pipeline for automated lead generation. Features modular architecture for resilient crawling, data enrichment, and multi-factor lead scoring. (Generalised demonstration of enterprise deployment).
# Directory Harvester — Automated Lead-Gen ETL Pipeline

A production-grade Python ETL pipeline that scrapes public business directory data, enriches records with lead-scoring signals, deduplicates, and exports a prioritised CSV ready for outreach or downstream modelling.

Built as a generalised demonstration of an architecture deployed in a live lead-generation pipeline for a non-profit client (under NDA).

---

## What it does

```
Public directory  →  Fetch + parse  →  Enrich & score  →  Deduplicate  →  CSV export
```

1. **Crawls** a paginated public directory with polite rate limiting and retry logic
2. **Parses** structured fields from each listing (name, category, rating, price, availability)
3. **Enriches** each record with derived signals: price band and a composite lead score (0–100)
4. **Deduplicates** on normalised name to prevent double-counting
5. **Exports** a CSV sorted by lead score descending — highest-priority targets first

---

## Quickstart

```bash
# Install dependencies
pip install requests beautifulsoup4 pandas

# Run (demo target: books.toscrape.com — a public sandbox with no ToS restrictions)
python directory_harvester.py
```

Output: `leads_export.csv` in the current directory.

---

## Output schema

| Column | Type | Description |
|---|---|---|
| `id` | int | Unique listing ID |
| `name` | str | Organisation / listing name |
| `category` | str | Directory category |
| `rating` | str | Star rating (1–5) |
| `price_gbp` | float | Listed price |
| `availability` | str | In-stock status |
| `source_url` | str | Source URL |
| `scraped_at` | str | UTC timestamp |
| `price_band` | str | `low / mid / high / unknown` |
| `score` | int | Lead score 0–100 (higher = higher priority) |

---

## Lead scoring logic

```python
score = (star_rating × 15) + price_band_score + availability_score
```

| Signal | Weight | Rationale |
|---|---|---|
| Star rating (1–5) | × 15 (max 75) | Engagement/quality proxy |
| Price band | 5 / 15 / 25 | Deal-size proxy |
| Availability | +10 | Reachability signal |

Weights are configurable — in the production version these were calibrated against historical conversion data.

---

## Production extensions (not included due to NDA)

- External enrichment API calls (company size, LinkedIn presence, grant history)
- SQL database output (PostgreSQL) instead of CSV
- Incremental crawl mode with change detection
- Proxied requests for large-scale crawls
- Slack alert on crawl completion with summary stats

---

## Configuration

Edit the constants at the top of `directory_harvester.py`:

```python
BASE_URL  = "https://books.toscrape.com/catalogue/"   # swap for target directory
MAX_PAGES = 5       # set to None for full crawl
DELAY     = 1.5     # seconds between requests
```

---

## Stack

- `requests` — HTTP with retry logic
- `BeautifulSoup4` — HTML parsing
- `pandas` — data manipulation and CSV export
- `dataclasses` — typed data model for each lead record

---

## Author

Rawlings Muhinja · [github.com/muhinja2002-svg](https://github.com/muhinja2002-svg) · [datascienceportfol.io/muhinja2002](https://datascienceportfol.io/muhinja2002)
