"""
directory_harvester.py
======================
Automated ETL pipeline — public business directory scraper.

This is a generalised demonstration of the architecture used in a
production lead-generation pipeline built for a non-profit client (NDA).
It scrapes publicly available business directory data, cleans and
deduplicates it, and exports a structured CSV ready for lead scoring.

Target (demo): books.toscrape.com — a sandbox scraping site with no ToS
restrictions. Swap BASE_URL and the parsing logic for any public directory.

Author : Rawlings Muhinja
GitHub : github.com/muhinja2002-svg
Date   : 2026
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
from dataclasses import dataclass, asdict, field
from typing import Optional
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://books.toscrape.com/catalogue/"
START_URL   = "https://books.toscrape.com/catalogue/page-1.html"
MAX_PAGES   = 5          # cap for demo; set to None for full crawl
DELAY       = 1.5        # polite delay between requests (seconds)
OUTPUT_FILE = "leads_export.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; DirectoryHarvester/1.0; "
        "+https://github.com/muhinja2002-svg)"
    )
}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Lead:
    """Structured record extracted from one directory listing."""
    id:           int
    name:         str
    category:     str
    rating:       str
    price_gbp:    Optional[float]
    availability: str
    source_url:   str
    scraped_at:   str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Derived lead-scoring signals (populated in enrich step)
    price_band:   str = ""
    score:        int = 0


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def fetch_page(url: str) -> Optional[BeautifulSoup]:
    """
    GET a URL with retry logic. Returns a BeautifulSoup object or None.

    Implements:
      - Timeout to avoid hanging
      - HTTP error detection
      - Retry on transient failures (up to 3 attempts)
    """
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")

        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s on %s (attempt %d/3)", e.response.status_code, url, attempt)
        except requests.exceptions.ConnectionError:
            log.warning("Connection error on %s (attempt %d/3)", url, attempt)
        except requests.exceptions.Timeout:
            log.warning("Timeout on %s (attempt %d/3)", url, attempt)

        time.sleep(DELAY * attempt)   # exponential back-off

    log.error("Failed to fetch %s after 3 attempts — skipping.", url)
    return None


# ── Parsing ───────────────────────────────────────────────────────────────────

RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

def parse_listing(article, listing_id: int) -> Optional[Lead]:
    """
    Extract structured fields from a single directory listing.
    Returns None if a required field is missing (avoids partial records).
    """
    try:
        name  = article.h3.a["title"].strip()
        price_raw = article.find("p", class_="price_color").text.strip()
        avail = article.find("p", class_="instock").text.strip() if article.find("p", class_="instock") else "Unknown"
        rating_class = article.find("p", class_="star-rating")["class"][1]
        rating = str(RATING_MAP.get(rating_class, 0))
        href   = article.h3.a["href"]
        url    = BASE_URL + href.replace("../", "")

        # Clean price: strip currency symbol, handle encoding issues
        price_clean = re.sub(r"[^\d.]", "", price_raw)
        price_float = float(price_clean) if price_clean else None

        return Lead(
            id=listing_id,
            name=name,
            category="General",   # real pipeline: pull from category page
            rating=rating,
            price_gbp=price_float,
            availability=avail,
            source_url=url,
        )

    except (AttributeError, KeyError, ValueError) as e:
        log.warning("Parse error on listing %d: %s", listing_id, e)
        return None


# ── Enrichment & scoring ──────────────────────────────────────────────────────

def enrich(lead: Lead) -> Lead:
    """
    Add derived signals used for downstream lead scoring.

    In the production pipeline, this step incorporates external enrichment
    (e.g. company size, LinkedIn presence, grant history). Here we
    demonstrate the pattern with price banding and a simple composite score.
    """
    # Price band
    if lead.price_gbp is None:
        lead.price_band = "unknown"
    elif lead.price_gbp < 20:
        lead.price_band = "low"
    elif lead.price_gbp < 40:
        lead.price_band = "mid"
    else:
        lead.price_band = "high"

    # Composite score (0–100)
    # Weights reflect commercial priority in the original use case:
    #   rating (engagement proxy) + price band (deal-size proxy)
    rating_score = int(lead.rating) * 15          # max 75
    price_score  = {"low": 5, "mid": 15, "high": 25, "unknown": 0}[lead.price_band]
    avail_score  = 0 if lead.availability == "Unknown" else 10
    lead.score   = min(rating_score + price_score + avail_score, 100)

    return lead


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(leads: list[Lead]) -> list[Lead]:
    """
    Remove duplicate records by normalised name.
    In production: also de-duped on URL and organisation ID.
    """
    seen, unique = set(), []
    for lead in leads:
        key = lead.name.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    removed = len(leads) - len(unique)
    if removed:
        log.info("Deduplication removed %d duplicate record(s).", removed)
    return unique


# ── Export ────────────────────────────────────────────────────────────────────

def export_csv(leads: list[Lead], path: str) -> None:
    df = pd.DataFrame([asdict(l) for l in leads])
    # Sort by lead score descending — highest-priority targets first
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df.to_csv(path, index=False)
    log.info("Exported %d records to %s", len(df), path)
    # Preview
    print("\n── Top 5 leads by score ──────────────────────────────")
    print(df[["name", "rating", "price_gbp", "price_band", "score"]].head().to_string(index=False))
    print()


# ── Main crawl loop ───────────────────────────────────────────────────────────

def run():
    log.info("Starting harvest — base: %s", START_URL)
    all_leads: list[Lead] = []
    listing_id = 1
    next_url   = START_URL
    page_count = 0

    while next_url:
        page_count += 1
        log.info("Fetching page %d: %s", page_count, next_url)

        soup = fetch_page(next_url)
        if soup is None:
            break

        articles = soup.select("article.product_pod")
        log.info("  Found %d listings on page %d.", len(articles), page_count)

        for article in articles:
            lead = parse_listing(article, listing_id)
            if lead:
                lead = enrich(lead)
                all_leads.append(lead)
                listing_id += 1

        # Polite crawl delay
        time.sleep(DELAY)

        # Pagination
        next_btn = soup.select_one("li.next a")
        if next_btn and (MAX_PAGES is None or page_count < MAX_PAGES):
            href     = next_btn["href"]
            next_url = BASE_URL + href if not href.startswith("http") else href
        else:
            next_url = None

    log.info("Crawl complete: %d raw records from %d pages.", len(all_leads), page_count)

    all_leads = deduplicate(all_leads)
    export_csv(all_leads, OUTPUT_FILE)
    log.info("Done. Output: %s", OUTPUT_FILE)


if __name__ == "__main__":
    run()
