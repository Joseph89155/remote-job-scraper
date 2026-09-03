#!/usr/bin/env python3
"""
Remote Job Scraper — Bookkeeping / Accounting / Tax / Procurement
===================================================================

Goes beyond LinkedIn/Indeed by pulling from sources that are actually
meant to be consumed programmatically: public job-board APIs and RSS
feeds. This avoids ToS violations and IP bans that come from scraping
LinkedIn/Indeed HTML directly.

Sources wired up:
  1. Remotive API      (https://remotive.com/api/remote-jobs)
  2. Arbeitnow API     (https://www.arbeitnow.com/api/job-board-api)
  3. Jobicy API        (https://jobicy.com/api/v2/remote-jobs)
  4. We Work Remotely  (RSS feeds, category-scoped)

Also includes a `greenhouse_company()` helper — most mid-size finance,
accounting-outsourcing (BPO), and procurement-consulting firms post
jobs through Greenhouse or Lever, both of which expose a public JSON
API per company. This is the "go deeper" trick: instead of relying on
aggregators, you can point this script directly at 50-100 relevant
employers' career pages and catch listings before they ever reach
Indeed/LinkedIn.

Install:
    pip install requests feedparser

Run:
    python remote_job_scraper.py
    python remote_job_scraper.py --keywords bookkeeping tax procurement
    python remote_job_scraper.py --companies gitlab zapier automattic
    python remote_job_scraper.py --output my_jobs.csv

State tracking (new since last run):
    By default the script remembers every job URL it has ever seen in
    job_state.json, sitting next to the script. Each run's CSV only
    contains jobs that weren't in that file yet — the first run treats
    everything as a baseline (nothing to compare against), and every
    run after that shows only genuinely new listings.

    python remote_job_scraper.py                  # writes only new jobs
    python remote_job_scraper.py --all             # writes everything, still updates state
    python remote_job_scraper.py --no-state        # old behavior, no tracking at all
    python remote_job_scraper.py --state-file foo.json   # custom state file location
"""

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import requests

try:
    import feedparser
except ImportError:
    feedparser = None

DEFAULT_KEYWORDS = [
    "bookkeeping", "bookkeeper", "accounting", "accountant",
    "tax compliance", "tax analyst", "tax associate", "tax manager",
    "procurement", "purchasing", "sourcing specialist",
    "accounts payable", "accounts payables", "accounts receivable", "accounts receivables",
    "financial compliance", "audit", "controller", "AP/AR",
    # Entry-level/junior titles that use a different noun entirely, so they
    # wouldn't otherwise share a root with the terms above (e.g. "Junior
    # Accountant" already matches via "accountant" — these cover the ones
    # that don't, like a bare "Buyer" or "AP Clerk").
    "buyer", "ap clerk", "ar clerk", "billing clerk", "accounting intern",
    "tax intern", "procurement intern", "purchasing intern",
]

# Titles that tend to false-positive when a keyword only shows up in the
# job DESCRIPTION rather than the title — e.g. an "ERP System Administrator"
# posting that happens to mention "accounting software" in its blurb, or a
# "Relationship Manager" role whose description name-drops "compliance".
# If a title contains one of these AND has no strong keyword match itself,
# it's rejected even if the description matched.
EXCLUDE_TITLE_TERMS = [
    "relationship manager", "system administrator", "account executive",
    "sales representative", "customer success", "software engineer",
    "product manager", "marketing", "recruiter", "administrative assistant",
    "business development", "media buyer", "personal shopper",
]

HEADERS = {"User-Agent": "job-search-script/1.0 (personal use)"}
TIMEOUT = 15


@dataclass
class Job:
    title: str
    company: str
    location: str
    url: str
    source: str
    posted: str = ""
    snippet: str = ""


def _word_match(text: str, terms: List[str]) -> bool:
    text = text.lower()
    return any(re.search(rf"\b{re.escape(term.lower())}\b", text) for term in terms)


def matches_keywords(text: str, keywords: List[str]) -> bool:
    """Legacy loose match (used as a description-level fallback only)."""
    return _word_match(text, keywords)


def is_relevant(title: str, description: str, keywords: List[str],
                 excludes: List[str] = EXCLUDE_TITLE_TERMS) -> bool:
    """
    Exclude terms in the title win outright — this matters now that a broad
    keyword like "buyer" exists, so "Media Buyer" gets rejected via the
    "media buyer" exclude rather than slipping through on the bare word
    "buyer" before the exclude check ever ran. Otherwise: a title keyword
    match is trusted, and a description-only match is accepted as a fallback.
    """
    if _word_match(title, excludes):
        return False
    if _word_match(title, keywords):
        return True
    return _word_match(description, keywords)


# ---------------------------------------------------------------- sources --

def fetch_remotive(keywords: List[str], excludes: List[str] = EXCLUDE_TITLE_TERMS) -> List[Job]:
    jobs = []
    for kw in keywords:
        try:
            r = requests.get(
                "https://remotive.com/api/remote-jobs",
                params={"search": kw}, headers=HEADERS, timeout=TIMEOUT,
            )
            r.raise_for_status()
            for j in r.json().get("jobs", []):
                title = j.get("title", "")
                description = re.sub("<[^<]+?>", "", j.get("description", ""))
                if not is_relevant(title, description, keywords, excludes):
                    continue
                jobs.append(Job(
                    title=title,
                    company=j.get("company_name", ""),
                    location=j.get("candidate_required_location", ""),
                    url=j.get("url", ""),
                    source="Remotive",
                    posted=j.get("publication_date", ""),
                    snippet=description[:200],
                ))
        except requests.RequestException as e:
            print(f"[Remotive] skipped '{kw}': {e}", file=sys.stderr)
        time.sleep(0.3)
    return jobs


def fetch_arbeitnow(keywords: List[str], excludes: List[str] = EXCLUDE_TITLE_TERMS) -> List[Job]:
    jobs = []
    try:
        r = requests.get(
            "https://www.arbeitnow.com/api/job-board-api",
            headers=HEADERS, timeout=TIMEOUT,
        )
        r.raise_for_status()
        for j in r.json().get("data", []):
            if not j.get("remote"):
                continue
            title = j.get("title", "")
            description = f"{j.get('description','')} {' '.join(j.get('tags', []))}"
            if is_relevant(title, description, keywords, excludes):
                jobs.append(Job(
                    title=j.get("title", ""),
                    company=j.get("company_name", ""),
                    location=j.get("location", "Remote"),
                    url=j.get("url", ""),
                    source="Arbeitnow",
                    posted=str(j.get("created_at", "")),
                    snippet=re.sub("<[^<]+?>", "", j.get("description", ""))[:200],
                ))
    except requests.RequestException as e:
        print(f"[Arbeitnow] skipped: {e}", file=sys.stderr)
    return jobs


def fetch_jobicy(keywords: List[str], excludes: List[str] = EXCLUDE_TITLE_TERMS) -> List[Job]:
    jobs = []
    for tag in ["accounting", "finance", "procurement"]:
        try:
            r = requests.get(
                "https://jobicy.com/api/v2/remote-jobs",
                params={"count": 50, "tag": tag}, headers=HEADERS, timeout=TIMEOUT,
            )
            r.raise_for_status()
            for j in r.json().get("jobs", []):
                title = j.get("jobTitle", "")
                description = j.get("jobExcerpt", "") or ""
                if is_relevant(title, description, keywords, excludes):
                    jobs.append(Job(
                        title=j.get("jobTitle", ""),
                        company=j.get("companyName", ""),
                        location=j.get("jobGeo", "Remote"),
                        url=j.get("url", ""),
                        source="Jobicy",
                        posted=j.get("pubDate", ""),
                        snippet=(j.get("jobExcerpt", "") or "")[:200],
                    ))
        except requests.RequestException as e:
            print(f"[Jobicy] skipped tag '{tag}': {e}", file=sys.stderr)
        time.sleep(0.3)
    return jobs


WWR_FEEDS = [
    "https://weworkremotely.com/categories/remote-accounting-finance-jobs.rss",
    "https://weworkremotely.com/categories/remote-management-and-finance-jobs.rss",
]


def fetch_weworkremotely(keywords: List[str], excludes: List[str] = EXCLUDE_TITLE_TERMS) -> List[Job]:
    if feedparser is None:
        print("[WeWorkRemotely] skipped: run `pip install feedparser`", file=sys.stderr)
        return []
    jobs = []
    for feed_url in WWR_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                title = entry.get("title", "")
                description = entry.get("summary", "")
                if is_relevant(title, description, keywords, excludes):
                    company = entry.get("title", "").split(":")[0] if ":" in entry.get("title", "") else ""
                    jobs.append(Job(
                        title=entry.get("title", ""),
                        company=company,
                        location="Remote",
                        url=entry.get("link", ""),
                        source="WeWorkRemotely",
                        posted=entry.get("published", ""),
                        snippet=re.sub("<[^<]+?>", "", entry.get("summary", ""))[:200],
                    ))
        except Exception as e:
            print(f"[WeWorkRemotely] skipped feed: {e}", file=sys.stderr)
    return jobs


# ---------------------------------------------- deeper: direct ATS lookups --

def greenhouse_company(slug: str, keywords: List[str]) -> List[Job]:
    """
    Pull open roles directly from a company's Greenhouse board.
    Find a company's slug from its careers URL, e.g.
    boards.greenhouse.io/gitlab -> slug = "gitlab"
    This is how you go deeper than aggregators: aggregators lag,
    and many companies (esp. finance/procurement BPOs) don't syndicate
    every posting to Indeed/LinkedIn.
    """
    jobs = []
    try:
        r = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
            headers=HEADERS, timeout=TIMEOUT,
        )
        r.raise_for_status()
        for j in r.json().get("jobs", []):
            title = j.get("title", "")
            if _word_match(title, keywords):
                jobs.append(Job(
                    title=j.get("title", ""),
                    company=slug,
                    location=(j.get("location") or {}).get("name", ""),
                    url=j.get("absolute_url", ""),
                    source=f"Greenhouse:{slug}",
                ))
    except requests.RequestException as e:
        print(f"[Greenhouse:{slug}] skipped: {e}", file=sys.stderr)
    return jobs


def lever_company(slug: str, keywords: List[str]) -> List[Job]:
    """Same idea as greenhouse_company() but for Lever-hosted boards."""
    jobs = []
    try:
        r = requests.get(
            f"https://api.lever.co/v0/postings/{slug}?mode=json",
            headers=HEADERS, timeout=TIMEOUT,
        )
        r.raise_for_status()
        for j in r.json():
            title = j.get("text", "")
            if _word_match(title, keywords):
                jobs.append(Job(
                    title=j.get("text", ""),
                    company=slug,
                    location=(j.get("categories") or {}).get("location", ""),
                    url=j.get("hostedUrl", ""),
                    source=f"Lever:{slug}",
                ))
    except requests.RequestException as e:
        print(f"[Lever:{slug}] skipped: {e}", file=sys.stderr)
    return jobs


# ---------------------------------------------------------- state tracking --

def load_state(state_file: str) -> dict:
    """
    State file schema: {"seen": {url: first_seen_iso_timestamp, ...}}
    Missing file = first-ever run; treated as an empty baseline.
    """
    path = Path(state_file)
    if not path.exists():
        return {"seen": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("seen", {})
            return data
    except (json.JSONDecodeError, OSError) as e:
        print(f"[state] couldn't read {state_file} ({e}), starting fresh", file=sys.stderr)
        return {"seen": {}}


def save_state(state_file: str, state: dict) -> None:
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def split_new_vs_seen(jobs: List[Job], state: dict):
    """Returns (new_jobs, updated_state). Does not mutate the input state dict."""
    seen = dict(state.get("seen", {}))
    now = datetime.now(timezone.utc).isoformat()
    new_jobs = []
    for j in jobs:
        key = j.url or f"{j.title}|{j.company}"
        if key not in seen:
            new_jobs.append(j)
            seen[key] = now
    return new_jobs, {"seen": seen}


# ------------------------------------------------------------------- main --

def dedupe(jobs: List[Job]) -> List[Job]:
    seen = set()
    out = []
    for j in jobs:
        key = j.url or (j.title, j.company)
        if key not in seen:
            seen.add(key)
            out.append(j)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keywords", nargs="+", default=DEFAULT_KEYWORDS,
                         help="Keywords to filter on (default: accounting/bookkeeping/tax/procurement terms)")
    parser.add_argument("--companies", nargs="*", default=[],
                         help="Greenhouse company slugs to check directly, e.g. --companies gitlab automattic")
    parser.add_argument("--lever-companies", nargs="*", default=[],
                         help="Lever company slugs to check directly")
    parser.add_argument("--output", default="remote_jobs.csv", help="Output CSV filename")
    parser.add_argument("--skip-search-keywords", nargs="*", default=["bookkeeping", "accounting", "tax compliance", "procurement"],
                         help="Shorter keyword set used for the Remotive per-term search (keeps API calls low)")
    parser.add_argument("--state-file", default="job_state.json",
                         help="Path to the JSON file tracking previously-seen job URLs (default: job_state.json)")
    parser.add_argument("--all", action="store_true",
                         help="Write every matching job to the CSV, not just ones new since the last run")
    parser.add_argument("--no-state", action="store_true",
                         help="Ignore/skip state tracking entirely (old CSV-only behavior)")
    parser.add_argument("--exclude-terms", nargs="*", default=EXCLUDE_TITLE_TERMS,
                         help="Title terms that reject a description-only match (default: common false-positive roles)")
    args = parser.parse_args()

    all_jobs: List[Job] = []

    print("Searching Remotive...")
    all_jobs += fetch_remotive(args.skip_search_keywords, args.exclude_terms)

    print("Searching Arbeitnow...")
    all_jobs += fetch_arbeitnow(args.keywords, args.exclude_terms)

    print("Searching Jobicy...")
    all_jobs += fetch_jobicy(args.keywords, args.exclude_terms)

    print("Searching We Work Remotely...")
    all_jobs += fetch_weworkremotely(args.keywords, args.exclude_terms)

    for slug in args.companies:
        print(f"Checking Greenhouse board: {slug}")
        all_jobs += greenhouse_company(slug, args.keywords)

    for slug in args.lever_companies:
        print(f"Checking Lever board: {slug}")
        all_jobs += lever_company(slug, args.keywords)

    all_jobs = dedupe(all_jobs)
    all_jobs.sort(key=lambda j: j.source)

    if args.no_state:
        jobs_to_write = all_jobs
        is_first_run = False
    else:
        state = load_state(args.state_file)
        is_first_run = not state.get("seen")
        new_jobs, updated_state = split_new_vs_seen(all_jobs, state)
        save_state(args.state_file, updated_state)
        jobs_to_write = all_jobs if args.all else new_jobs

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "company", "location", "url", "source", "posted", "snippet"])
        writer.writeheader()
        for j in jobs_to_write:
            writer.writerow(asdict(j))

    if args.no_state:
        label = "matching roles"
    elif args.all:
        label = "matching roles (--all: state tracked but not filtered)"
    elif is_first_run:
        label = "matching roles (first run — all treated as baseline, saved to state)"
    else:
        label = "NEW roles since last run"

    print(f"\nFound {len(jobs_to_write)} {label}. Saved to {args.output}")
    for j in jobs_to_write[:15]:
        print(f"  [{j.source}] {j.title} — {j.company} ({j.location})")
    if len(jobs_to_write) > 15:
        print(f"  ...and {len(jobs_to_write) - 15} more in the CSV.")


if __name__ == "__main__":
    main()