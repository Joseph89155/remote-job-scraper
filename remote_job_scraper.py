#!/usr/bin/env python3
"""
Remote Analytics Job Finder — Data Science / Data Analysis / BI
================================================================

Pulls remote analytics roles from sources that are meant to be consumed
programmatically (public job-board APIs and RSS feeds), filters them by
job TITLE, and remembers what it has already shown you so each run only
surfaces new listings.

Sources:
  1. Remotive API       (category "data" + keyword searches)
  2. Arbeitnow API      (remote-flagged jobs, first 3 pages)
  3. Jobicy API         (industry "data-science" + keyword tags)
  4. We Work Remotely   (RSS: all-jobs, programming, all-other,
                         management-and-finance, product)

Plus two "go deeper" helpers: greenhouse_company() and lever_company()
read a company's own job board directly (public JSON, no login), which
catches roles before they reach aggregators.

Install:
    pip install requests feedparser

Run:
    python remote_job_scraper.py                       # new jobs since last run
    python remote_job_scraper.py --all                 # everything, still updates state
    python remote_job_scraper.py --no-state            # browse only, state untouched
    python remote_job_scraper.py --scan-descriptions   # also match on description text
    python remote_job_scraper.py --hide-restricted     # drop US/Canada/LATAM-only roles
    python remote_job_scraper.py --companies gitlab    # check a Greenhouse board directly
    python remote_job_scraper.py --output my_jobs.csv

Matching rules:
    A job is kept when its TITLE contains one of the keywords. Titles that
    contain an exclude term (instructor, data entry, ...) are dropped even
    if a keyword matched. Descriptions are ignored by default because almost
    every tech job description mentions "data analysis" somewhere; turn them
    on with --scan-descriptions if you want a wider net.

Location fit (for a Nairobi-based candidate, UTC+3):
    Every row gets a fit column: yes (worldwide / anywhere / Africa / EMEA),
    maybe (Europe/UK, no location stated, or unrecognized) or no (only lists
    regions that exclude Kenya). Rows are sorted yes, maybe, no. This reads
    the posting's location text only; it cannot see eligibility rules that
    appear only in the description. If the same role (company + title) shows
    up from two sources and one lists it as restricted, the other copies are
    downgraded to maybe, since sources sometimes disagree.

State tracking:
    Every job URL ever shown is stored in job_state.json. The first run
    treats everything as new; later runs only write jobs not seen before.
"""

import argparse
import csv
import html
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests

try:
    import feedparser
except ImportError:
    feedparser = None

# ------------------------------------------------------------- targeting --

DEFAULT_KEYWORDS = [
    # Data science
    "data scientist", "data science",
    # Data analysis
    "data analyst", "data analysis", "data analytics",
    "reporting analyst", "insights analyst", "product analyst",
    # BI
    "business intelligence", "bi analyst", "bi developer", "bi engineer",
    "power bi", "tableau",
]

# Title terms that reject a job even when a keyword matched, e.g.
# "Data Science Instructor" or "Online Part Time Faculty - Data Science".
EXCLUDE_TITLE_TERMS = [
    "instructor", "tutor", "teacher", "professor", "faculty", "lecturer",
    "recruiter", "sales representative", "account executive", "data entry",
    "software engineer",
]

# Search terms sent to Remotive (kept short to keep API calls low; the
# category="data" request already covers most of its data roles).
DEFAULT_REMOTIVE_SEARCHES = ["data scientist", "data analyst", "business intelligence"]

JOBICY_TAGS = ["data analyst", "data scientist", "business intelligence"]

WWR_FEEDS = [
    "https://weworkremotely.com/remote-jobs.rss",
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
    "https://weworkremotely.com/categories/all-other-remote-jobs.rss",
    "https://weworkremotely.com/categories/remote-management-and-finance-jobs.rss",
    "https://weworkremotely.com/categories/remote-product-jobs.rss",
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
    fit: str = ""        # yes / maybe / no, for a Nairobi-based candidate
    fit_note: str = ""

    def __post_init__(self):
        # Some APIs return HTML entities (e.g. "&#038;"); store plain text.
        for name in ("title", "company", "location", "snippet"):
            setattr(self, name, html.unescape(getattr(self, name) or ""))


def _word_match(text: str, terms: List[str]) -> bool:
    text = (text or "").lower()
    return any(re.search(rf"\b{re.escape(t.lower())}\b", text) for t in terms)


def _strip_html(text: str) -> str:
    return re.sub(r"<[^<]+?>", "", text or "")


@dataclass
class Matcher:
    """Decides whether a job is relevant. Call it with (title, description)."""
    keywords: List[str]
    excludes: List[str]
    scan_description: bool = False

    def __call__(self, title: str, description: str = "") -> bool:
        if _word_match(title, self.excludes):
            return False
        if _word_match(title, self.keywords):
            return True
        return self.scan_description and _word_match(description, self.keywords)


# ------------------------------------------------------------ location fit --
# Home base: Nairobi, Kenya (UTC+3). Edit the lists below if that changes.
#   yes   = location text says worldwide / anywhere / Africa / EMEA / Kenya
#   maybe = Europe/UK (time zone fits, but residency rules may apply),
#           no location stated, or a location we don't recognize
#   no    = text lists only regions that exclude Kenya (USA, Canada, LATAM...)

OPEN_TERMS = [
    "worldwide", "anywhere", "global", "globally", "international",
    "africa", "kenya", "emea", "mea", "any location", "all countries",
]
EUROPE_TERMS = [
    "europe", "european", "eu", "eea", "uk", "united kingdom", "ireland",
    "germany", "france", "spain", "portugal", "italy", "netherlands",
    "belgium", "austria", "switzerland", "poland", "czechia", "czech republic",
    "slovakia", "hungary", "romania", "bulgaria", "greece", "sweden", "norway",
    "denmark", "finland", "estonia", "latvia", "lithuania", "croatia", "serbia",
    "berlin", "munich", "hamburg", "frankfurt", "cologne", "stuttgart",
    "london", "dublin", "amsterdam", "vienna", "zurich", "barcelona", "madrid",
    "lisbon", "warsaw", "prague",
]
RESTRICTED_TERMS = [
    "us", "usa", "united states", "canada", "north america", "northern america",
    "americas", "latam", "latin america", "mexico", "brazil", "argentina",
    "colombia", "chile", "peru", "apac", "asia", "australia", "new zealand",
    "india", "philippines", "singapore", "japan",
]
FIT_RANK = {"yes": 0, "maybe": 1, "no": 2}

# "Anywhere in the US" contains "anywhere" but is the opposite of open.
_ANYWHERE_IN_REGION = re.compile(
    r"anywhere in (?:the )?(us|usa|united states|canada|europe|eu|uk|united kingdom|"
    r"north america|latam|latin america|apac|asia)\b"
)


def classify_location(location: str):
    """Return (fit, note) for a Nairobi-based candidate; fit is 'yes', 'maybe' or 'no'."""
    text = (location or "").lower().strip()
    m = _ANYWHERE_IN_REGION.search(text)
    if m:
        region = m.group(1)
        if region in ("europe", "eu", "uk", "united kingdom"):
            return "maybe", "Europe/UK only: time zone fits, residency rules may apply"
        return "no", f"restricted to {region.upper()}"
    if text in ("", "remote", "remote only", "n/a"):
        return "maybe", "no location stated"
    for term in OPEN_TERMS:
        if _word_match(text, [term]):
            return "yes", f"open: {term}"
    if _word_match(text, EUROPE_TERMS):
        return "maybe", "Europe/UK: time zone fits, residency rules may apply"
    if _word_match(text, RESTRICTED_TERMS):
        return "no", "restricted to regions that exclude Kenya"
    return "maybe", "unrecognized location, check the posting"


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _role_key(job: "Job"):
    company = _norm(job.company).split(" ")[0] if _norm(job.company) else ""
    return (company, _norm(job.title))


def flag_location_conflicts(jobs: List["Job"]) -> None:
    """
    Sources sometimes disagree about the same posting (one says "Anywhere in
    the World", another says "USA"). If the same role (company + title) has a
    'no' copy, downgrade its 'yes' copies to 'maybe' so a contradicted role
    never looks like an easy yes.
    """
    groups = {}
    for j in jobs:
        key = _role_key(j)
        if key[0] and key[1]:
            groups.setdefault(key, []).append(j)
    for group in groups.values():
        restricted = [j for j in group if j.fit == "no"]
        if not restricted:
            continue
        for j in group:
            if j.fit == "yes":
                j.fit = "maybe"
                j.fit_note = f"conflict: {restricted[0].source} lists this role as {restricted[0].location}"


# ---------------------------------------------------------------- sources --

def fetch_remotive(match: Matcher, search_terms: List[str]) -> List[Job]:
    jobs: List[Job] = []
    queries = [{"category": "data"}] + [{"search": t} for t in search_terms]
    for params in queries:
        try:
            r = requests.get("https://remotive.com/api/remote-jobs",
                             params=params, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            for j in r.json().get("jobs", []):
                title = j.get("title", "")
                description = _strip_html(j.get("description", ""))
                if not match(title, description):
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
        except (requests.RequestException, ValueError) as e:
            print(f"[Remotive] skipped {params}: {e}", file=sys.stderr)
        time.sleep(0.3)
    return jobs


def fetch_arbeitnow(match: Matcher) -> List[Job]:
    jobs: List[Job] = []
    for page in (1, 2, 3):
        try:
            r = requests.get("https://www.arbeitnow.com/api/job-board-api",
                             params={"page": page}, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json().get("data", [])
        except (requests.RequestException, ValueError) as e:
            print(f"[Arbeitnow] skipped page {page}: {e}", file=sys.stderr)
            break
        if not data:
            break
        for j in data:
            if not j.get("remote"):
                continue
            title = j.get("title", "")
            description = _strip_html(f"{j.get('description', '')} {' '.join(j.get('tags', []))}")
            if match(title, description):
                jobs.append(Job(
                    title=title,
                    company=j.get("company_name", ""),
                    location=j.get("location", "Remote"),
                    url=j.get("url", ""),
                    source="Arbeitnow",
                    posted=str(j.get("created_at", "")),
                    snippet=description[:200],
                ))
        time.sleep(0.3)
    return jobs


def fetch_jobicy(match: Matcher) -> List[Job]:
    jobs: List[Job] = []
    queries = [
        {"industry": "data-science", "count": 100},
        # Dedicated queries for regions a Nairobi-based candidate can work from.
        {"industry": "data-science", "geo": "anywhere", "count": 100},
        {"industry": "data-science", "geo": "emea", "count": 100},
    ]
    queries += [{"tag": t, "count": 50} for t in JOBICY_TAGS]
    for params in queries:
        try:
            r = requests.get("https://jobicy.com/api/v2/remote-jobs",
                             params=params, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            for j in r.json().get("jobs", []):
                title = j.get("jobTitle", "")
                description = _strip_html(j.get("jobExcerpt", "") or "")
                if match(title, description):
                    jobs.append(Job(
                        title=title,
                        company=j.get("companyName", ""),
                        location=j.get("jobGeo", "Remote"),
                        url=j.get("url", ""),
                        source="Jobicy",
                        posted=j.get("pubDate", ""),
                        snippet=description[:200],
                    ))
        except (requests.RequestException, ValueError) as e:
            print(f"[Jobicy] skipped {params}: {e}", file=sys.stderr)
        time.sleep(0.3)
    return jobs


def fetch_weworkremotely(match: Matcher) -> List[Job]:
    if feedparser is None:
        print("[WeWorkRemotely] skipped: run `pip install feedparser`", file=sys.stderr)
        return []
    jobs: List[Job] = []
    for feed_url in WWR_FEEDS:
        try:
            feed = feedparser.parse(feed_url, request_headers=HEADERS)
        except Exception as e:  # feedparser can raise on odd network failures
            print(f"[WeWorkRemotely] skipped {feed_url}: {e}", file=sys.stderr)
            continue
        if not feed.entries:
            print(f"[WeWorkRemotely] no entries from {feed_url}", file=sys.stderr)
            continue
        for entry in feed.entries:
            # WWR titles look like "Company: Role". Match on the role only,
            # otherwise a company called "Tableau" would match every posting.
            raw_title = entry.get("title", "")
            company, sep, role = raw_title.partition(":")
            if not sep:
                company, role = "", raw_title
            company, role = company.strip(), role.strip()
            description = _strip_html(entry.get("summary", ""))
            if match(role, description):
                jobs.append(Job(
                    title=role,
                    company=company,
                    location=entry.get("region", "") or "Remote",
                    url=entry.get("link", ""),
                    source="WeWorkRemotely",
                    posted=entry.get("published", ""),
                    snippet=description[:200],
                ))
        time.sleep(0.3)
    return jobs


# ---------------------------------------------- deeper: direct ATS lookups --

def greenhouse_company(slug: str, match: Matcher) -> List[Job]:
    """
    Pull open roles directly from a company's Greenhouse board.
    boards.greenhouse.io/gitlab -> slug = "gitlab"
    """
    jobs: List[Job] = []
    try:
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        for j in r.json().get("jobs", []):
            title = j.get("title", "")
            if match(title):
                jobs.append(Job(
                    title=title,
                    company=slug,
                    location=(j.get("location") or {}).get("name", ""),
                    url=j.get("absolute_url", ""),
                    source=f"Greenhouse:{slug}",
                ))
    except (requests.RequestException, ValueError) as e:
        print(f"[Greenhouse:{slug}] skipped: {e}", file=sys.stderr)
    return jobs


def lever_company(slug: str, match: Matcher) -> List[Job]:
    """Same idea as greenhouse_company() but for Lever-hosted boards."""
    jobs: List[Job] = []
    try:
        r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json",
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        for j in r.json():
            title = j.get("text", "")
            if match(title):
                jobs.append(Job(
                    title=title,
                    company=slug,
                    location=(j.get("categories") or {}).get("location", ""),
                    url=j.get("hostedUrl", ""),
                    source=f"Lever:{slug}",
                ))
    except (requests.RequestException, ValueError) as e:
        print(f"[Lever:{slug}] skipped: {e}", file=sys.stderr)
    return jobs


# ---------------------------------------------------------- state tracking --

def load_state(state_file: str) -> dict:
    """State file schema: {"seen": {url: first_seen_iso_timestamp, ...}}"""
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
    """Returns (new_jobs, updated_state). Does not mutate the input state."""
    seen = dict(state.get("seen", {}))
    now = datetime.now(timezone.utc).isoformat()
    new_jobs = []
    for j in jobs:
        key = j.url or f"{j.title}|{j.company}"
        if key not in seen:
            new_jobs.append(j)
            seen[key] = now
    return new_jobs, {"seen": seen}


# -------------------------------------------------------------------- main --

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
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keywords", nargs="+", default=DEFAULT_KEYWORDS,
                        help="Title keywords to match (default: data science / analysis / BI terms)")
    parser.add_argument("--exclude-terms", nargs="*", default=EXCLUDE_TITLE_TERMS,
                        help="Title terms that reject a job even if a keyword matched")
    parser.add_argument("--scan-descriptions", action="store_true",
                        help="Also accept jobs whose description (not title) contains a keyword. Noisy.")
    parser.add_argument("--remotive-searches", nargs="*", default=DEFAULT_REMOTIVE_SEARCHES,
                        help="Search terms sent to the Remotive API")
    parser.add_argument("--companies", nargs="*", default=[],
                        help="Greenhouse company slugs to check directly")
    parser.add_argument("--lever-companies", nargs="*", default=[],
                        help="Lever company slugs to check directly")
    parser.add_argument("--hide-restricted", action="store_true",
                        help="Drop jobs whose location is limited to regions that exclude Kenya (fit = no)")
    parser.add_argument("--output", default="remote_jobs.csv", help="Output CSV filename")
    parser.add_argument("--state-file", default="job_state.json",
                        help="JSON file tracking previously seen job URLs (default: job_state.json)")
    parser.add_argument("--all", action="store_true",
                        help="Write every matching job, not just ones new since the last run")
    parser.add_argument("--no-state", action="store_true",
                        help="Skip state tracking entirely (doesn't read or write the state file)")
    args = parser.parse_args()

    match = Matcher(args.keywords, args.exclude_terms, args.scan_descriptions)

    sources = [
        ("Remotive", lambda: fetch_remotive(match, args.remotive_searches)),
        ("Arbeitnow", lambda: fetch_arbeitnow(match)),
        ("Jobicy", lambda: fetch_jobicy(match)),
        ("We Work Remotely", lambda: fetch_weworkremotely(match)),
    ]
    sources += [(f"Greenhouse:{s}", lambda s=s: greenhouse_company(s, match)) for s in args.companies]
    sources += [(f"Lever:{s}", lambda s=s: lever_company(s, match)) for s in args.lever_companies]

    all_jobs: List[Job] = []
    for name, fetch in sources:
        print(f"Searching {name}...", end=" ", flush=True)
        found = dedupe(fetch())
        print(f"{len(found)} matching")
        all_jobs += found

    all_jobs = dedupe(all_jobs)
    for j in all_jobs:
        j.fit, j.fit_note = classify_location(j.location)
    flag_location_conflicts(all_jobs)
    if args.hide_restricted:
        before = len(all_jobs)
        all_jobs = [j for j in all_jobs if j.fit != "no"]
        print(f"Hid {before - len(all_jobs)} location-restricted roles")
    all_jobs.sort(key=lambda j: (FIT_RANK[j.fit], j.source))

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
        writer = csv.DictWriter(f, fieldnames=["fit", "fit_note", "title", "company", "location", "url", "source", "posted", "snippet"])
        writer.writeheader()
        for j in jobs_to_write:
            writer.writerow(asdict(j))

    if args.no_state:
        label = "matching roles (state untouched)"
    elif args.all:
        label = "matching roles (--all: state updated but not used to filter)"
    elif is_first_run:
        label = "matching roles (first run, everything is new)"
    else:
        label = "NEW roles since last run"

    print(f"\nFound {len(jobs_to_write)} {label}. Saved to {args.output}")
    counts = {k: sum(1 for j in jobs_to_write if j.fit == k) for k in ("yes", "maybe", "no")}
    print(f"Fit for Nairobi: {counts['yes']} yes, {counts['maybe']} maybe, {counts['no']} restricted")
    for j in jobs_to_write[:20]:
        flag = f"  <- {j.fit_note}" if j.fit_note.startswith("conflict") else ""
        print(f"  {j.fit.upper():5} [{j.source}] {j.title} — {j.company} ({j.location}){flag}")
    if len(jobs_to_write) > 20:
        print(f"  ...and {len(jobs_to_write) - 20} more in the CSV.")


if __name__ == "__main__":
    main()