import json, yaml, os, sys, datetime
from typing import List, Dict, Any, Optional

import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import (
    load_seen, save_seen, append_csv, job_matches_music, mk_row
)

# ---------- HTTP session with retries & headers ----------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 MusicJobs/1.1"
        ),
        "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
    })
    retry = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()
REQ_TIMEOUT = 35

# ---------- helpers ----------
def _slugify(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.startswith("#"):
        return None
    return s

def _safe_get(url: str) -> Optional[requests.Response]:
    try:
        r = SESSION.get(url, timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            print(f"[WARN] GET {url} -> HTTP {r.status_code}", file=sys.stderr)
            return None
        return r
    except Exception as e:
        print(f"[WARN] GET {url} failed: {e}", file=sys.stderr)
        return None

# ---------- Greenhouse ----------
def fetch_greenhouse(company_slug: str) -> List[Dict[str, Any]]:
    """
    Uses the official public Boards API:
    https://boards-api.greenhouse.io/v1/boards/<slug>/jobs?content=true
    """
    out: List[Dict[str, Any]] = []
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs?content=true"
    r = _safe_get(url)
    if not r:
        print(f"[WARN] greenhouse:{company_slug} API fetch failed", file=sys.stderr)
        return out

    try:
        data = r.json()
        jobs = data.get("jobs", []) or []
    except Exception:
        print(f"[WARN] greenhouse:{company_slug} invalid JSON body", file=sys.stderr)
        return out

    for job in jobs:
        title = job.get("title") or ""
        job_id = str(job.get("id") or "")
        abs_url = job.get("absolute_url") or ""
        # location: join office names if present
        offices = job.get("offices") or []
        location = ", ".join([o.get("name", "") for o in offices if isinstance(o, dict)]) or ""
        # content contains HTML description when content=true
        desc = job.get("content") or ""

        matched_on = None
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            matched_on = "title_or_description"

        if not matched_on:
            continue

        # Greenhouse boards API exposes updated_at; posted_at varies by tenant
        posted_iso = (job.get("updated_at") or "").replace(" ", "T")  # GH returns 'YYYY-MM-DD HH:MM:SS'
        if posted_iso and not posted_iso.endswith("Z"):
            posted_iso += "Z"

        out.append(mk_row(company_slug, "greenhouse", title, location, job_id, abs_url, posted_iso, matched_on))
    return out


# ---------- Lever ----------
def fetch_lever(company_slug: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    r = _safe_get(url)
    if not r:
        print(f"[WARN] lever:{company_slug} API fetch failed", file=sys.stderr)
        return out

    try:
        postings = r.json()
        if not isinstance(postings, list):
            print(f"[WARN] lever:{company_slug} unexpected JSON shape (not list)", file=sys.stderr)
            return out
    except Exception:
        print(f"[WARN] lever:{company_slug} invalid JSON body", file=sys.stderr)
        return out

    for p in postings or []:
        if not isinstance(p, dict):
            continue

        title = p.get("text") or p.get("title") or ""
        location = (p.get("categories") or {}).get("location") or ""
        job_id = p.get("id") or p.get("leverId") or p.get("hostedJobId") or ""
        apply_url = (
            p.get("hostedUrl") or p.get("applyUrl") or (p.get("urls") or {}).get("apply") or ""
        )
        desc = p.get("descriptionPlain") or p.get("description") or ""
        matched_on = None

        if job_matches_music(f"{title}\n{location}\n{desc}"):
            matched_on = "title_or_description"
        elif apply_url:
            jr = _safe_get(apply_url)
            if jr and job_matches_music(jr.text or ""):
                matched_on = "description_html"

        if not matched_on:
            continue

        iso = ""
        created_at = p.get("createdAt") or p.get("created_at")
        if created_at:
            try:
                iso = (
                    datetime.datetime.utcfromtimestamp(int(created_at) / 1000)
                    .replace(microsecond=0)
                    .isoformat() + "Z"
                )
            except Exception:
                iso = ""

        out.append(mk_row(company_slug, "lever", title, location, str(job_id), apply_url, iso, matched_on))
    return out

# ---------- Dispatcher ----------
FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
}

def run(platform_filter: Optional[str] = None, company_filter: Optional[str] = None) -> int:
    cfg_path = "companies.yaml"
    if not os.path.exists(cfg_path):
        print("[ERROR] companies.yaml not found", file=sys.stderr)
        return 2

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    seen = load_seen()
    total_new = 0
    summary = {}

    items = list(cfg.items()) if isinstance(cfg, dict) else []
    for platform, companies in items:
        if platform not in FETCHERS:
            # Ignore unsupported keys like comments or future adapters
            continue
        if platform_filter and platform != platform_filter:
            continue
        fetcher = FETCHERS[platform]

        if not isinstance(companies, list):
            print(f"[WARN] {platform} list malformed; skipping", file=sys.stderr)
            continue

        per_platform_new = 0

        for raw_slug in companies:
            slug = _slugify(raw_slug)
            if not slug:
                continue
            if company_filter and slug != company_filter:
                continue

            try:
                jobs = fetcher(slug)
            except Exception as e:
                print(f"[WARN] {platform}:{slug} failed -> {e}", file=sys.stderr)
                continue

            per_company_new = 0
            for j in jobs:
                key = f"{j['platform']}::{j['company']}::{j['job_id']}::{j['url']}"
                if key in seen:
                    continue
                seen.add(key)
                append_csv(j)
                total_new += 1
                per_platform_new += 1
                per_company_new += 1
                print(f"[NEW] {j['company']} | {j['title']} | {j['url']}")

            print(f"[INFO] {platform}:{slug} -> {per_company_new} new", file=sys.stderr)

        summary[platform] = per_platform_new

    save_seen(seen)
    # Summary
    for plat, cnt in summary.items():
        print(f"[SUMMARY] {plat}: {cnt} new", file=sys.stderr)
    print(f"Done. New matches: {total_new}")
    return 0

def parse_args():
    ap = argparse.ArgumentParser(description="Music job watcher")
    ap.add_argument("--platform", choices=list(FETCHERS.keys()), help="Limit to one platform")
    ap.add_argument("--company", help="Limit to one company slug")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    sys.exit(run(args.platform, args.company))
