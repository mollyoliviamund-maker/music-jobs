import os, sys, yaml, argparse
from typing import Dict, Any, List

from utils import load_seen, save_seen, append_csv
from adapters import (
    fetch_greenhouse, fetch_lever, fetch_workday,
    fetch_workable, fetch_icims, fetch_teamtailor,
    fetch_adp, fetch_successfactors, fetch_jobvite, fetch_pereless
)

FETCHERS = {
    "greenhouse": lambda slug: fetch_greenhouse(slug),
    "lever":      lambda slug: fetch_lever(slug),
}

DICT_FETCHERS = {
    "workday":         fetch_workday,
    "workable":        fetch_workable,
    "icims":           fetch_icims,
    "teamtailor":      fetch_teamtailor,
    "adp":             fetch_adp,
    "successfactors":  fetch_successfactors,
    "jobvite":         fetch_jobvite,
    "pereless":        fetch_pereless,
}

def run(platform_filter=None, company_filter=None):
    cfg_path = "companies.yaml"
    if not os.path.exists(cfg_path):
        print("[ERROR] companies.yaml not found", file=sys.stderr)
        return 2

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    seen = load_seen()
    total_new = 0

    # simple list-based platforms
    for plat in ["greenhouse", "lever"]:
        if plat not in cfg: continue
        if platform_filter and platform_filter != plat: continue
        slugs = cfg.get(plat) or []
        for slug in slugs:
            if not isinstance(slug, str) or not slug.strip():
                continue
            if company_filter and slug != company_filter:
                continue
            try:
                jobs = FETCHERS[plat](slug.strip())
            except Exception as e:
                print(f"[WARN] {plat}:{slug} failed -> {e}", file=sys.stderr)
                continue
            new_count = 0
            for j in jobs:
                key = f"{j['platform']}::{j['company']}::{j['job_id']}::{j['url']}"
                if key in seen:
                    continue
                seen.add(key)
                append_csv(j)
                total_new += 1
                new_count += 1
                print(f"[NEW] {j['company']} | {j['title']} | {j['url']}")
            print(f"[SUMMARY] {plat}:{slug} -> {new_count} new", file=sys.stderr)

    # dict-based platforms
    for plat, fetcher in DICT_FETCHERS.items():
        if plat not in cfg: continue
        if platform_filter and platform_filter != plat: continue
        entries = cfg.get(plat) or []
        for entry in entries:
            if not isinstance(entry, dict): continue
            cname = entry.get("company") or entry.get("tenant") or entry.get("host") or "unknown"
            if company_filter and company_filter not in {cname, entry.get("host"), entry.get("tenant")}:
                continue
            try:
                jobs = fetcher(entry)
            except Exception as e:
                print(f"[WARN] {plat}:{cname} failed -> {e}", file=sys.stderr)
                continue
            new_count = 0
            for j in jobs:
                key = f"{j['platform']}::{j['company']}::{j['job_id']}::{j['url']}"
                if key in seen:
                    continue
                seen.add(key)
                append_csv(j)
                total_new += 1
                new_count += 1
                print(f"[NEW] {j['company']} | {j['title']} | {j['url']}")
            print(f"[SUMMARY] {plat}:{cname} -> {new_count} new", file=sys.stderr)

    save_seen(seen)
    print(f"Done. New matches: {total_new}")
    return 0

def parse_args():
    ap = argparse.ArgumentParser(description="Multi-ATS Music Job Watcher")
    ap.add_argument("--platform", help="Limit to one platform (greenhouse, lever, workday, workable, icims, teamtailor, adp, successfactors, jobvite, pereless)")
    ap.add_argument("--company", help="Limit to one company slug/host/name")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    sys.exit(run(args.platform, args.company))
