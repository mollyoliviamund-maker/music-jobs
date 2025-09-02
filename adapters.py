from typing import List, Dict, Any, Optional
import sys, json, datetime, re
import requests
from bs4 import BeautifulSoup

# --- put near the top with the other imports ---
import re, warnings
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import job_matches_music, mk_row

# --------- shared HTTP session ----------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 MusicJobs/2.0"
        ),
        "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
        "Content-Type": "application/json",
    })
    retry = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()
REQ_TIMEOUT = 35

def _warn(msg: str):
    print(msg, file=sys.stderr)

# ---------------- Greenhouse ----------------
def fetch_greenhouse(slug: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    r = SESSION.get(url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] greenhouse:{slug} -> HTTP {r.status_code}")
        return out
    try:
        jobs = (r.json() or {}).get("jobs", []) or []
    except Exception:
        _warn(f"[WARN] greenhouse:{slug} invalid JSON")
        return out
    for j in jobs:
        title = j.get("title") or ""
        job_id = str(j.get("id") or "")
        abs_url = j.get("absolute_url") or ""
        offices = j.get("offices") or []
        location = ", ".join([o.get("name","") for o in offices if isinstance(o, dict)]) or ""
        desc = j.get("content") or ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            posted_iso = (j.get("updated_at") or "").replace(" ", "T")
            if posted_iso and not posted_iso.endswith("Z"):
                posted_iso += "Z"
            out.append(mk_row(slug, "greenhouse", title, location, job_id, abs_url, posted_iso, "title_or_description"))
    return out

# ---------------- Lever ----------------
def fetch_lever(slug: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = SESSION.get(url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] lever:{slug} -> HTTP {r.status_code}")
        return out
    try:
        postings = r.json()
        if not isinstance(postings, list):
            return out
    except Exception:
        _warn(f"[WARN] lever:{slug} invalid JSON")
        return out
    for p in postings:
        title = p.get("text") or p.get("title") or ""
        location = (p.get("categories") or {}).get("location") or ""
        job_id = p.get("id") or p.get("leverId") or p.get("hostedJobId") or ""
        apply_url = p.get("hostedUrl") or p.get("applyUrl") or (p.get("urls") or {}).get("apply") or ""
        desc = p.get("descriptionPlain") or p.get("description") or ""
        matched = None
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            matched = "title_or_description"
        elif apply_url:
            jr = SESSION.get(apply_url, timeout=REQ_TIMEOUT)
            if jr.status_code < 400 and job_matches_music(jr.text or ""):
                matched = "description_html"
        if matched:
            iso = ""
            created_at = p.get("createdAt") or p.get("created_at")
            if created_at:
                try:
                    iso = (
                        datetime.datetime.utcfromtimestamp(int(created_at)/1000)
                        .replace(microsecond=0).isoformat() + "Z"
                    )
                except Exception:
                    iso = ""
            out.append(mk_row(slug, "lever", title, location, str(job_id), apply_url, iso, matched))
    return out

# ---------------- Workday CxS ----------------
# ---- Workday helpers (add these above fetch_workday) ----
def _dedupe_keep_order(items):
    seen = set()
    out = []
    for x in items:
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _wd_scrape_sites_from_html(host: str, paths: list[str]) -> list[str]:
    candidates = []
    for path in paths:
        try:
            r = SESSION.get(f"https://{host}{path}", timeout=REQ_TIMEOUT)
            if r.status_code >= 400:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            # Look for /en-XX/<SiteToken>/ patterns
            for a in soup.select("a[href]"):
                href = a.get("href") or ""
                m = re.search(r"/en-[A-Z]{2}/([^/]+)/", href)
                if m:
                    candidates.append(m.group(1))
        except Exception:
            continue
    return _dedupe_keep_order(candidates)

def _wd_discover_sites(host: str, tenant: str, provided: str | None) -> list[str]:
    seeds = [provided] if provided else []
    # Common defaults (both cases)
    defaults = [
        "Careers", "careers", "External", "external",
        "Career", "career", "Jobs", "jobs",
        "US", "usa", "NA", "na", "NorthAmerica", "northamerica",
        "Campus", "campus", "Students", "students", "EarlyCareers", "earlycareers"
    ]
    seeds += defaults

    # Try config endpoint (not all tenants expose this)
    try:
        cfg = SESSION.get(f"https://{host}/wday/cxs/{tenant}/config", timeout=REQ_TIMEOUT)
        if cfg.status_code < 400:
            try:
                j = cfg.json() or {}
                # VERY tenant-specific; grab anything that looks like a site token-ish string
                # e.g. j.get("branding", {}).get("sites") -> [{"name": "Careers"}, ...]
                sites = []
                branding = j.get("branding") or {}
                for v in branding.values():
                    if isinstance(v, list):
                        for it in v:
                            if isinstance(it, dict):
                                for k, val in it.items():
                                    if isinstance(val, str) and 2 <= len(val) <= 40 and "/" not in val:
                                        sites.append(val)
                seeds += sites
            except Exception:
                pass
    except Exception:
        pass

    # Scrape a couple of public pages for /en-XX/<Site>/
    seeds += _wd_scrape_sites_from_html(host, ["", "/en-US", "/en-GB", "/careers", "/career"])

    # Underscore/hyphen variants for provided sites if present
    if provided and ("_" in provided or "-" in provided):
        seeds += [provided.replace("_", "-"), provided.replace("-", "_")]

    return _dedupe_keep_order(seeds)[:20]


# ---- REPLACE your existing fetch_workday with this version ----
def fetch_workday(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip()
    tenant = (entry.get("tenant") or "").strip()
    site_hint = (entry.get("site") or "").strip()
    company = entry.get("company") or tenant or host
    if not (host and tenant):
        _warn(f"[WARN] workday bad entry (need host+tenant): {entry}")
        return out

    def try_site(site_token: str) -> List[Dict[str, Any]]:
        url = f"https://{host}/wday/cxs/{tenant}/{site_token}/jobs"
        payload = {"appliedFacets": {}, "limit": 50, "offset": 0, "searchText": "music"}
        # Some tenants require Referer/Origin/Accept-Language
        headers = {
            "Referer": f"https://{host}/en-US/{site_token}",
            "Origin": f"https://{host}",
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = SESSION.post(url, json=payload, headers=headers, timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            _warn(f"[WARN] workday:{tenant}/{site_token} -> HTTP {r.status_code}")
            return []
        try:
            data = r.json()
        except Exception:
            _warn(f"[WARN] workday:{tenant}/{site_token} invalid JSON")
            return []
        jobs = data.get("jobPostings") or data.get("jobs") or []
        found = []
        for j in jobs or []:
            title = (j.get("title") or "").strip()
            # URL
            urlp = j.get("externalPath") or j.get("externalUrl") or j.get("url") or ""
            if urlp and urlp.startswith("/"):
                urlp = f"https://{host}{urlp}"
            # Location
            loc = ""
            locs = j.get("locations") or j.get("bulletFields") or []
            if isinstance(locs, list):
                loc = ", ".join(str(x) for x in locs if x)
            elif isinstance(locs, str):
                loc = locs
            # Description (best effort)
            desc = " ".join([
                j.get("shortDescription") or "",
                j.get("jobPostingInfo", {}).get("jobDescription", "")
            ]).strip()
            if job_matches_music(f"{title}\n{loc}\n{desc}"):
                jid = j.get("id") or j.get("jobId") or j.get("externalId") or ""
                posted = (j.get("postedOn") or j.get("startDate") or "").replace(" ", "T")
                if posted and not posted.endswith("Z"):
                    posted += "Z"
                found.append(mk_row(company, "workday", title, loc, str(jid), urlp, posted, "title_or_description"))
        return found

    tried = set()
    # 1) Try provided site first (if any)
    if site_hint:
        tried.add(site_hint)
        out.extend(try_site(site_hint))
        if out:
            return out

    # 2) Discover & try candidates
    for cand in _wd_discover_sites(host, tenant, site_hint):
        if cand in tried:
            continue
        tried.add(cand)
        res = try_site(cand)
        if res:
            out.extend(res)
            break  # first working site is enough

    return out

# --- Headless Workday adapter (Playwright) ---
def fetch_workday_headless(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Headless Workday adapter that *sniffs* the portal's own jobs XHR to discover:
      - variant: /wday/cx/ or /wday/cxs/
      - tenant:   actually used by the portal (may differ in case)
      - site:     the real site token
    Then it posts the same endpoint with searchText="music".
    """
    from playwright.sync_api import sync_playwright
    import re, json
    out: List[Dict[str, Any]] = []

    host = (entry.get("host") or "").strip()
    tenant_hint = (entry.get("tenant") or "").strip()
    site_hint = (entry.get("site") or "").strip()
    company = entry.get("company") or tenant_hint or host
    if not host:
        _warn(f"[WARN] workday(headless) missing host: {entry}")
        return out

    # Keep first discovered endpoint here.
    sniff = {"variant": None, "tenant": None, "site": None}

    def parse_jobs_url(url: str):
        # Match .../wday/(cx|cxs)/<tenant>/<site>/jobs...
        m = re.search(r"/wday/(cxs|cx)/([^/]+)/([^/]+)/jobs", url)
        if m:
            sniff["variant"], sniff["tenant"], sniff["site"] = m.group(1), m.group(2), m.group(3)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=True, user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 MusicJobs/HD2"
        ))
        page = context.new_page()

        # Watch all responses to capture the portal's own jobs call.
        def on_response(resp):
            url = resp.url
            if "/wday/cx" in url or "/wday/cxs" in url:
                parse_jobs_url(url)
        page.on("response", on_response)

        # 1) Land on the host root to establish cookies
        for url in [f"https://{host}/", f"https://{host}/en-US", f"https://{host}/career", f"https://{host}/careers"]:
            try:
                page.goto(url, wait_until="networkidle", timeout=45000)
                break
            except Exception:
                continue

        # 2) Try a few obvious portal pages to trigger the jobs XHR
        candidate_paths = []
        # collect hrefs from the DOM that look promising
        try:
            hrefs = page.eval_on_selector_all(
                "a[href]", "els => els.map(a => a.getAttribute('href'))"
            ) or []
        except Exception:
            hrefs = []

        for h in hrefs:
            if not h:
                continue
            if h.startswith("//"):  # protocol-relative
                h = "https:" + h
            if h.startswith("/"):
                h = f"https://{host}{h}"
            if host not in h:
                continue
            # keep likely career/search links
            if any(k in h.lower() for k in ["career", "careers", "jobs", "search", "/en-"]):
                candidate_paths.append(h)

        # Always try common en-US paths first
        hardcoded = [
            f"https://{host}/en-US/Careers",
            f"https://{host}/en-US/External",
            f"https://{host}/en-US/Jobs",
            f"https://{host}/en-US/{site_hint}" if site_hint else None,
        ]
        candidate_paths = [u for u in hardcoded if u] + candidate_paths[:20]

        for u in candidate_paths[:25]:
            if sniff["variant"]:
                break
            try:
                page.goto(u, wait_until="networkidle", timeout=45000)
            except Exception:
                continue

        # 3) If we still didn't sniff anything, try a direct POST with common variants
        def try_direct(variant: str, tenant: str, site: str):
            return page.evaluate(
                """async ({variant, tenant, site}) => {
                    const payload = {appliedFacets:{}, limit:50, offset:0, searchText:"music"};
                    const res = await fetch(`/wday/${variant}/${tenant}/${site}/jobs`, {
                      method: 'POST',
                      headers: {'content-type':'application/json'},
                      body: JSON.stringify(payload),
                      credentials: 'same-origin'
                    });
                    return {ok: res.ok, status: res.status};
                }""",
                {"variant": variant, "tenant": tenant, "site": site},
            )

        if not sniff["variant"]:
            tenants = [sniff["tenant"], tenant_hint, (tenant_hint or "").lower(),
                       (tenant_hint or "").upper()]
            sites = [sniff["site"], site_hint, "Careers", "External", "Jobs", "US", "Students", "Campus"]
            tenants = [t for t in tenants if t]
            sites = [s for s in sites if s]
            tried = set()
            for v in ("cxs", "cx"):
                for t in tenants:
                    for s in sites:
                        key = (v, t, s)
                        if key in tried: 
                            continue
                        tried.add(key)
                        try:
                            res = try_direct(v, t, s)
                            if res and res.get("ok"):
                                sniff["variant"], sniff["tenant"], sniff["site"] = v, t, s
                                break
                        except Exception:
                            continue
                    if sniff["variant"]:
                        break
                if sniff["variant"]:
                    break

        # 4) If still nothing, bail.
        if not sniff["variant"]:
            context.close(); browser.close()
            _warn(f"[WARN] workday({company}) sniff failed (no jobs endpoint found)")
            return out

        # 5) Query the discovered endpoint for music jobs
        resp = page.evaluate(
            """async ({variant, tenant, site}) => {
                const payload = {appliedFacets:{}, limit:50, offset:0, searchText:"music"};
                const r = await fetch(`/wday/${variant}/${tenant}/${site}/jobs`, {
                  method:'POST',
                  headers:{'content-type':'application/json'},
                  body: JSON.stringify(payload),
                  credentials:'same-origin'
                });
                if (!r.ok) return null;
                return await r.json();
            }""",
            sniff,
        )

        if resp:
            jobs = (resp.get("jobPostings") or resp.get("jobs") or [])
            for j in jobs:
                title = (j.get("title") or "").strip()
                urlp = j.get("externalPath") or j.get("externalUrl") or j.get("url") or ""
                if urlp and urlp.startswith("/"):
                    urlp = f"https://{host}{urlp}"
                # location
                loc = ""
                locs = j.get("locations") or j.get("bulletFields") or []
                if isinstance(locs, list):
                    loc = ", ".join(str(x) for x in locs if x)
                elif isinstance(locs, str):
                    loc = locs
                desc = " ".join([
                    j.get("shortDescription") or "",
                    j.get("jobPostingInfo", {}).get("jobDescription", "")
                ]).strip()
                if job_matches_music(f"{title}\n{loc}\n{desc}"):
                    jid = j.get("id") or j.get("jobId") or j.get("externalId") or ""
                    posted = (j.get("postedOn") or j.get("startDate") or "").replace(" ", "T")
                    if posted and not posted.endswith("Z"): posted += "Z"
                    out.append(mk_row(company, "workday", title, loc, str(jid), urlp, posted, "title_or_description"))

        context.close()
        browser.close()
    return out


# ---------------- Workable ----------------
def fetch_workable(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    account = (entry.get("account") or "").strip()
    company = entry.get("company") or account
    if not account:
        return out

    def try_account(acc: str) -> List[Dict[str, Any]]:
        url = f"https://apply.workable.com/api/v3/accounts/{acc}/jobs?state=published"
        r = SESSION.get(url, timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            _warn(f"[WARN] workable:{acc} -> HTTP {r.status_code}")
            return []
        try:
            jobs = (r.json() or {}).get("results", []) or []
        except Exception:
            _warn(f"[WARN] workable:{acc} invalid JSON")
            return []
        rows: List[Dict[str, Any]] = []
        for j in jobs:
            title = j.get("title") or ""
            url = j.get("url") or ""
            loc = j.get("location") or {}
            location = ", ".join([loc.get("city",""), loc.get("region",""), loc.get("country","")]).strip(", ").replace(",,", ",")
            desc = (j.get("description") or "") if isinstance(j.get("description"), str) else ""
            if job_matches_music(f"{title}\n{location}\n{desc}"):
                jid = j.get("id") or j.get("shortcode") or ""
                rows.append(mk_row(company, "workable", title, location, str(jid), url, "", "title_or_description"))
        return rows

    # try original, then a version without hyphens (common)
    out.extend(try_account(account))
    if not out and "-" in account:
        out.extend(try_account(account.replace("-", "")))
    return out


# ---------------- iCIMS (HTML) ----------------
def _text(el) -> str:
    try:
        return el.get_text(strip=True)
    except Exception:
        return ""

def fetch_icims(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out

    # Main search page
    search_url = f"https://{host}/jobs/search"
    r = SESSION.get(search_url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] icims:{host} -> HTTP {r.status_code}")
        return out

    soup = BeautifulSoup(r.text, "lxml")
    job_links = set()
    for a in soup.select("a[href*='/jobs/']"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://{host}{href}"
        job_links.add(href)

    for job_url in list(job_links)[:120]:
        jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
        if jr.status_code >= 400:
            continue
        jsoup = BeautifulSoup(jr.text, "lxml")
        title = _text(jsoup.select_one("h1")) or _text(jsoup.select_one("h2")) or _text(jsoup.select_one(".iCIMS_JobTitle"))
        loc_el = jsoup.find("li", class_="iCIMS_JobLocation") or jsoup.find("span", class_="jobLocation")
        location = _text(loc_el)
        desc = jsoup.get_text(" ", strip=True)[:20000]
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            m = re.search(r"/jobs/(\d+)", job_url)
            jid = m.group(1) if m else job_url
            out.append(mk_row(company, "icims", title, location, jid, job_url, "", "html_text"))
    return out


# ---------------- Teamtailor (JSON-LD in HTML) ----------------
def fetch_teamtailor(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    url = f"https://{host}/jobs"
    r = SESSION.get(url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] teamtailor:{host} -> HTTP {r.status_code}")
        return out
    soup = BeautifulSoup(r.text, "lxml")
    # JSON-LD blocks may contain job data
    scripts = soup.find_all("script", type="application/ld+json")
    jobs = []
    for sc in scripts:
        try:
            data = json.loads(sc.string or "")
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                jobs.append(data)
            elif isinstance(data, list):
                for it in data:
                    if isinstance(it, dict) and it.get("@type") == "JobPosting":
                        jobs.append(it)
        except Exception:
            continue
    for j in jobs:
        title = j.get("title") or ""
        url = j.get("url") or ""
        location = ""
        loc = j.get("jobLocation", {})
        if isinstance(loc, dict):
            addr = loc.get("address", {})
            location = ", ".join([addr.get("addressLocality",""), addr.get("addressRegion",""), addr.get("addressCountry","")]).strip(", ")
        desc = j.get("description") or ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            jid = j.get("identifier") or url
            out.append(mk_row(company, "teamtailor", title, location, str(jid), url, "", "jsonld"))
    return out

# ---------------- ADP Workforce Now (HTML) ----------------
def fetch_adp(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    # Try common public listing page(s)
    urls = [f"https://{host}/career-center/search", f"https://{host}/career-center"]
    for url in urls:
        r = SESSION.get(url, timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href*='job?'] , a[href*='/job/'] , a[href*='positions']"):
            job_url = a.get("href")
            if not job_url: continue
            if job_url.startswith("/"):
                job_url = f"https://{host}{job_url}"
            jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
            if jr.status_code >= 400: continue
            jsoup = BeautifulSoup(jr.text, "lxml")
            title = (jsoup.find("h1") or jsoup.find("h2") or {}).get_text(strip=True) if jsoup else ""
            location = ""
            desc = jsoup.get_text(" ", strip=True)[:20000] if jsoup else ""
            if job_matches_music(f"{title}\n{location}\n{desc}"):
                out.append(mk_row(company, "adp", title, location, job_url, job_url, "", "html_text"))
    return out

# ---------------- SAP SuccessFactors (HTML) ----------------
def fetch_successfactors(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    # Try main careers domain; tenants vary a lot, so we parse listing page then details
    r = SESSION.get(f"https://{host}", timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] successfactors:{host} -> HTTP {r.status_code}")
        return out
    soup = BeautifulSoup(r.text, "lxml")
    links = set(a.get("href") for a in soup.select("a[href*='job']") if a.get("href"))
    for href in list(links)[:80]:
        job_url = href
        if job_url.startswith("/"):
            job_url = f"https://{host}{job_url}"
        jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
        if jr.status_code >= 400: continue
        jsoup = BeautifulSoup(jr.text, "lxml")
        title = (jsoup.find("h1") or jsoup.find("h2") or {}).get_text(strip=True) if jsoup else ""
        location = ""
        desc = jsoup.get_text(" ", strip=True)[:20000] if jsoup else ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            out.append(mk_row(company, "successfactors", title, location, job_url, job_url, "", "html_text"))
    return out

# ---------------- Jobvite (HTML) ----------------
def fetch_jobvite(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    r = SESSION.get(f"https://{host}/", timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] jobvite:{host} -> HTTP {r.status_code}")
        return out
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("a[href*='jobs?'] , a[href*='/job/'] , a[href*='?jvi='] , a[href*='/jobs/']"):
        job_url = a.get("href")
        if not job_url: continue
        if job_url.startswith("/"):
            job_url = f"https://{host}{job_url}"
        jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
        if jr.status_code >= 400: continue
        jsoup = BeautifulSoup(jr.text, "lxml")
        title = (jsoup.find("h1") or jsoup.find("h2") or {}).get_text(strip=True) if jsoup else ""
        location = ""
        desc = jsoup.get_text(" ", strip=True)[:20000] if jsoup else ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            out.append(mk_row(company, "jobvite", title, location, job_url, job_url, "", "html_text"))
    return out

# ---------------- Pereless / Submit4Jobs (HTML) ----------------
def fetch_pereless(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    r = SESSION.get(f"https://{host}/", timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] pereless:{host} -> HTTP {r.status_code}")
        return out
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("a[href*='JobDetails'] , a[href*='?fulldesc='] , a[href*='/job/'] , a[href*='?pos=']"):
        job_url = a.get("href")
        if not job_url: continue
        if job_url.startswith("/"):
            job_url = f"https://{host}{job_url}"
        jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
        if jr.status_code >= 400: continue
        jsoup = BeautifulSoup(jr.text, "lxml")
        title = (jsoup.find("h1") or jsoup.find("h2") or jsoup.title or {})
        title = title.get_text(strip=True) if hasattr(title, "get_text") else str(title)
        location = ""
        desc = jsoup.get_text(" ", strip=True)[:20000]
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            out.append(mk_row(company, "pereless", title, location, job_url, job_url, "", "html_text"))
    return out
