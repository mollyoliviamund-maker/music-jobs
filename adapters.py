from typing import List, Dict, Any, Optional
import sys, json, datetime, re
import requests
from bs4 import BeautifulSoup

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
# ---- helpers for Workday ----
import re
from bs4 import BeautifulSoup

def _wd_discover_sites(host: str, tenant: str, seeds: list[str] | None = None) -> list[str]:
    """
    Try to discover valid Workday 'site' tokens by scanning public pages.
    We look for URLs like /en-US/<SiteToken>/ and test them against CxS.
    """
    candidates = []
    seeds = seeds or []
    # Common defaults
    defaults = ["Careers", "External", "Career", "Jobs", "US"]
    for s in seeds + defaults:
        if s and s not in candidates:
            candidates.append(s)

    def scrape_for_sites(url: str):
        try:
            r = SESSION.get(url, timeout=REQ_TIMEOUT)
            if r.status_code >= 400:
                return
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a[href*='/en-US/']"):
                href = a.get("href") or ""
                m = re.search(r"/en-US/([^/]+)/", href)
                if m:
                    site = m.group(1)
                    if site and site not in candidates:
                        candidates.append(site)
        except Exception:
            pass

    # Scan a couple of public pages
    scrape_for_sites(f"https://{host}/")
    scrape_for_sites(f"https://{host}/en-US")
    scrape_for_sites(f"https://{host}/en-US/Careers")
    return candidates[:12]  # keep it small

def fetch_workday(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip()
    tenant = (entry.get("tenant") or "").strip()
    site = (entry.get("site") or "").strip()
    company = entry.get("company") or tenant or host
    if not (host and tenant):
        _warn(f"[WARN] workday bad entry (need host+tenant): {entry}")
        return out

    def try_site(sitename: str) -> List[Dict[str, Any]]:
        url = f"https://{host}/wday/cxs/{tenant}/{sitename}/jobs"
        payload = {"appliedFacets": {}, "limit": 50, "offset": 0, "searchText": "music"}
        r = SESSION.post(url, data=json.dumps(payload), timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            _warn(f"[WARN] workday:{tenant}/{sitename} -> HTTP {r.status_code}")
            return []
        try:
            data = r.json()
        except Exception:
            _warn(f"[WARN] workday:{tenant}/{sitename} invalid JSON")
            return []
        jobs = data.get("jobPostings") or data.get("jobs") or []
        results: List[Dict[str, Any]] = []
        for j in jobs:
            title = (j.get("title") or "").strip()
            # Build URL
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
            # Desc (best-effort)
            desc = " ".join([
                j.get("shortDescription") or "",
                j.get("jobPostingInfo", {}).get("jobDescription", "")
            ]).strip()

            if job_matches_music(f"{title}\n{loc}\n{desc}"):
                jid = j.get("id") or j.get("jobId") or j.get("externalId") or ""
                posted = (j.get("postedOn") or j.get("startDate") or "").replace(" ", "T")
                if posted and not posted.endswith("Z"):
                    posted += "Z"
                results.append(mk_row(company, "workday", title, loc, str(jid), urlp, posted, "title_or_description"))
        return results

    # If a site is provided, try it first.
    tried = set()
    if site:
        tried.add(site)
        out.extend(try_site(site))
        if out:
            return out  # good

    # Auto-discover potential sites and try them
    for s in _wd_discover_sites(host, tenant, seeds=[site] if site else []):
        if s in tried:
            continue
        tried.add(s)
        res = try_site(s)
        if res:
            out.extend(res)
            break  # first working site is enough

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
