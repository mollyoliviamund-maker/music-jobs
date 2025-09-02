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
def fetch_workday(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip()
    tenant = (entry.get("tenant") or "").strip()
    site = (entry.get("site") or "").strip()
    company = entry.get("company") or tenant or host
    if not (host and tenant and site):
        _warn(f"[WARN] workday bad entry: {entry}")
        return out
    api = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    offset, limit = 0, 50
    while True:
        payload = {"appliedFacets":{}, "limit":limit, "offset":offset, "searchText":"music"}
        r = SESSION.post(api, data=json.dumps(payload), timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            _warn(f"[WARN] workday:{tenant}/{site} -> HTTP {r.status_code}")
            break
        try:
            data = r.json()
        except Exception:
            _warn(f"[WARN] workday:{tenant}/{site} invalid JSON")
            break
        jobs = data.get("jobPostings") or data.get("jobs") or []
        if not jobs:
            break
        for j in jobs:
            title = (j.get("title") or "").strip()
            url = j.get("externalPath") or j.get("externalUrl") or j.get("url") or ""
            if url and url.startswith("/"):
                url = f"https://{host}{url}"
            loc = ""
            locs = j.get("locations") or j.get("bulletFields") or []
            if isinstance(locs, list):
                loc = ", ".join([str(x) for x in locs if x])
            elif isinstance(locs, str):
                loc = locs
            desc = " ".join([j.get("shortDescription") or "", j.get("jobPostingInfo", {}).get("jobDescription", "")]).strip()
            if job_matches_music(f"{title}\n{loc}\n{desc}"):
                jid = j.get("id") or j.get("jobId") or j.get("externalId") or ""
                posted = (j.get("postedOn") or j.get("startDate") or "").replace(" ", "T")
                if posted and not posted.endswith("Z"):
                    posted += "Z"
                out.append(mk_row(company, "workday", title, loc, str(jid), url, posted, "title_or_description"))
        if len(jobs) < limit:
            break
        offset += limit
    return out

# ---------------- Workable ----------------
def fetch_workable(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    account = (entry.get("account") or "").strip()
    company = entry.get("company") or account
    if not account:
        return out
    url = f"https://apply.workable.com/api/v3/accounts/{account}/jobs?state=published"
    r = SESSION.get(url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] workable:{account} -> HTTP {r.status_code}")
        return out
    try:
        jobs = (r.json() or {}).get("results", []) or []
    except Exception:
        _warn(f"[WARN] workable:{account} invalid JSON")
        return out
    for j in jobs:
        title = j.get("title") or ""
        url = j.get("url") or ""
        loc = j.get("location") or {}
        location = ", ".join([loc.get("city",""), loc.get("region",""), loc.get("country","")]).strip(", ").replace(",,", ",")
        desc = " ".join([j.get("shortlink"), j.get("description","")]) if isinstance(j.get("description"), str) else ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            jid = j.get("id") or j.get("shortcode") or ""
            out.append(mk_row(company, "workable", title, location, str(jid), url, "", "title_or_description"))
    return out

# ---------------- iCIMS (HTML) ----------------
def fetch_icims(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    host = (entry.get("host") or "").strip().rstrip("/")
    company = entry.get("company") or host
    if not host:
        return out
    url = f"https://{host}/jobs/search"
    r = SESSION.get(url, timeout=REQ_TIMEOUT)
    if r.status_code >= 400:
        _warn(f"[WARN] icims:{host} -> HTTP {r.status_code}")
        return out
    soup = BeautifulSoup(r.text, "lxml")
    cards = soup.select("div.iCIMS_JobList li, div.iCIMS_JobsTable a, a[href*='/jobs/']")
    seen_urls = set()
    for a in soup.select("a[href*='/jobs/']"):
        job_url = a.get("href")
        if not job_url: continue
        if job_url.startswith("/"):
            job_url = f"https://{host}{job_url}"
        if job_url in seen_urls: continue
        seen_urls.add(job_url)
        jr = SESSION.get(job_url, timeout=REQ_TIMEOUT)
        if jr.status_code >= 400: continue
        jsoup = BeautifulSoup(jr.text, "lxml")
        title = (jsoup.select_one("h1") or jsoup.select_one("h2") or {}).get_text(strip=True) if jsoup else ""
        location = (jsoup.find("li", class_="iCIMS_JobLocation") or {}).get_text(strip=True) if jsoup else ""
        desc = jsoup.get_text(" ", strip=True)[:20000] if jsoup else ""
        if job_matches_music(f"{title}\n{location}\n{desc}"):
            # try to grab req id from URL
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
