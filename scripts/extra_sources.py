#!/usr/bin/env python3
"""Extra job sources for the free (no-Claude) engine.

Writes one JSON list of "leads" that feeds/<app>/pipeline.py --leads understands. The pipeline then
classifies, scores, checks India eligibility and de-duplicates them exactly like Claude's own leads.

Sources (each one is skipped quietly when its key is missing or it fails):
  * inbox/<app>/*.json   leads Claude's hourly monitor found (LinkedIn, Naukri ... via search) and saved here
  * The Muse             public API, no key (design roles; Motion Intelligence only)
  * Adzuna               free API key  -> repo secrets ADZUNA_APP_ID, ADZUNA_APP_KEY   (developer.adzuna.com)
  * Jooble               free API key  -> repo secret  JOOBLE_KEY                      (jooble.org/api/about)
  * Tavily search        free API key  -> repo secret  TAVILY_API_KEY                  (app.tavily.com, 1,000 searches/month)
                         searches the LinkedIn / Naukri / Indeed / ATS search index, 3 times a day

Pay is copied only when the source states it; Adzuna's "predicted" salaries are dropped.
Pages of linkedin.com, naukri.com, indeed.com and foundit.in are never fetched: only search-result titles are read.

Usage: python3 scripts/extra_sources.py qa|mi OUT.json [EXPORT.json]
EXPORT gets the leads found here (not the ones Claude saved), so Claude's monitor can import them when it runs.
"""
import glob, json, os, re, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UA = "JobIntelligenceFeed/1.0 (personal job search; GitHub Actions)"
NOW = datetime.now(timezone.utc)
ENV = os.environ.get

QUERIES = {
    "qa": dict(
        words=["QA tester", "game tester", "software tester", "manual tester", "QA engineer", "test engineer", "QA analyst", "game QA"],
        places=["Mumbai", "Navi Mumbai", "Thane", "India"],
        remote_words=["QA tester remote", "game tester remote"],
        search=[  # (query, domains)
            ("game tester OR QA tester jobs Mumbai", ["linkedin.com"]),
            ("software tester OR manual tester OR QA engineer jobs Mumbai", ["linkedin.com"]),
            ("game tester jobs India", ["linkedin.com", "naukri.com"]),
            ("QA tester manual testing jobs Mumbai", ["naukri.com", "indeed.com", "foundit.in", "instahyre.com", "hirist.tech"]),
            ("QA tester OR game tester India", ["job-boards.greenhouse.io", "jobs.lever.co", "jobs.ashbyhq.com", "apply.workable.com", "jobs.smartrecruiters.com"]),
        ],
    ),
    "mi": dict(
        words=["motion designer", "motion graphics designer", "product motion designer", "motion design lead", "creative technologist", "lottie animator", "rive animator", "UI animator"],
        places=["Bengaluru", "India"],
        remote_words=["motion designer remote", "senior motion designer remote"],
        search=[
            ("senior motion designer jobs Bengaluru India", ["linkedin.com"]),
            ("product motion designer OR UI motion designer jobs remote", ["linkedin.com"]),
            ("motion designer jobs India", ["naukri.com", "instahyre.com", "cutshort.io", "hirist.tech", "wellfound.com", "foundit.in"]),
            ("motion designer", ["jobs.ashbyhq.com", "job-boards.greenhouse.io", "jobs.lever.co", "apply.workable.com"]),
            ("Rive OR Lottie motion designer hiring", []),
        ],
    ),
}


def log(*a):
    print("[extra]", *a, file=sys.stderr, flush=True)


def http(url, data=None, headers=None, timeout=30):
    h = {"User-Agent": UA, "Accept": "application/json"}
    h.update(headers or {})
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def strip_html(s):
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def iso(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def lead(**kw):
    return {k: v for k, v in kw.items() if v not in (None, "", [])}


# ------------------------------------------------------------------ Claude inbox
def inbox(app):
    out = []
    cutoff = (NOW - timedelta(days=21)).strftime("%Y%m%d")
    for f in sorted(glob.glob(os.path.join(ROOT, "inbox", app, "*.json"))):
        if os.path.basename(f)[:8] < cutoff:
            continue
        try:
            rows = json.load(open(f, encoding="utf-8"))
        except Exception as e:
            log("inbox", f, e); continue
        for r in rows if isinstance(rows, list) else []:
            if isinstance(r, dict) and r.get("title") and r.get("company") and r.get("url"):
                out.append(r)
    log("inbox", len(out))
    return out


# ------------------------------------------------------------------ The Muse (keyless)
def themuse(app):
    if app != "mi":
        return []
    out = []
    for page in range(0, 4):
        q = urllib.parse.urlencode([("category", "Design and UX"), ("category", "Creative and Design"), ("page", page)])
        try:
            d = http("https://www.themuse.com/api/public/jobs?" + q)
        except Exception as e:
            log("themuse", e); break
        for j in d.get("results", []):
            t = j.get("name") or ""
            if not re.search(r"(?i)motion|animat|creative technolog|video design|brand design", t):
                continue
            locs = ", ".join(x.get("name", "") for x in j.get("locations") or [])
            out.append(lead(title=t, company=(j.get("company") or {}).get("name"), url=(j.get("refs") or {}).get("landing_page"),
                            source_id="themuse", source_name="The Muse", source_kind="board", posted_at=iso(j.get("publication_date")),
                            location=locs or "Not stated", remote=bool(re.search(r"(?i)flexible|remote", locs)),
                            desc=strip_html(j.get("contents"))[:4000], level=", ".join(x.get("name", "") for x in j.get("levels") or [])))
        if page + 1 >= d.get("page_count", 0):
            break
        time.sleep(0.5)
    log("themuse", len(out))
    return out


# ------------------------------------------------------------------ Adzuna (free key)
def adzuna(app):
    aid, akey = ENV("ADZUNA_APP_ID"), ENV("ADZUNA_APP_KEY")
    if not (aid and akey):
        return []
    Q = QUERIES[app]
    plans = [("in", w, p) for w in Q["words"] for p in Q["places"][-2:]] + [(c, w, "") for c in ("gb", "us") for w in Q["remote_words"]]
    out, seen = [], set()
    for country, what, where in plans:
        params = {"app_id": aid, "app_key": akey, "results_per_page": 50, "what": what, "max_days_old": 30, "sort_by": "date", "content-type": "application/json"}
        if where:
            params["where"] = where
        try:
            d = http(f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?" + urllib.parse.urlencode(params))
        except Exception as e:
            log("adzuna", country, what, e); continue
        for j in d.get("results", []):
            if j.get("id") in seen:
                continue
            seen.add(j.get("id"))
            loc = (j.get("location") or {}).get("display_name") or ""
            sal = None
            if j.get("salary_min") and str(j.get("salary_is_predicted")) != "1":  # never keep Adzuna's estimates
                cur = {"in": "INR", "gb": "GBP", "us": "USD"}[country]
                sal = dict(currency=cur, min=float(j["salary_min"]), max=float(j.get("salary_max") or j["salary_min"]), period="annual")
            text = (j.get("title") or "") + " " + (j.get("description") or "")
            out.append(lead(title=strip_html(j.get("title")), company=(j.get("company") or {}).get("display_name"), url=j.get("redirect_url"),
                            source_id="adzuna", source_name="Adzuna", source_kind="board", posted_at=iso(j.get("created")),
                            location=loc if country == "in" else (loc + (" (remote)" if re.search(r"(?i)remote", text) else "")),
                            remote=bool(re.search(r"(?i)\bremote\b|work from home|wfh", text)),
                            employment={"full_time": "full-time", "part_time": "part-time"}.get(j.get("contract_time"), ""),
                            desc=strip_html(j.get("description")), salary=sal))
        time.sleep(0.3)
    log("adzuna", len(out))
    return out


# ------------------------------------------------------------------ Jooble (free key)
def jooble(app):
    key = ENV("JOOBLE_KEY")
    if not key:
        return []
    Q = QUERIES[app]
    out, seen = [], set()
    for w in Q["words"][:6]:
        for p in Q["places"][:1] + ["India", "Remote"]:
            try:
                d = http("https://jooble.org/api/" + urllib.parse.quote(key), data={"keywords": w, "location": p, "page": "1"})
            except Exception as e:
                log("jooble", w, p, e); continue
            for j in d.get("jobs", []):
                u = j.get("link")
                if not u or u in seen:
                    continue
                seen.add(u)
                loc = j.get("location") or p
                out.append(lead(title=strip_html(j.get("title")), company=j.get("company"), url=u, source_id="jooble",
                                source_name="Jooble" + (f" ({j['source']})" if j.get("source") else ""), source_kind="board",
                                posted_at=iso(j.get("updated")), location=loc, remote=bool(re.search(r"(?i)remote|anywhere", loc + " " + (j.get("title") or ""))),
                                employment=(j.get("type") or "").lower(), desc=strip_html(j.get("snippet"))))
            time.sleep(0.3)
    log("jooble", len(out))
    return out


# ------------------------------------------------------------------ Tavily search index (free key)
LI_JOB = re.compile(r"^https?://([a-z]{2,3}\.)?(www\.)?linkedin\.com/jobs/view/", re.I)
ATS = re.compile(r"^https?://(job-boards\.greenhouse\.io|boards\.greenhouse\.io|jobs\.lever\.co|jobs\.ashbyhq\.com|apply\.workable\.com|jobs\.smartrecruiters\.com)/([^/?#]+)/", re.I)
BOARD = {"naukri.com": ("naukri", "Naukri (via search index)"), "indeed.com": ("indeed", "Indeed (via search index)"), "foundit.in": ("foundit", "foundit (via search index)"),
         "instahyre.com": ("instahyre", "Instahyre (via search index)"), "hirist.tech": ("hirist", "Hirist (via search index)"), "cutshort.io": ("cutshort", "Cutshort (via search index)"),
         "wellfound.com": ("wellfound", "Wellfound (via search index)")}
SKIP_TITLE = re.compile(r"(?i)^(back to jobs|return to|jobs? (in|at)|\d[\d,]* .*jobs|search results|job search)")


def parse_hit(h):
    url, title = h.get("url") or "", re.sub(r"\s+", " ", h.get("title") or "").strip()
    desc = re.sub(r"\s+", " ", h.get("content") or "").strip()[:300]
    if not url or not title or SKIP_TITLE.search(title):
        return None
    host = urllib.parse.urlparse(url).netloc.lower()
    if LI_JOB.match(url):
        m = re.match(r"^(?P<c>.+?) hiring (?P<t>.+?) in (?P<l>.+?)\s*\|\s*LinkedIn", title)
        if not m:
            return None
        return lead(title=m["t"], company=m["c"], url=url.split("?")[0], source_id="linkedin", source_name="LinkedIn Jobs (via search index)",
                    source_kind="search", location=m["l"], remote=bool(re.search(r"(?i)remote", m["l"])), desc=desc)
    a = ATS.match(url)
    if a:
        t = re.sub(r"(?i)^job application for\s+", "", title)
        t = re.split(r"\s+(?:at|@|[-|–])\s+", t)[0]
        return lead(title=t, company=a.group(2).replace("-", " ").title(), url=url, source_id="ats", source_name="Employer job board (via search index)",
                    source_kind="search", official=True, apply_url=url, desc=desc)
    for dom, (sid, sname) in BOARD.items():
        if dom in host:
            m = (re.match(r"^(?P<t>.+?)\s+[Jj]ob\s+in\s+(?P<l>.+?)\s+at\s+(?P<c>.+?)(?:\s*[-|].*)?$", title)
                 or re.match(r"^(?P<t>[^|]+?)\s+-\s+(?P<c>[^|]+?)\s+-\s+(?P<l>[^|]+?)(?:\s*\|.*)?$", title)
                 or re.match(r"^(?P<t>.+?)\s+at\s+(?P<c>[^|-]+?)(?:\s*[-|].*)?$", title))
            if not m or re.search(r"(?i)\bjobs\b|vacanc|openings", m["t"]):
                return None
            loc = m.groupdict().get("l") or ""
            if re.search(r"(?i)\d+\s*(?:to|-)\s*\d+\s*years?", loc):
                loc = ""
            return lead(title=m["t"].strip(), company=m["c"].strip(), url=url, source_id=sid, source_name=sname, source_kind="search",
                        location=loc, remote=bool(re.search(r"(?i)remote|work from home", title + " " + desc)), desc=desc)
    return None


def tavily(app):
    key = ENV("TAVILY_API_KEY")
    if not key or ENV("FORCE_SEARCH") != "1" and NOW.hour not in (2, 10, 18):  # 3 times a day keeps it inside the free 1,000 searches/month
        return []
    out = []
    for q, domains in QUERIES[app]["search"]:
        body = {"query": q, "max_results": 20, "search_depth": "basic", "time_range": "month"}
        if domains:
            body["include_domains"] = domains
        try:
            d = http("https://api.tavily.com/search", data=body, headers={"Authorization": "Bearer " + key})
        except Exception as e:
            log("tavily", q, e); continue
        for h in d.get("results", []):
            L = parse_hit(h)
            if L:
                out.append(L)
        time.sleep(0.5)
    log("tavily", len(out))
    return out


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in QUERIES:
        sys.exit(__doc__)
    app, path = sys.argv[1], sys.argv[2]
    rows, seen, mine = [], set(), []
    for fn in (inbox, themuse, adzuna, jooble, tavily):
        try:
            got = fn(app)
        except Exception as e:
            log(fn.__name__, "failed", e); got = []
        for r in got:
            k = (r.get("url") or "").split("?")[0]
            if r.get("title") and r.get("company") and k and k not in seen:
                seen.add(k); rows.append(r)
                if fn is not inbox:
                    mine.append(r)
    json.dump(rows, open(path, "w", encoding="utf-8"), ensure_ascii=False)
    if len(sys.argv) > 3:
        exp = sys.argv[3]
        try:  # keep earlier finds for 10 days so Claude can pick them up after a pause
            old = [r for r in json.load(open(exp, encoding="utf-8")) if (r.get("found_at") or "") >= (NOW - timedelta(days=10)).strftime("%Y-%m-%d")]
        except Exception:
            old = []
        stamp = NOW.strftime("%Y-%m-%dT%H:%MZ")
        new_urls = {r["url"] for r in mine}
        merged = [dict(r, found_at=stamp) for r in mine] + [r for r in old if r.get("url") not in new_urls]
        os.makedirs(os.path.dirname(exp), exist_ok=True)
        json.dump(merged[:600], open(exp, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
    log("total leads", len(rows))


if __name__ == "__main__":
    main()
