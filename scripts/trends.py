#!/usr/bin/env python3
"""Live hiring trends: this month vs the same month last year, from published data only.

Sources (all free, no keys):
  * Naukri JobSpeak (Info Edge investor PDFs): India white-collar index, overall + industries + metros, monthly.
    Each report carries 13 months, so a year of reports gives exact year-on-year values for every month.
  * Indeed Hiring Lab job postings index (github.com/hiring-lab/job_postings_tracker): daily, by occupation
    sector, for the US, UK, Canada, Australia, Germany and France. Feb 2020 = 100, seasonally adjusted.
  * Hacker News "Who is hiring?" threads (hn.algolia.com API): every monthly thread, counting posts that
    mention the role. Small numbers, but exact and same-role.
  * This tracker's own roles: counted by posting month, and a daily count of confirmed-open roles.
  * research/<app>_reports.json: function-level figures (e.g. "Software engineering and QA") that are only
    published in report text. A monthly Claude task adds them with a verbatim quote and the source link.

Nothing is estimated: a value missing at the source stays missing.
Usage: python3 scripts/trends.py qa|mi    (writes export/trends_<app>.json and db/<app>/meta/trends.json)
"""
import csv, datetime as dt, glob, io, json, os, re, subprocess, sys, tempfile, time, urllib.parse, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UA = "JobIntelligenceFeed/1.0 (personal job search; GitHub Actions)"
NOW = dt.datetime.now(dt.timezone.utc)
MON = {m: i + 1 for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}

CFG = {
    "mi": dict(
        jobspeak_industries=["Media/Dotcom/Entertainment", "Advertising/PR/MR/Events", "Internet/Ecommerce", "IT-Software/Software Services"],
        jobspeak_cities=["Bangalore/Bengaluru", "Mumbai", "Pune", "Hyderabad/Secunderabad", "Delhi / NCR"],
        indeed=[("US", "Arts & Entertainment"), ("GB", "Arts & Entertainment"), ("CA", "Arts & Entertainment"), ("AU", "Arts & Entertainment"),
                ("DE", "Arts & Entertainment"), ("US", "Media & Communications"), ("US", "Software Development")],
        hn=[("Design roles", ['designer', '"motion design"', 'animator']), ("Motion / animation", ['"motion design"', '"motion designer"', 'animator', 'animation'])],
        role="motion and design",
    ),
    "qa": dict(
        jobspeak_industries=["IT-Software/Software Services", "Internet/Ecommerce", "Media/Dotcom/Entertainment", "BPO/ITES"],
        jobspeak_cities=["Mumbai", "Pune", "Bangalore/Bengaluru", "Hyderabad/Secunderabad", "Delhi / NCR"],
        indeed=[("US", "Software Development"), ("GB", "Software Development"), ("CA", "Software Development"), ("AU", "Software Development"),
                ("US", "Arts & Entertainment")],
        hn=[("QA / testing roles", ['"QA engineer"', '"QA tester"', '"quality assurance"', 'SDET', '"test engineer"'])],
        role="QA and testing",
    ),
}


def log(*a):
    print("[trends]", *a, file=sys.stderr, flush=True)


def get(url, timeout=60, binary=False):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "replace")


def ym(d):
    return f"{d.year:04d}-{d.month:02d}"


def shift(ymstr, n):
    y, m = map(int, ymstr.split("-")); m += n
    while m < 1: m += 12; y -= 1
    while m > 12: m -= 12; y += 1
    return f"{y:04d}-{m:02d}"


def pct(a, b):
    return round((a / b - 1) * 100, 1) if a is not None and b not in (None, 0) else None


# ------------------------------------------------------------------ Naukri JobSpeak (Info Edge PDFs)
JS_PAGE = "https://www.infoedge.in/InvestorRelations/NaukriJobSpeak"


def _label_ym(lab):
    m = re.match(r"([A-Za-z]{3})[A-Za-z]*['’`]?(\d{2})", lab)
    return f"20{m.group(2)}-{MON[m.group(1).lower()]:02d}" if m and m.group(1).lower() in MON else None


def parse_jobspeak(text, report_ym):
    out = {"overall": {}, "industries": {}, "cities": {}}
    hdr = re.search(r"Industry\s+((?:[A-Za-z]{3}[a-z]*['’`]\s?\d{2}\s+){12}[A-Za-z]{3}[a-z]*['’`]\s?\d{2})", text)
    months = None
    if hdr:
        labs = re.findall(r"[A-Za-z]{3}[a-z]*['’`]\s?\d{2}", hdr.group(1))
        months = [_label_ym(l.replace(" ", "")) for l in labs]
    if not months or None in months or len(months) != 13:
        months = [shift(report_ym, i - 12) for i in range(13)]
    if months[-1] != report_ym:
        log("jobspeak header end", months[-1], "!= report", report_ym)
    m = re.search(r"Overall\s+((?:\d{3,5}\s+){12}\d{3,5})", text)
    if m:
        vals = [int(v) for v in m.group(1).split()]
        out["overall"] = dict(zip(months, vals))
    for line in text.splitlines():
        r = re.match(r"^\s*([A-Za-z][A-Za-z0-9/&,.()\- ]+?)\s{2,}((?:\d{2,6}\s+){12}\d{2,6})\s+([↑↓])?\s*(-?[\d.]+)\s*%", line)
        if r and not r.group(1).strip().lower().startswith(("overall", "industry")):
            vals = [int(v) for v in r.group(2).split()]
            out["industries"][r.group(1).strip()] = dict(zip(months, vals))
            continue
        c = re.search(r"([A-Za-z][A-Za-z/ .()\-]+?)\s+(\d{3,5})\s+(\d{3,5})\s+(-?[\d.]+)%\s+([↑↓])?\s*(-?[\d.]+)\s*%", line)
        if c:
            name = re.split(r"\s{2,}", c.group(1).strip())[-1]
            out["cities"][name] = {shift(report_ym, -12): int(c.group(2)), report_ym: int(c.group(3))}
    return out


def jobspeak():
    cache_dir = os.path.join(ROOT, "research", "jobspeak"); os.makedirs(cache_dir, exist_ok=True)
    try:
        page = get(JS_PAGE)
    except Exception as e:
        log("jobspeak page", e); page = ""
    links = sorted(set(re.findall(r'href="(/pdfs/News_Events_pdfs/Naukri-Jobspeak-([A-Za-z]+)-(\d{4})\.pdf)"', page, re.I)))
    reports = []
    for path, mon, yr in links:
        k = mon[:3].lower()
        if k not in MON: continue
        reports.append((f"{yr}-{MON[k]:02d}", path))
    reports.sort(reverse=True)
    series = {"overall": {}, "industries": {}, "cities": {}}
    used = []
    for rep_ym, path in reversed(reports[:26]):  # oldest first, so newer reports overwrite revised values
        cf = os.path.join(cache_dir, rep_ym + ".json")
        if os.path.exists(cf):
            data = json.load(open(cf))
        else:
            try:
                pdf = get("https://www.infoedge.in" + path, timeout=120, binary=True)
                with tempfile.TemporaryDirectory() as td:
                    p = os.path.join(td, "r.pdf"); open(p, "wb").write(pdf)
                    txt = subprocess.run(["pdftotext", "-layout", p, "-"], capture_output=True, text=True, timeout=120).stdout
                data = parse_jobspeak(txt, rep_ym) if len(txt) > 500 else {"overall": {}, "industries": {}, "cities": {}}
                data["source"] = "https://www.infoedge.in" + path
                json.dump(data, open(cf, "w"), indent=0)
                time.sleep(1)
            except Exception as e:
                log("jobspeak pdf", path, e); continue
        if data.get("overall"):
            used.append(dict(month=rep_ym, url=data.get("source") or "https://www.infoedge.in" + path))
        for k in ("overall",):
            series[k].update(data.get(k) or {})
        for k in ("industries", "cities"):
            for name, vals in (data.get(k) or {}).items():
                if k == "cities":
                    name = re.split(r"\s{2,}", name)[-1].strip()  # the PDF table puts the state before the city
                series[k].setdefault(name, {}).update(vals)
    log("jobspeak reports", len(used), "months", len(series["overall"]))
    return series, used


# ------------------------------------------------------------------ Indeed Hiring Lab
IH = "https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/master/{c}/job_postings_by_sector_{c}.csv"


def indeed(pairs):
    out = {}
    for c in sorted({c for c, _ in pairs}):
        want = {s for cc, s in pairs if cc == c}
        try:
            text = get(IH.format(c=c), timeout=180)
        except Exception as e:
            log("indeed", c, e); continue
        daily = {}
        for r in csv.DictReader(io.StringIO(text)):
            if r.get("variable") == "total postings" and r.get("display_name") in want:
                try: daily.setdefault(r["display_name"], {})[r["date"]] = float(r["indeed_job_postings_index"])
                except Exception: pass
        for s, d in daily.items():
            last = max(d); ld = dt.date.fromisoformat(last)
            ly = (ld - dt.timedelta(days=364)).isoformat()  # same weekday a year earlier
            monthly = {}
            for k, v in d.items():
                monthly.setdefault(k[:7], []).append(v)
            monthly = {k: round(sum(v) / len(v), 1) for k, v in monthly.items()}
            out[f"{c}|{s}"] = dict(country=c, sector=s, last_date=last, last=d[last], year_ago=d.get(ly), yoy=pct(d[last], d.get(ly)),
                                   month_ago=d.get((ld - dt.timedelta(days=28)).isoformat()), mom=pct(d[last], d.get((ld - dt.timedelta(days=28)).isoformat())),
                                   monthly={k: monthly[k] for k in sorted(monthly)[-26:]},
                                   source=f"https://github.com/hiring-lab/job_postings_tracker/blob/master/{c}/job_postings_by_sector_{c}.csv")
    log("indeed series", len(out))
    return out


# ------------------------------------------------------------------ Hacker News "Who is hiring?"
def hn(groups):
    try:
        r = json.loads(get("https://hn.algolia.com/api/v1/search_by_date?tags=story,author_whoishiring&hitsPerPage=200"))
    except Exception as e:
        log("hn", e); return {}
    threads = {}
    for h in r.get("hits", []):
        m = re.search(r"Who is hiring\? \((\w+) (\d{4})\)", h.get("title") or "", re.I)
        if m and m.group(1)[:3].lower() in MON:
            threads[f"{m.group(2)}-{MON[m.group(1)[:3].lower()]:02d}"] = h["objectID"]
    months = sorted(threads)[-25:]
    out = {"total": {}, "groups": {g: {} for g, _ in groups}, "threads": {k: f"https://news.ycombinator.com/item?id={threads[k]}" for k in months}}

    def count(sid, q):
        u = "https://hn.algolia.com/api/v1/search?" + urllib.parse.urlencode({"tags": f"comment,story_{sid}", "query": q, "hitsPerPage": 1000,
                                                                               "typoTolerance": "false", "attributesToRetrieve": "parent_id,story_id", "attributesToHighlight": ""})
        hits = json.loads(get(u)).get("hits", [])
        return {x["objectID"] for x in hits if str(x.get("parent_id")) == str(sid)}  # top-level posts only (one per company)
    for mo in months:
        sid = threads[mo]
        try:
            top = json.loads(get(f"https://hn.algolia.com/api/v1/items/{sid}", timeout=90))
            out["total"][mo] = len([c for c in top.get("children", []) if c.get("text")])
            for g, qs in groups:
                ids = set()
                for q in qs:
                    ids |= count(sid, q)
                out["groups"][g][mo] = len(ids)
            time.sleep(0.3)
        except Exception as e:
            log("hn", mo, e)
    log("hn months", len(out["total"]))
    return out


# ------------------------------------------------------------------ this tracker
def tracker(app):
    db = os.path.join(ROOT, "db", app)
    by_month = {}
    for f in glob.glob(os.path.join(db, "jobs", "*.json")):
        try:
            j = json.load(open(f, encoding="utf-8")); j = j.get("data", j) if "title" not in j else j
        except Exception:
            continue
        if j.get("status") == "excluded":
            continue
        d = j.get("posted_at")  # first-seen dates would only show when the tracker started, so they are not counted
        if not d: continue
        by_month[d[:7]] = by_month.get(d[:7], 0) + 1
    vfile = os.path.join(db, "meta", "verify.json")
    live = None
    if os.path.exists(vfile):
        v = json.load(open(vfile)); v = v.get("data", v)
        live = sum(1 for x in (v.get("map") or {}).values() if x.get("s") in ("verified", "active", "lead"))
    hist_f = os.path.join(ROOT, "research", f"{app}_open_daily.json")
    hist = json.load(open(hist_f)) if os.path.exists(hist_f) else {}
    if live is not None:
        hist[NOW.strftime("%Y-%m-%d")] = live
        os.makedirs(os.path.dirname(hist_f), exist_ok=True)
        json.dump(dict(sorted(hist.items())[-800:]), open(hist_f, "w"), indent=0)
    return dict(by_month={k: by_month[k] for k in sorted(by_month)[-25:]}, open_daily=dict(sorted(hist.items())[-400:]), live_now=live)


def reports(app):
    f = os.path.join(ROOT, "research", f"{app}_reports.json")
    try:
        rows = json.load(open(f, encoding="utf-8"))
        return [r for r in rows if r.get("source_url") and r.get("quote")]
    except Exception:
        return []


def main():
    app = sys.argv[1]
    cfg = CFG[app]
    exp = os.path.join(ROOT, "export", f"trends_{app}.json")
    prev = {}
    if os.path.exists(exp):
        try: prev = json.load(open(exp))
        except Exception: prev = {}
    fresh = prev.get("updated_at") and (NOW - dt.datetime.fromisoformat(prev["updated_at"].replace("Z", "+00:00"))).total_seconds() < 20 * 3600
    out = dict(prev) if fresh and os.environ.get("FORCE_TRENDS") != "1" else {}
    if not out:
        js, used = jobspeak()
        out["jobspeak"] = dict(overall=dict(sorted(js["overall"].items())[-26:]),
                               industries={k: dict(sorted(v.items())[-26:]) for k, v in js["industries"].items() if k in cfg["jobspeak_industries"]},
                               cities={k: dict(sorted(v.items())[-26:]) for k, v in js["cities"].items() if k in cfg["jobspeak_cities"]},
                               reports=used[-14:], page=JS_PAGE)
        out["indeed"] = indeed(cfg["indeed"])
        out["hn"] = hn(cfg["hn"])
        out["updated_at"] = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    out["tracker"] = tracker(app)
    out["reports"] = reports(app)
    out["role"] = cfg["role"]
    out["tracker_updated_at"] = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    os.makedirs(os.path.dirname(exp), exist_ok=True)
    json.dump(out, open(exp, "w"), ensure_ascii=False)
    mdir = os.path.join(ROOT, "db", app, "meta"); os.makedirs(mdir, exist_ok=True)
    json.dump(out, open(os.path.join(mdir, "trends.json"), "w"), ensure_ascii=False)
    log(app, "jobspeak months", len(out.get("jobspeak", {}).get("overall", {})), "indeed", len(out.get("indeed", {})), "hn", len((out.get("hn") or {}).get("total", {})))


if __name__ == "__main__":
    main()
