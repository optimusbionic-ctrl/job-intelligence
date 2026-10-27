#!/usr/bin/env python3
"""Run one feed (qa or mi) without Claude.

1. Fetch fresh roles from the official job-board and employer ATS APIs (feeds/<app>/pipeline.py).
2. Compare them with the stored database and plan the changes (feeds/<app>/sync.py).
3. Apply the plan to db/<app>/<collection>/<id>.json.
4. Write the static site data: site/<app>/data.json and site/<app>/text/<id>.json.

Standard library only. Usage: python3 scripts/run_feed.py qa|mi [--site-only]
"""
import glob, json, os, shutil, subprocess, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEEP_LOGS = 48        # sync_logs kept in the repo (about two days of hourly runs)
KEEP_ALERTS = 400     # alerts kept in the repo
SITE_LOGS = 12        # what the app reads
SITE_ALERTS = 150

LABEL = {"qa": "QA Intelligence", "mi": "Motion Intelligence"}


def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)


def load_coll(base, coll):
    out = {}
    for f in glob.glob(os.path.join(base, coll, "*.json")):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        if isinstance(d, dict) and set(d) == {"data"} and isinstance(d["data"], dict):
            d = d["data"]
        out[os.path.basename(f)[:-5]] = d
    return out


def run_feed(app, db, work):
    feed = os.path.join(ROOT, "feeds", app)
    out, plan = os.path.join(work, "out"), os.path.join(work, "plan")
    cmd = [sys.executable, os.path.join(feed, "pipeline.py"), "--out", out, "--existing", db,
           "--companies", os.path.join(feed, "companies.json"),
           "--benchmarks", os.path.join(feed, "benchmarks.json")]
    log("pipeline:", app)
    subprocess.run(cmd, check=True, timeout=40 * 60)
    log("sync:", app)
    subprocess.run([sys.executable, os.path.join(feed, "sync.py"), "--out", out, "--existing", db, "--plan", plan],
                   check=True, timeout=10 * 60)
    n = 0
    for f in glob.glob(os.path.join(plan, "docs", "*.json")):
        name = os.path.basename(f)[:-5]
        if "__" not in name:
            continue
        coll, doc_id = name.split("__", 1)
        d = json.load(open(f, encoding="utf-8"))
        if isinstance(d, dict) and set(d) == {"data"} and isinstance(d["data"], dict):
            d = d["data"]
        os.makedirs(os.path.join(db, coll), exist_ok=True)
        with open(os.path.join(db, coll, doc_id + ".json"), "w", encoding="utf-8") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1, sort_keys=True)
        n += 1
    summary = json.load(open(os.path.join(plan, "summary.json")))
    log(f"applied {n} documents", json.dumps(summary))
    return summary


def prune(db):
    for coll, keep, key in (("sync_logs", KEEP_LOGS, "started_at"), ("alerts", KEEP_ALERTS, "created_at")):
        docs = load_coll(db, coll)
        order = sorted(docs, key=lambda k: str(docs[k].get(key) or ""), reverse=True)
        for k in order[keep:]:
            os.remove(os.path.join(db, coll, k + ".json"))


def build_site(app, db, site):
    os.makedirs(site, exist_ok=True)
    bundle = {}
    for path in sorted(glob.glob(os.path.join(db, "*"))):
        coll = os.path.basename(path)
        if os.path.isdir(path) and coll != "job_text":
            bundle[coll] = load_coll(db, coll)
    for coll, keep, key in (("sync_logs", SITE_LOGS, "started_at"), ("alerts", SITE_ALERTS, "created_at")):
        docs = bundle.get(coll, {})
        order = sorted(docs, key=lambda k: str(docs[k].get(key) or ""), reverse=True)[:keep]
        bundle[coll] = {k: docs[k] for k in order}
    system = dict(bundle.get("meta", {}).get("system") or {})
    system["monitor"] = {
        "trigger_id": "github-actions",
        "name": f"{LABEL[app]} feed (GitHub Actions)",
        "cadence": "every hour",
        "cron": "17 * * * * (UTC)",
        "host": "GitHub Actions",
        "notifications": "none",
    }
    bundle.setdefault("meta", {})["system"] = system
    bundle["_built_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(site, "data.json"), "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, ensure_ascii=False, separators=(",", ":"))
    text_dir = os.path.join(site, "text")
    shutil.rmtree(text_dir, ignore_errors=True)
    os.makedirs(text_dir)
    jobs = bundle.get("jobs", {})
    for f in glob.glob(os.path.join(db, "job_text", "*.json")):
        if os.path.basename(f)[:-5] in jobs:
            shutil.copy(f, text_dir)
    log(f"site: {len(jobs)} roles, {len(bundle.get('companies', {}))} companies,"
        f" {os.path.getsize(os.path.join(site, 'data.json')) // 1024} KB")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in LABEL:
        sys.exit(__doc__)
    app = sys.argv[1]
    db = os.path.join(ROOT, "db", app)
    site = os.path.join(ROOT, "site", app)
    if "--site-only" not in sys.argv:
        with tempfile.TemporaryDirectory() as work:
            try:
                run_feed(app, db, work)
            except Exception as e:  # keep the last good data and still publish the site
                log(f"feed failed, keeping previous data: {e!r}")
        prune(db)
    build_site(app, db, site)


if __name__ == "__main__":
    main()
