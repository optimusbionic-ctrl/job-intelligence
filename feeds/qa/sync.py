#!/usr/bin/env python3
"""
QA Intelligence — sync planner
==================================
Compares a fresh pipeline run (OUT dir) with the current database dump
(EXISTING dir, as written by `ArtifactData list ... out_dir`) and produces
an idempotent write plan:

  plan/creates_NNN.json   ArtifactData batch bodies (<=50 writes, create-only)
  plan/updates.json       docs that already exist: [{collection, doc_id, file}]
                          -> read each doc's version with ArtifactData get, then set with if_version
  plan/summary.json       counts + noteworthy items for the run's final message
  plan/message.md         the push-notification text (empty when nothing noteworthy)

Usage:
  python3 qa_sync.py --out OUT --existing EXISTING --plan PLAN
                     [--settings settings.json] [--apps APPS_DIR] [--first-run]
"""
import shutil
import argparse, datetime as dt, glob, json, os, re

NOW = dt.datetime.now(dt.timezone.utc)
ISO = lambda d: d.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
ATS_IDS = {"greenhouse", "ashby", "lever", "smartrecruiters"}
DEF_W = dict(skill=30, company=15, seniority=20, ai=10, comp=10, location=15)
DEF_RULES = dict(minFit=80, over90=True, salaryDisclosed=True, recruiterFound=True, deadlines=True, closures=True, allNew=True, digest="run", tracks=["game", "software"])


def load_coll(base, coll):
    out = {}
    for f in glob.glob(os.path.join(base or "", coll, "*.json")):
        try:
            d = json.load(open(f))
        except Exception:
            continue
        # ArtifactData dumps may wrap the body; accept {data:{...}} or the bare document
        if isinstance(d, dict) and "data" in d and isinstance(d["data"], dict) and "id" not in d:
            d = d["data"]
        out[os.path.basename(f)[:-5]] = d
    return out


def parse(v):
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def fit(scores, w):
    tot = sum(w.values()) or 1
    return round(sum((scores or {}).get(k, 50) * w[k] for k in w) / tot)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--existing", required=True)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--settings")
    ap.add_argument("--apps")
    a = ap.parse_args()
    os.makedirs(a.plan, exist_ok=True)
    for f in glob.glob(os.path.join(a.plan, "*")):
        if os.path.isdir(f): shutil.rmtree(f)
        else: os.remove(f)
    docs_dir = os.path.join(a.plan, "docs"); os.makedirs(docs_dir, exist_ok=True)

    settings = json.load(open(a.settings)) if a.settings and os.path.exists(a.settings) else {}
    if isinstance(settings, dict) and "data" in settings and isinstance(settings["data"], dict):
        settings = settings["data"]
    w = {**DEF_W, **(settings.get("weights") or {})}
    rules = {**DEF_RULES, **(settings.get("alerts") or {})}
    apps = load_coll(a.apps, "") if False else {}
    if a.apps and os.path.isdir(a.apps):
        for f in glob.glob(os.path.join(a.apps, "**", "*.json"), recursive=True):
            try:
                d = json.load(open(f)); d = d.get("data", d) if isinstance(d, dict) else d
                apps[os.path.basename(f)[:-5]] = d
            except Exception:
                pass

    new_jobs = load_coll(a.out, "jobs")
    new_text = load_coll(a.out, "job_text")
    new_comp = load_coll(a.out, "companies")
    new_cont = load_coll(a.out, "contacts")
    new_bm = load_coll(a.out, "salary_benchmarks")
    new_links = load_coll(a.out, "company_links")
    new_checks = load_coll(a.out, "web_checks")
    run = json.load(open(os.path.join(a.out, "sync_log.json")))
    fx = json.load(open(os.path.join(a.out, "fx.json"))).get("fx") if os.path.exists(os.path.join(a.out, "fx.json")) else None

    ex_jobs = load_coll(a.existing, "jobs")
    ex_comp = load_coll(a.existing, "companies")
    ex_cont = load_coll(a.existing, "contacts")
    ex_bm = load_coll(a.existing, "salary_benchmarks")
    ex_links = load_coll(a.existing, "company_links")
    ex_checks = load_coll(a.existing, "web_checks")
    ex_meta = load_coll(a.existing, "meta")
    ex_alerts = load_coll(a.existing, "alerts")
    first_run = not ex_jobs
    verify_prev = (ex_meta.get("verify") or {}).get("map", {})
    system_prev = ex_meta.get("system") or {}

    creates, updates = [], []

    def put(coll, doc_id, body, exists):
        path = os.path.join(docs_dir, f"{coll.replace('/', '__')}__{doc_id}.json")
        json.dump(body, open(path, "w"), ensure_ascii=False)
        (updates if exists else creates).append(dict(op="set", collection=coll, doc_id=doc_id, file_path=os.path.abspath(path)))

    ok_sources = {k for k, v in (run.get("source_status") or {}).items() if v.get("ok")}
    seen_path = os.path.join(a.out, "seen_source_ids.json")
    seen_ids = set(json.load(open(seen_path))) if os.path.exists(seen_path) else set()
    ex_path = os.path.join(a.out, "excluded_ids.json")
    excluded_ids = json.load(open(ex_path)) if os.path.exists(ex_path) else {}
    new_ids, changed_ids = [], []
    for jid, j in new_jobs.items():
        prev = ex_jobs.get(jid)
        if prev:
            j["first_seen"] = prev.get("first_seen") or j.get("first_seen")
            if prev.get("content_hash") == j.get("content_hash"):
                continue
            changed_ids.append(jid)
            put("jobs", jid, j, True)
        else:
            new_ids.append(jid)
            put("jobs", jid, j, False)
            if jid in new_text:
                put("job_text", jid, new_text[jid], False)

    # verification overlay
    vmap = {}
    for jid, j in new_jobs.items():
        vmap[jid] = dict(s=j["status"], lv=ISO(NOW))
    closed_now = []
    for jid, j in ex_jobs.items():
        if jid in vmap:
            continue
        pv = verify_prev.get(jid, {})
        st = pv.get("s", j.get("status"))
        srcs = {s.get("id") for s in (j.get("sources") or [])}
        if st in ("closed",):
            vmap[jid] = pv; continue
        if jid in excluded_ids:
            vmap[jid] = dict(s="excluded", r=excluded_ids[jid], lv=ISO(NOW)); continue
        if j.get("status") in ("lead",) or st in ("lead", "stale"):
            age = (NOW - (parse(j.get("first_seen")) or NOW)).days
            vmap[jid] = dict(s="stale" if age > 21 else "lead", lv=pv.get("lv") or j.get("last_verified"))
            continue
        seen_here = any(str(x.get("job_id")) in seen_ids for x in (j.get("sources") or []))
        if seen_here:
            vmap[jid] = dict(s=st if st not in ("stale",) else j.get("status"), lv=ISO(NOW))
        elif srcs and srcs <= (ok_sources & ATS_IDS):
            vmap[jid] = dict(s="closed", lv=pv.get("lv") or j.get("last_verified"), ca=ISO(NOW))
            closed_now.append(jid)
        else:
            lv = parse(pv.get("lv") or j.get("last_verified")) or NOW
            vmap[jid] = dict(s="stale" if (NOW - lv).days > 3 else st, lv=pv.get("lv") or j.get("last_verified"))
    # liveness decisions from the pipeline: recent, still open, confirmed by the employer or 2+ sources
    lv_path = os.path.join(a.out, "liveness.json")
    live = json.load(open(lv_path)) if os.path.exists(lv_path) else {}
    for jid, L in (live.get("jobs") or {}).items():
        doc = new_jobs.get(jid) or ex_jobs.get(jid) or {}
        cur = dict(vmap.get(jid) or verify_prev.get(jid) or {"s": doc.get("status"), "lv": ISO(NOW)})
        for k in ("pc", "pr", "pd"):
            if L.get(k): cur[k] = L[k]
        s = L.get("s")
        if s == "closed" and cur.get("s") != "closed":
            cur.update(s="closed", r=L.get("r"), ca=ISO(NOW)); cur.pop("vb", None); cur.pop("vu", None)
            if jid in ex_jobs and jid not in closed_now: closed_now.append(jid)
        elif s == "stale" and cur.get("s") not in ("closed", "excluded"):
            cur.update(s="stale", r=L.get("r")); cur.pop("vb", None); cur.pop("vu", None)
        elif s == "live" and cur.get("s") not in ("closed", "excluded"):
            if L.get("vu"):
                cur["s"] = "verified"; cur["vu"] = L["vu"]
            elif cur.get("s") in ("stale", None):
                cur["s"] = doc.get("status") if doc.get("status") not in (None, "stale", "closed", "excluded") else "lead"
            cur.pop("r", None); cur["vb"] = L.get("vb")
        vmap[jid] = cur
    if live.get("boards") is not None:
        put("meta", "liveness", dict(boards=live["boards"], counts=live.get("counts"), updated_at=ISO(NOW)), "liveness" in ex_meta)
    put("meta", "verify", dict(map=vmap, updated_at=ISO(NOW)), "verify" in ex_meta)

    for cid, c in new_comp.items():
        c.pop("job_ids", None)
        if cid not in ex_comp:
            put("companies", cid, c, False)
        else:
            old = dict(ex_comp[cid]); old.pop("job_ids", None)
            if json.dumps(old, sort_keys=True, ensure_ascii=False) != json.dumps(c, sort_keys=True, ensure_ascii=False):
                put("companies", cid, c, True)
    for cid, c in new_cont.items():
        if cid not in ex_cont:
            put("contacts", cid, c, False)
        elif sorted(ex_cont[cid].get("job_ids", [])) != sorted(c.get("job_ids", [])):
            merged = dict(ex_cont[cid]); merged["job_ids"] = sorted(set(merged.get("job_ids", [])) | set(c.get("job_ids", [])))
            put("contacts", cid, merged, True)
    for bid, b in new_bm.items():
        if bid not in ex_bm:
            put("salary_benchmarks", bid, b, False)
        elif json.dumps(ex_bm[bid], sort_keys=True) != json.dumps(b, sort_keys=True):
            put("salary_benchmarks", bid, b, True)

    for k, v in new_links.items():
        if k not in ex_links: put("company_links", k, v, False)
        elif ex_links[k].get("linkedin") != v.get("linkedin"): put("company_links", k, v, True)
    ex_sizes = load_coll(a.existing, "company_sizes")
    for k, v in load_coll(a.out, "company_sizes").items():
        if k not in ex_sizes: put("company_sizes", k, v, False)
        elif (ex_sizes[k].get("range"), ex_sizes[k].get("employees")) != (v.get("range"), v.get("employees")): put("company_sizes", k, v, True)
    for k, v in new_checks.items():
        if k not in ex_checks: put("web_checks", k, v, False)

    # alerts (one per job+type; never repeated across sources or runs)
    alerts = []
    def alert(typ, j, extra=None):
        aid = f"{typ}-{j['id']}"
        if aid in ex_alerts or any(x["id"] == aid for x in alerts):
            return
        alerts.append(dict(id=aid, type=typ, job_id=j["id"], title=j["title"], company=j["company"], fit=fit(j.get("scores"), w),
                           source=j.get("source_primary"), comp_state=j.get("salary", {}).get("state"), apply_url=j.get("apply_url") or j.get("url"),
                           india=(j.get("india") or {}).get("status"), location=j.get("location"), work_mode=j.get("work_mode"), posted_at=j.get("posted_at"),
                           date_quality=j.get("date_quality"), discovered_at=j.get("first_seen"), track=j.get("track"), created_at=ISO(NOW), **(extra or {})))
    tracks = set(rules.get("tracks") or ["game", "software"])
    open_ids = [i for i in new_ids if vmap.get(i, {}).get("s") not in ("stale", "closed", "excluded") and new_jobs[i]["india"]["status"] in ("confirmed", "likely") and new_jobs[i].get("track", "software") in tracks]
    if first_run:
        for tr in ("game", "software"):
            top = sorted([i for i in open_ids if new_jobs[i].get("track") == tr], key=lambda i: -fit(new_jobs[i].get("scores"), w))
            for i in top[:5]:
                if fit(new_jobs[i].get("scores"), w) >= rules["minFit"]:
                    alert("initial", new_jobs[i])
    else:
        for i in open_ids:
            j = new_jobs[i]; f = fit(j.get("scores"), w)
            if rules.get("over90") and f >= 90: alert("over90", j)
            elif f >= rules["minFit"]: alert("new_high_fit", j)
            elif rules.get("allNew", True) and j.get("status") != "excluded": alert("new_match", j)
    for jid, ap_ in apps.items():
        j = new_jobs.get(jid) or ex_jobs.get(jid)
        if not j or ap_.get("stage") in ("closed",):
            continue
        if rules.get("closures") and jid in closed_now: alert("closed", j)
        prev = ex_jobs.get(jid)
        if rules.get("salaryDisclosed") and prev and prev.get("salary", {}).get("state") != "verified" and new_jobs.get(jid, {}).get("salary", {}).get("state") == "verified":
            alert("salary_disclosed", new_jobs[jid])
        if rules.get("recruiterFound") and new_jobs.get(jid, {}).get("has_named_contact") and prev and not prev.get("has_named_contact"):
            alert("recruiter_found", new_jobs[jid])
        dl = parse(j.get("deadline"))
        if rules.get("deadlines") and dl and 0 <= (dl - NOW).days <= 3:
            alert("deadline", j, dict(deadline=j.get("deadline")))
    for al in alerts:
        put("alerts", al["id"], {k: v for k, v in al.items() if k != "id"}, False)

    run.update(new_count=len(new_ids), changed_count=len(changed_ids), closed_count=len(closed_now), alerts_count=len(alerts))
    put("sync_logs", run["id"], run, False)
    system = dict(system_prev)
    system.update(pipeline_version=run.get("pipeline_version"), first_run_at=system_prev.get("first_run_at") or run["started_at"],
                  last_run=dict(id=run["id"], finished_at=run["finished_at"], kept=run["kept"], raw_count=run["raw_count"],
                                new_count=len(new_ids), closed_count=len(closed_now), alerts_count=len(alerts)),
                  india_checks=run.get("india_checks") or system_prev.get("india_checks"),
                  fx=dict(usd_inr=fx["rates"]["INR"], as_of=fx["as_of"], source=fx["source"]) if fx else system_prev.get("fx"),
                  updated_at=ISO(NOW))
    if isinstance(system.get("monitor"), dict) and system["monitor"].get("trigger_id"):
        system["monitor"] = {**system["monitor"], "cadence": "every 3 hours", "cron": "every 3 hours (UTC)"}
    put("meta", "system", system, "system" in ex_meta)

    # batches of <=50 create writes, <=900KB each
    batches, cur, size = [], [], 0
    for c in creates:
        s = os.path.getsize(c["file_path"])
        if cur and (len(cur) >= 50 or size + s > 900_000):
            batches.append(cur); cur, size = [], 0
        cur.append(c); size += s
    if cur: batches.append(cur)
    for i, b in enumerate(batches):
        json.dump(b, open(os.path.join(a.plan, f"creates_{i:03d}.json"), "w"), indent=0)
    json.dump(updates, open(os.path.join(a.plan, "updates.json"), "w"), indent=1)

    # summary / push message
    hour_ist = (NOW + dt.timedelta(hours=5, minutes=30)).hour
    weekday_ist = (NOW + dt.timedelta(hours=5, minutes=30)).weekday()
    digest = rules.get("digest", "run")
    send = bool(alerts) and (digest == "run" or (digest == "daily" and 5 <= hour_ist < 12) or (digest == "weekly" and weekday_ist == 0 and 5 <= hour_ist < 12))
    lines = []
    if send:
        top = sorted({x["job_id"]: x for x in sorted(alerts, key=lambda x: (x.get("fit") or 0))}.values(), key=lambda x: -(x.get("fit") or 0))[:5]
        label = dict(initial="Top match", new_high_fit="New high-fit role", over90="Fit above 90", new_match="New role", closed="Saved role closed",
                     salary_disclosed="Salary now disclosed", recruiter_found="Recruiter found", deadline="Deadline in 3 days")
        n_new = len({x["job_id"] for x in alerts if x["type"] in ("new_match", "new_high_fit", "over90")})
        n_hi = len({x["job_id"] for x in alerts if x["type"] in ("new_high_fit", "over90")})
        n_game = len({x["job_id"] for x in alerts if x["type"] in ("new_match", "new_high_fit", "over90") and x.get("track") == "game"})
        lines.append(f"QA Intelligence: {n_new} new role{'s' if n_new != 1 else ''} — {n_game} Game QA, {n_new - n_game} Software QA ({n_hi} high fit)" if n_new else f"QA Intelligence: {len(alerts)} alert{'s' if len(alerts) != 1 else ''} from this check")
        for x in top:
            where = (x.get("location") or "India").split(",")[0] if x.get("india") == "confirmed" else ("remote, open to India" if x.get("india") == "likely" else (x.get("location") or ""))
            sec = "Game QA" if x.get("track") == "game" else "Software QA"
            lines.append(f"• [{sec}] {x['title']} at {x['company']} · {where} · fit {x['fit']} {x.get('apply_url') or ''}".strip())
    open(os.path.join(a.plan, "message.md"), "w").write("\n".join(lines))
    summary = dict(run_id=run["id"], first_run=first_run, new=len(new_ids), changed=len(changed_ids), closed=len(closed_now), alerts=len(alerts),
                   create_batches=len(batches), creates=len(creates), updates=len(updates), push=send)
    json.dump(summary, open(os.path.join(a.plan, "summary.json"), "w"), indent=1)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
