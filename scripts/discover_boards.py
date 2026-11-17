#!/usr/bin/env python3
"""Find employers' own job boards without Claude or paid tools.

Usage: python3 scripts/discover_boards.py <qa|mi>

Each run looks up a few more company names (research/candidates_<app>.txt, plus employers already seen
in the tracker) on the public job-board APIs of Greenhouse, Ashby, Lever and SmartRecruiters. A name
counts only when a live board answers with at least one open job. Results are remembered in
research/boards_<app>.json, and feeds/<app>/boards_auto.json lists the boards the feed then reads every
run, so roles come straight from the employer and can be confirmed as live.
"""
import datetime as dt
import glob
import json
import os
import re
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UA = "Mozilla/5.0 (compatible; JobIntelligenceBot/1.0; +https://github.com/optimusbionic-ctrl/job-intelligence)"
PER_RUN = int(os.environ.get("DISCOVER_PER_RUN", "60"))
RECHECK_DAYS = 21          # names with no board are tried again after this long
NOW = dt.datetime.now(dt.timezone.utc)


def log(*a):
    print("[boards]", *a, flush=True)


def get(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:
        return getattr(e, "code", 0) or 0, b""


def norm(s):
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def slugs(name):
    base = re.sub(r"(?i)\b(inc|ltd|pvt|private|limited|llc|technologies|technology|labs|studios?|games|software|solutions|india)\b\.?", " ", name)
    a = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    b = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
    out = [a.replace("-", ""), a, b.replace("-", ""), b]
    return [s for s in dict.fromkeys(out) if len(s) >= 4][:4]   # very short names match unrelated boards too easily


def probe(name):
    """Return {type, slug, jobs} for the first board that answers with open jobs, else None."""
    for slug in slugs(name):
        s, b = get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if s == 200:
            try:
                bn = norm(json.loads(b).get("name"))
                nn = norm(name); short, long_ = sorted([bn, nn], key=len)
                if bn and short in long_ and len(short) >= 0.7 * len(long_):   # "Pine" must not pass for "Pine Labs"
                    s2, b2 = get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
                    n = len(json.loads(b2).get("jobs", [])) if s2 == 200 else 0
                    if n:
                        return dict(type="greenhouse", slug=slug, jobs=n)
            except Exception:
                pass
        s, b = get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        if s == 200:
            try:
                n = len(json.loads(b).get("jobs", []))
                if n:
                    return dict(type="ashby", slug=slug, jobs=n)
            except Exception:
                pass
        s, b = get(f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=50")
        if s == 200:
            try:
                d = json.loads(b)
                if isinstance(d, list) and d:
                    return dict(type="lever", slug=slug, jobs=len(d))
            except Exception:
                pass
        s, b = get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=20")
        if s == 200:
            try:
                d = json.loads(b)
                # old test postings linger on abandoned boards: count the board only if something was released in the last 4 months
                recent = [c for c in d.get("content", []) if (c.get("releasedDate") or "")[:10] >= (NOW - dt.timedelta(days=120)).date().isoformat()]
                if recent:
                    return dict(type="smartrecruiters", slug=slug, jobs=int(d.get("totalFound") or len(recent)))
            except Exception:
                pass
        time.sleep(0.15)
    return None


def main():
    app = sys.argv[1]
    state_p = os.path.join(ROOT, "research", f"boards_{app}.json")
    out_p = os.path.join(ROOT, "feeds", app, "boards_auto.json")
    state = json.load(open(state_p)) if os.path.exists(state_p) else {}
    reg = json.load(open(os.path.join(ROOT, "feeds", app, "companies.json")))
    known = {norm(c.get("name")) for c in reg} | {norm(a) for c in reg for a in (c.get("aliases") or [])}
    known_boards = {(c["ats"]["type"], c["ats"]["slug"]) for c in reg if isinstance(c.get("ats"), dict)}

    names = []
    cand_p = os.path.join(ROOT, "research", f"candidates_{app}.txt")
    if os.path.exists(cand_p):
        names += [l.strip() for l in open(cand_p, encoding="utf-8") if l.strip() and not l.startswith("#")]
    # employers already seen in the tracker (any source): their own board is the best place to confirm a role
    for f in glob.glob(os.path.join(ROOT, "db", app, "jobs", "*.json")):
        try:
            c = (json.load(open(f, encoding="utf-8")).get("company") or "").strip()
            if c and len(c) <= 40:
                names.append(c)
        except Exception:
            pass
    seen, todo = set(), []
    for n in names:
        k = norm(n)
        if not k or k in seen or k in known:
            continue
        seen.add(k)
        st = state.get(k)
        if st is None:
            todo.append((0, n))
        elif not st.get("board") and (NOW - dt.datetime.fromisoformat(st["checked_at"])).days >= RECHECK_DAYS:
            todo.append((1, n))
    todo.sort(key=lambda t: t[0])
    found = 0
    for _, n in todo[:PER_RUN]:
        b = None
        try:
            b = probe(n)
        except Exception as e:
            log("probe failed", n, repr(e)[:120])
        state[norm(n)] = dict(name=n, board=b, checked_at=NOW.isoformat(timespec="seconds"))
        if b:
            found += 1
            log("found", n, b)
    boards, used = [], set(known_boards)
    for k, st in sorted(state.items()):
        b = st.get("board")
        if b and (b["type"], b["slug"]) not in used:
            used.add((b["type"], b["slug"]))
            boards.append(dict(name=st["name"], ats=dict(type=b["type"], slug=b["slug"]), found_by="board lookup"))
    os.makedirs(os.path.dirname(state_p), exist_ok=True)
    json.dump(state, open(state_p, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
    json.dump(boards, open(out_p, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
    log(app, f"checked {min(len(todo), PER_RUN)} names ({max(0, len(todo) - PER_RUN)} waiting), {found} new boards, {len(boards)} extra boards in use")


if __name__ == "__main__":
    main()
