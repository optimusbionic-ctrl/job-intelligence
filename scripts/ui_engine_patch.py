#!/usr/bin/env python3
"""Adds the engine status strip, the "Just in" list and on-demand Claude wording to an app page. Idempotent.

Usage: python3 scripts/ui_engine_patch.py <qa|mi> <page.html> [more pages]
"""
import sys

LINKS = {
    "mi": ("https://claude.ai/artifact/QnWjsKvbvwNwva9Vhb64sw", "https://optimusbionic-ctrl.github.io/job-intelligence/mi/"),
    "qa": ("https://claude.ai/artifact/2osxYDWPuyA7pcBV5nLVmt", "https://optimusbionic-ctrl.github.io/job-intelligence/qa/"),
}

JS = r'''
/* ---------- Engine status: the free feed runs by itself; Claude fetches only on request ---------- */
const APP_LINKS = { claude: '__CLAUDE__', free: '__FREE__' };
const IS_FREE = !!(window.claude && window.claude.standalone);
const FREE_EVERY_MIN = 30;
function engineStrip() {
  const last = S.logs[0]; const sys = S.meta.system || {};
  const lastAt = ts(last?.finished_at || sys.last_run?.finished_at);
  const ss = last?.source_status || {}; const total = Object.keys(ss).length; const okN = Object.values(ss).filter(x => x.ok).length;
  const fresh = [...S.jobs.values()].filter(j => isOpen(j) && (ts(j.first_seen) || 0) > NOW() - DAY).length;
  const src = total ? `${okN} of ${total} sources answered` : '';
  const found = `${fresh} new role${fresh === 1 ? '' : 's'} in the last 24 hours`;
  if (IS_FREE) {
    const late = lastAt ? (NOW() - lastAt) > 100 * 60000 : true;
    const next = lastAt ? new Date(lastAt + FREE_EVERY_MIN * 60000).toISOString() : null;
    return `<section class="engine ${late ? 'late' : ''}" aria-label="Auto-fetch status"><span class="pulse ${late ? 'off' : ''}"></span>
      <div class="eg-t"><b>${late ? 'Auto-fetch is running late' : 'Auto-fetch is on'}</b><span>Runs by itself every ${FREE_EVERY_MIN} minutes. Claude is not needed.</span>
        <span class="eg-s">${lastAt ? `Checked ${ago(new Date(lastAt).toISOString())}` : 'Waiting for the first check'}${next && !late ? ` · next ≈ ${until(next)}` : ''}${src ? ` · ${src}` : ''} · ${found}</span></div>
      <div class="eg-a"><button class="btn sm" data-act="reload-data">${ico('i-refresh', 12)} Refresh</button><a class="btn sm primary" href="${APP_LINKS.claude}" target="_blank" rel="noopener">${ico('i-spark', 12)} Fetch with Claude</a></div></section>`;
  }
  return `<section class="engine" aria-label="Fetch status"><span class="pulse off"></span>
    <div class="eg-t"><b>Claude fetches only when you ask</b><span>The free copy keeps checking every ${FREE_EVERY_MIN} minutes by itself, and a Claude fetch brings in what it found.</span>
      <span class="eg-s">${lastAt ? `Last Claude fetch ${ago(new Date(lastAt).toISOString())}` : 'No Claude fetch yet'} · ${found}${S.monitor.err ? ` · <span style="color:var(--bad)">${esc(S.monitor.err)}</span>` : ''}</span></div>
    <div class="eg-a"><a class="btn sm" href="${APP_LINKS.free}" target="_blank" rel="noopener">${ico('i-ext', 12)} Open the free copy</a><button class="btn sm primary" data-act="run-now" ${S.monitor.loading ? 'disabled' : ''}>${ico('i-spark', 12)} ${S.monitor.loading ? 'Starting…' : 'Fetch with Claude'}</button></div></section>`;
}
function justIn(open, fit) {
  const rows = open.filter(j => (ts(j.first_seen) || 0) > NOW() - 3 * DAY).sort((a, b) => (ts(b.first_seen) || 0) - (ts(a.first_seen) || 0) || fit.get(b.id) - fit.get(a.id));
  const top = rows.slice(0, 6);
  return `<section class="panel justin"><div class="panel-h"><div><span class="eyebrow">Found in the last 3 days · newest first</span><h2>Just in${rows.length ? ` <span class="count">${rows.length}</span>` : ''}</h2></div><a class="btn sm ghost" href="#explorer">All roles ${ico('i-chev', 12)}</a></div>
    <div class="panel-b"><div class="list-rows">${top.length ? top.map(j => listRow(j, fit.get(j.id))).join('') : `<p class="note" style="padding:6px 0">${ico('i-info')}<span>Nothing new in the last 3 days. ${IS_FREE ? `The feed checks every ${FREE_EVERY_MIN} minutes; new roles that fit your lens appear here first.` : 'Press “Fetch with Claude” to check now, or open the free copy, which checks by itself.'}</span></p>`}</div></div></section>`;
}
'''

CSS = '''.engine{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:12px 16px;border:1px solid var(--line);border-radius:var(--r-lg);background:var(--surface)}
.engine.late{border-color:var(--warn)}
.engine .eg-t{display:flex;flex-direction:column;gap:2px;flex:1 1 320px;min-width:0;font-size:13px;color:var(--fg-2)}
.engine .eg-t b{color:var(--fg);font-weight:600;font-size:14px}
.engine .eg-s{font-size:12px;color:var(--mute)}
.engine .eg-a{display:flex;gap:8px;flex-wrap:wrap}
.justin h2 .count{font:500 12px/1 var(--f-mono);color:var(--mute);margin-left:6px;vertical-align:middle}
@media (max-width:640px){.engine .eg-a{width:100%}.engine .eg-a .btn{flex:1;justify-content:center}}
'''


# Wording that used to describe an hourly Claude monitor.
WORDING = [
    ("A scheduled task checks every live source every hour, deduplicates new roles,", "Every check reads each live source, deduplicates new roles,"),
    ("and notifies you. Sources without an API are covered through search indexing and stay labelled as leads.",
     "and lists what is new. The free copy does this by itself every 30 minutes; Claude does it when you press “Fetch with Claude” and adds search-index leads, which stay labelled as leads."),
    ("Checks run every hour, not continuously. A run takes a few minutes;", "The free copy checks every 30 minutes by itself; Claude checks only when you ask. A run takes a few minutes;"),
    ("The monitor checks sources every hour. New roles appear here as they are found.", "New roles appear here as they are found."),
    ("A scheduled task checks every live source every hour for both sections, deduplicates new roles,", "Every check reads each live source for both sections, deduplicates new roles,"),
    ("jobs every hour, removes duplicates", "jobs automatically, removes duplicates"),
    ("A scheduled task in Claude checks every live source every 3 hours (the free copy on GitHub checks every hour in between), deduplicates new roles,", "Every check reads each live source, deduplicates new roles,"),
    ("A scheduled task in Claude checks every live source every 3 hours (the free copy on GitHub checks every hour in between) for both sections, deduplicates new roles,", "Every check reads each live source for both sections, deduplicates new roles,"),
    ("'Free hourly feed'", "'Free copy: checks every 30 minutes'"),
    ("A copy of this tracker that keeps updating every hour without Claude, for when your Claude usage runs out.", "A copy of this tracker that keeps updating by itself every 30 minutes, without Claude."),
    ("board checked every hour</li>", "board checked on every run</li>"),
    ("roles every hour, removes duplicates", "roles automatically, removes duplicates"),
]


MONITOR_STATE = r"""function monitorState() {
  const sys = S.meta.system || {}; const last = S.logs[0];
  const lastAt = ts(last?.finished_at || sys.last_run?.finished_at);
  const trig = S.monitor.trig; const onDemand = !IS_FREE;
  const cadence = IS_FREE ? `every ${FREE_EVERY_MIN} minutes` : 'when you ask';
  const stale = onDemand ? false : (lastAt ? (NOW() - lastAt) > 100 * 60000 : true);
  return { sys, last, lastAt, stale, cadence, onDemand, active: IS_FREE ? true : !!sys.monitor?.trigger_id, next: onDemand || !lastAt ? null : new Date(lastAt + FREE_EVERY_MIN * 60000).toISOString(), nextExact: false };
}
"""


def apply(s, app):
    if "function engineStrip()" in s:
        return s
    claude, free = LINKS[app]
    i = s.index("function viewOverview() {")
    s = s[:i] + JS.lstrip("\n").replace("__CLAUDE__", claude).replace("__FREE__", free) + s[i:]
    a = "  ${trendsBanner()}\n"
    assert s.count(a) == 1, "trendsBanner anchor"
    s = s.replace(a, "  ${engineStrip()}\n" + a, 1)
    i = s.index("function viewOverview() {")
    b = '  <div class="two">\n'
    j = s.index(b, i)
    s = s[:j] + "  ${justIn(open, fit)}\n" + s[j:]
    # monitor state: the free copy runs every 30 minutes; in Claude the monitor is on demand
    c0 = s.index("function monitorState() {"); c1 = s.index("\n}\n", c0) + 3
    s = s[:c0] + MONITOR_STATE + s[c1:]
    f = "function monitorBadge() { const m = monitorState(); return m.active ?"
    assert s.count(f) == 1, "badge anchor"
    s = s.replace(f, "function monitorBadge() { const m = monitorState(); if (m.onDemand) return `<span class=\"pill plain\">Claude · on demand</span>`; return m.active ?", 1)
    g = "<b>${m.active ? 'Monitoring' : 'Monitor off'}</b></div><span class=\"muted\">Last ${m.lastAt ? ago(new Date(m.lastAt).toISOString()) : '—'} · next ${m.next ? until(m.next) : '—'}</span>`;"
    assert s.count(g) == 1, "mini anchor"
    s = s.replace(g, "<b>${m.onDemand ? 'Claude on demand' : 'Auto-fetch on'}</b></div><span class=\"muted\">Last ${m.lastAt ? ago(new Date(m.lastAt).toISOString()) : '—'}${m.next ? ` · next ${until(m.next)}` : ''}</span>`;", 1)
    s = s.replace("${S.monitor.loading ? 'Starting…' : 'Run a check now'}", "${S.monitor.loading ? 'Starting…' : 'Fetch with Claude'}")
    s = s.replace("toast(`${ico('i-check')} Check started. New roles appear here when it finishes, usually within 10 minutes.`, 6000);", "toast(`${ico('i-check')} Claude is fetching. New roles appear here when it finishes, usually within 10 minutes.`, 6000);", 1)
    for app_name in ("Motion Intelligence", "QA Intelligence"):
        s = s.replace(f"text: 'Manual run requested from the {app_name} app.'", f"text: 'Manual run requested from the {app_name} app with the Fetch with Claude button. Run every step now, including the step 4 searches, whatever the current UTC hour is.'")
    h = "    case 'run-now': runNow(); break;\n"
    assert s.count(h) == 1, "action anchor"
    s = s.replace(h, h + "    case 'reload-data': if (window.standaloneReload) { el.disabled = true; window.standaloneReload().then(() => toast(`${ico('i-check')} Up to date.`, 2500), () => toast('Could not refresh. Check your connection.', 4000)).finally(() => { el.disabled = false; }); } break;\n", 1)
    for old, new in WORDING:
        s = s.replace(old, new)
    k = s.index(".kpis{")
    s = s[:k] + CSS + s[k:]
    return s


if __name__ == "__main__":
    app = sys.argv[1]
    for p in sys.argv[2:]:
        s = open(p, encoding="utf-8").read()
        t = apply(s, app)
        open(p, "w", encoding="utf-8").write(t)
        print(p, "patched" if t != s else "already patched")
