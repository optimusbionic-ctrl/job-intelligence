#!/usr/bin/env python3
"""Claude copy: say plainly when its data is older than the free feed, and point to the live copy. Idempotent."""
import sys

CHIP_OLD = "live.innerHTML = `<span class=\"pulse ${m.active && !m.stale ? '' : 'off'}\"></span><span>${m.active ? (m.lastAt ? `Updated ${ago(new Date(m.lastAt).toISOString())}` : 'Live') : 'Monitor off'}</span>`; live.title = m.next ? `Next check ${until(m.next)}` : '';"
CHIP_NEW = "if (m.onDemand) { const old = !m.lastAt || (NOW() - m.lastAt) > 90 * 60000; live.innerHTML = old ? `<a href=\"${APP_LINKS.free}\" target=\"_blank\" rel=\"noopener\" style=\"display:inline-flex;align-items:center;gap:7px;color:var(--warn)\"><span class=\"pulse off\"></span><span>Claude copy ${m.lastAt ? ago(new Date(m.lastAt).toISOString()) : 'not fetched yet'} · open live copy ↗</span></a>` : `<span class=\"pulse\"></span><span>Fetched by Claude ${ago(new Date(m.lastAt).toISOString())}</span>`; live.title = old ? 'This copy only updates when Claude fetches. The free copy updates every 30 minutes by itself.' : ''; } else { live.innerHTML = `<span class=\"pulse ${m.stale ? 'off' : ''}\"></span><span>${m.lastAt ? `Updated ${ago(new Date(m.lastAt).toISOString())}` : 'Live'}</span>`; live.title = m.next ? `Next check ${until(m.next)}` : ''; }"

STRIP_OLD = """  return `<section class="engine" aria-label="Fetch status"><span class="pulse off"></span>
    <div class="eg-t"><b>Claude fetches only when you ask</b><span>The free copy keeps checking every ${FREE_EVERY_MIN} minutes by itself, and a Claude fetch brings in what it found.</span>"""
STRIP_NEW = """  const old = !lastAt || (NOW() - lastAt) > 90 * 60000;
  if (old) return `<section class="engine late" aria-label="Fetch status"><span class="pulse off"></span>
    <div class="eg-t"><b>This Claude copy was last updated ${lastAt ? ago(new Date(lastAt).toISOString()) : '—'}</b><span>It only updates when you press “Fetch with Claude”. The free copy has the latest roles: it updates by itself every ${FREE_EVERY_MIN} minutes, without Claude.</span>
      <span class="eg-s">Your tracker, notes and saved views here are separate from the free copy.${S.monitor.err ? ` · <span style="color:var(--bad)">${esc(S.monitor.err)}</span>` : ''}</span></div>
    <div class="eg-a"><button class="btn sm" data-act="run-now" ${S.monitor.loading ? 'disabled' : ''}>${ico('i-spark', 12)} ${S.monitor.loading ? 'Starting…' : 'Fetch with Claude'}</button><a class="btn sm primary" href="${APP_LINKS.free}" target="_blank" rel="noopener">${ico('i-ext', 12)} Open live copy</a></div></section>`;
  return `<section class="engine" aria-label="Fetch status"><span class="pulse"></span>
    <div class="eg-t"><b>Up to date: fetched by Claude ${ago(new Date(lastAt).toISOString())}</b><span>Claude fetches only when you ask. The free copy keeps checking every ${FREE_EVERY_MIN} minutes by itself.</span>"""


def apply(s):
    if "Claude copy ${m.lastAt" in s:
        return s
    for a, b in ((CHIP_OLD, CHIP_NEW), (STRIP_OLD, STRIP_NEW)):
        assert s.count(a) == 1, a[:50]
        s = s.replace(a, b, 1)
    return s


if __name__ == "__main__":
    for p in sys.argv[1:]:
        s = open(p, encoding="utf-8").read(); t = apply(s); open(p, "w", encoding="utf-8").write(t); print(p, t != s)
