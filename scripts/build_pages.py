#!/usr/bin/env python3
"""Turn the Claude app pages (web/<app>/app.html) into standalone pages (site/<app>/index.html)."""
import html, os, re, shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APPS = {
    "qa": dict(title="QA Intelligence", who="Siddhant Singh · Game QA and Software QA",
               claude="https://claude.ai/artifact/2osxYDWPuyA7pcBV5nLVmt"),
    "mi": dict(title="Motion Intelligence", who="Prashant Singh · motion and product design",
               claude="https://claude.ai/artifact/QnWjsKvbvwNwva9Vhb64sw"),
}

# Wording that describes the Claude-hosted monitor, adjusted for this copy.
REWORD = [
    ("This page, live · phone push after each run", "This page · refreshed every hour, no phone alerts"),
    ("A scheduled task checks every live source every hour", "A GitHub Action checks every live source every hour"),
    ("and notifies you. Sources without an API are covered through search indexing and stay labelled as leads.",
     "and lists new roles here. Search-index leads found earlier by the Claude monitor are kept and labelled as leads."),
    ("Phone alerts arrive through the Claude app after a run that finds something.",
     "This free copy sends no phone alerts; new roles show up here and in the bell."),
    ("Email delivery is off; Slack or Telegram need a connector.",
     "In this free copy the hourly feed always uses the default rules."),
]

BADGE = """
<style>
.sa-badge{position:fixed;right:14px;bottom:14px;z-index:60;display:flex;align-items:center;gap:8px;max-width:calc(100vw - 28px);
  padding:7px 8px 7px 12px;border-radius:999px;background:var(--raised,#1C1D1A);color:var(--fg-2,#B9B4A8);
  border:1px solid var(--line-2,rgba(241,237,228,.16));box-shadow:0 6px 24px rgba(0,0,0,.25);font:500 12px/1.3 'Instrument Sans',system-ui,sans-serif}
.sa-badge a{color:var(--accent-text,#8A9BFF);text-decoration:none;white-space:nowrap}
.sa-badge a:hover{text-decoration:underline}
.sa-badge button{all:unset;cursor:pointer;width:22px;height:22px;display:grid;place-items:center;border-radius:50%;color:var(--mute,#8E8A80)}
.sa-badge button:hover{background:var(--hover,#20211E)}
.field:has(#ruleDigest),[data-act="refresh-trigger"]{display:none!important}
.sa-dot{width:7px;height:7px;border-radius:50%;background:#3FB27F;flex:none}
@media (max-width:900px){.sa-badge{bottom:calc(76px + env(safe-area-inset-bottom));right:10px}.sa-badge .sa-long{display:none}}
</style>
<div class="sa-badge" id="saBadge" role="note" hidden>
  <span class="sa-dot" aria-hidden="true"></span>
  <span><span class="sa-long">Free copy · </span>updated hourly<span id="saWhen"></span></span>
  <a href="__CLAUDE__" target="_blank" rel="noopener">AI tools in Claude ↗</a>
  <button type="button" aria-label="Hide" id="saHide">✕</button>
</div>
<script>
(function(){
  var b=document.getElementById('saBadge'); var k='standalone:badge-hidden';
  try{ if(sessionStorage.getItem(k)) return; }catch(e){}
  b.hidden=false;
  document.getElementById('saHide').onclick=function(){ b.hidden=true; try{sessionStorage.setItem(k,'1');}catch(e){} };
  function when(){ var i=window.standaloneInfo&&window.standaloneInfo(); if(!i||!i.builtAt) return;
    var m=Math.round((Date.now()-Date.parse(i.builtAt))/60000);
    document.getElementById('saWhen').textContent=' · built '+(m<1?'just now':m<60?m+' min ago':Math.round(m/60)+' h ago'); }
  setTimeout(when,1500); setInterval(when,60000);
  // "Run a check now" opens the workflow page on GitHub, where "Run workflow" starts a refresh.
  document.addEventListener('click',function(e){
    var t=e.target.closest&&e.target.closest('[data-act="run-now"]'); if(!t) return;
    e.preventDefault(); e.stopImmediatePropagation();
    var h=location.hostname.split('.'), seg=location.pathname.split('/').filter(Boolean);
    var url=h.slice(-2).join('.')==='github.io'&&seg.length>1?'https://github.com/'+h[0]+'/'+seg[0]+'/actions/workflows/feeds.yml':'https://github.com';
    window.open(url,'_blank','noopener');
  },true);
})();
</script>
"""


def build(app, meta):
    src = open(os.path.join(ROOT, "web", app, "app.html"), encoding="utf-8").read()
    head = (
        "<!doctype html>\n"
        f'<html lang="en" data-app="{app}">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
        '<meta name="robots" content="noindex">\n'
        '<script src="../standalone.js"></script>\n'
    )
    for old, new in REWORD:
        src = src.replace(old, new)
    # Public repo: keep personal email addresses out of the page (only used by Claude's outreach drafts).
    src = re.sub(r"email: '[^'@\s]+@[^'\s]+'", "email: ''", src)
    page = head + src + BADGE.replace("__CLAUDE__", meta["claude"]) + "\n</html>\n"
    out = os.path.join(ROOT, "site", app)
    os.makedirs(out, exist_ok=True)
    open(os.path.join(out, "index.html"), "w", encoding="utf-8").write(page)


def landing():
    cards = "".join(
        f'<a class="card" href="{a}/"><span class="t">{html.escape(m["title"])}</span>'
        f'<span class="s">{html.escape(m["who"])}</span><span class="go">Open →</span></a>'
        for a, m in APPS.items())
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Job Intelligence</title>
<style>
:root{{--bg:#F3EEE5;--card:#FBF8F2;--fg:#141412;--mute:#6A655C;--line:rgba(20,20,18,.12);--accent:#2238D6}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0D0E0C;--card:#151614;--fg:#F1EDE4;--mute:#8E8A80;--line:rgba(241,237,228,.12);--accent:#8A9BFF}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font:16px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}}
main{{max-width:760px;margin:0 auto;padding:56px 16px}}h1{{font-size:28px;margin:0 0 6px}}p{{color:var(--mute);margin:0 0 28px}}
.grid{{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}}
.card{{display:flex;flex-direction:column;gap:6px;padding:20px;border:1px solid var(--line);border-radius:14px;background:var(--card);color:inherit;text-decoration:none}}
.card:hover{{border-color:var(--accent)}}.t{{font-weight:650;font-size:18px}}.s{{color:var(--mute);font-size:14px}}.go{{color:var(--accent);font-size:14px;margin-top:8px}}
small{{display:block;margin-top:28px;color:var(--mute)}}
</style></head><body><main>
<h1>Job Intelligence</h1>
<p>Free copies of both job portals. A GitHub Action refreshes the roles every hour, with or without Claude.</p>
<div class="grid">{cards}</div>
<small>Roles come from official job-board and employer ATS APIs. Your tracker and notes stay in this browser.</small>
</main></body></html>
"""
    open(os.path.join(ROOT, "site", "index.html"), "w", encoding="utf-8").write(page)


if __name__ == "__main__":
    os.makedirs(os.path.join(ROOT, "site"), exist_ok=True)
    shutil.copy(os.path.join(ROOT, "web", "standalone.js"), os.path.join(ROOT, "site", "standalone.js"))
    for a, m in APPS.items():
        build(a, m)
    landing()
    open(os.path.join(ROOT, "site", ".nojekyll"), "w").close()
    print("pages built")
