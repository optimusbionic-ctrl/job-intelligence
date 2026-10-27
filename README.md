# Job Intelligence: free hourly feeds

Free, Claude-independent copies of **QA Intelligence** (Siddhant) and **Motion Intelligence** (Prashant).

A GitHub Action runs every hour, even when no Claude subscription or credits are active. It:

1. pulls fresh roles from the same official job-board and employer ATS APIs the Claude monitors use (Shine, Workable, Cutshort, Himalayas, Remotive, RemoteOK, Working Nomads, Jobicy, HN Who's Hiring, Greenhouse, Ashby, Lever, SmartRecruiters);
2. applies the same filters, fit scores, India-hiring checks, deduplication and alert rules;
3. saves the results in `db/` and publishes both apps on GitHub Pages.

Everything runs on GitHub's free plan. Public repositories get unlimited Actions minutes and free Pages hosting.

## One-time setup (about 5 minutes)

1. Create a new **public** repository on GitHub, for example `job-intelligence`. Leave it empty.
2. Upload everything in this folder, including the hidden `.github` folder. You can either:
   - push with git: `git remote add origin https://github.com/<you>/job-intelligence.git && git push -u origin main`, or
   - drag the files into the repo page on github.com ("uploading an existing file"). If `.github/workflows/feeds.yml` doesn't upload, create it with **Add file → Create new file** and paste its contents.
3. Go to **Settings → Pages**, and under **Build and deployment → Source**, choose **GitHub Actions**.
4. Go to **Actions**. If GitHub asks, click **I understand my workflows, go ahead and enable them**. Then open **Hourly job feeds** and click **Run workflow** once.
5. After about 5 minutes the sites are live at:
   - `https://<you>.github.io/job-intelligence/` (landing page)
   - `https://<you>.github.io/job-intelligence/qa/` (QA Intelligence)
   - `https://<you>.github.io/job-intelligence/mi/` (Motion Intelligence)

From then on it refreshes at about :17 past every hour. GitHub can start scheduled runs a few minutes late when it's busy. Each run commits the updated data, which also stops GitHub from pausing the schedule for inactivity.

## What's the same, and what needs Claude

| | Free copy | Claude version |
|---|---|---|
| Hourly role feed from all live APIs | ✅ | ✅ |
| Fit scores, filters, Game QA / Software QA sections, India-hiring checks | ✅ | ✅ |
| Companies, pay context, alerts (bell) | ✅ | ✅ |
| Tracker, notes, saved searches, exports | ✅ (kept in your browser) | ✅ (synced to your account) |
| New LinkedIn / Naukri search-index leads, LinkedIn page and salary lookups | earlier ones kept | ✅ |
| AI analysis, outreach drafts, resume reading, people finders | ❌ | ✅ |
| Phone push alerts | ❌ (chosen: no alerts) | ✅ |

The two versions keep separate databases. The Claude apps and their scheduled monitors are unchanged.

## Privacy and rules

- Only public job postings, company names and public careers contacts are stored in `db/`. No resumes, tracker entries, notes, phone numbers or personal email addresses are in this repo.
- Your tracker and notes in the free copy are saved in the browser you use (localStorage). They don't sync between devices.
- Sources are read only through their official public APIs, with rate limits and robots.txt respected. LinkedIn, Naukri, Indeed and foundit pages are never fetched.
- Salaries are shown only when the posting lists them. Everything else is labelled as an estimate.

## Files

```
.github/workflows/feeds.yml   hourly schedule + Pages deploy
feeds/qa, feeds/mi            pipeline.py (fetch + score), sync.py (diff + alerts), companies.json, benchmarks.json
scripts/run_feed.py           runs one feed and applies the changes to db/
scripts/build_pages.py        builds site/ from web/ (adds the standalone runtime)
web/standalone.js             serves the app's data from data.json instead of Claude
db/                           the database (one JSON file per record)
```

Run locally (Python 3.10+, no packages needed): `python3 scripts/run_feed.py qa && python3 scripts/run_feed.py mi && python3 scripts/build_pages.py`, then `cd site && python3 -m http.server`.
