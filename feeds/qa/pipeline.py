#!/usr/bin/env python3
"""
QA Intelligence — ingestion pipeline
====================================
Pulls QA / software-testing / game-testing roles for Siddhant Singh (Mumbai, 2+ years,
manual & game QA) from compliant public sources (official job-board APIs and
employer ATS posting APIs, plus search-index leads supplied by the monitor),
normalises them, applies hard exclusions, splits them into two sections
(Game QA and Software QA), checks that overseas roles are open to candidates in
India, deduplicates across sources, attaches posting-listed salary (never
invented), computes fit sub-scores against the matching resume, and writes
database documents for the QA Intelligence app.

Usage:
  python3 qa_pipeline.py --out OUT_DIR [--existing EXISTING_DIR]
                         [--companies companies.json] [--watchlist DIR]
                         [--leads leads.json] [--benchmarks benchmarks.json]
"""
import argparse, concurrent.futures as cf, datetime as dt, glob, hashlib, html, json, os, re, ssl, sys, time, urllib.parse, urllib.request
from email.utils import parsedate_to_datetime

PIPELINE_VERSION = "1.0.0"
UA = "QAIntelligence/1.0 (+personal job research; respects robots.txt)"
WINDOW_DAYS = 120          # fetch window; the app defaults to a 90-day view
NOW = dt.datetime.now(dt.timezone.utc)
CTX = ssl.create_default_context()

# ---------------------------------------------------------------- utilities
def log(*a):
    print("[mi]", *a, file=sys.stderr, flush=True)

def http_get(url, timeout=30, tries=2, accept="application/json"):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept})
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (404, 410, 403, 401):
                return e.code, b""
            time.sleep(1.5 * (i + 1))
        except Exception as e:
            last = e
            time.sleep(1.5 * (i + 1))
    return 0, str(last).encode()

def get_json(url, **kw):
    s, b = http_get(url, **kw)
    if s != 200:
        raise RuntimeError(f"HTTP {s} for {url}: {b[:120]!r}")
    return json.loads(b)

def strip_html(h):
    if not h:
        return ""
    h = html.unescape(h) if "&lt;" in h else h
    h = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", h)
    h = re.sub(r"(?i)<\s*(br|/p|/li|/h\d|/div|/ul|/ol)\s*/?>", "\n", h)
    h = re.sub(r"(?i)<\s*li[^>]*>", "\n• ", h)
    h = re.sub(r"<[^>]+>", " ", h)
    h = html.unescape(h)
    h = re.sub(r"[ \t ]+", " ", h)
    h = re.sub(r"\n\s*\n+", "\n\n", h)
    return h.strip()

def iso(d):
    return d.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if d else None

def parse_date(v):
    if v in (None, "", 0, "0"):
        return None
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            x = float(v)
            if x > 1e12:
                x /= 1000
            return dt.datetime.fromtimestamp(x, dt.timezone.utc)
        s = str(v).strip()
        if re.match(r"^\w{3}, \d", s):
            return parsedate_to_datetime(s)
        s = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except Exception:
        return None

def norm(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

COMPANY_SUFFIX = re.compile(r"\b(inc|llc|ltd|limited|gmbh|pvt|private|corp|corporation|co|technologies|technology|labs|hq|group|plc|sa|bv|ag)\b")
def company_key(name):
    n = norm(name)
    n = COMPANY_SUFFIX.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n.replace(" ", "")

def slugify(s):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", (s or "").lower())).strip("-")[:60] or "unknown"

def hid(*parts):
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:14]

# ---------------------------------------------------------------- taxonomy (QA)
QA_SYNONYMS = ["QA Tester", "Software Tester", "Quality Assurance Tester", "Software QA Tester", "QA Engineer", "Software QA Engineer",
               "Quality Assurance Engineer", "Manual Tester", "Manual QA Tester", "Manual QA Engineer", "Functional Tester", "Application Tester",
               "Software Test Engineer", "Test Engineer", "QA Analyst", "Quality Assurance Analyst", "Software Test Analyst", "Application Testing Engineer"]
GAME_SYNONYMS = ["Game Tester", "Game QA Tester", "QA Tester (Games)", "Game QA Analyst", "Game Test Engineer", "Gameplay Tester", "Playtester",
                 "Slot Game QA", "Casino Game Tester", "iGaming QA", "Mobile Game Tester"]
TITLE_QA = re.compile(r"(?i)(\bqa\b|\bq\.a\.|quality assurance|quality analyst|\btesters?\b|test engineer|test analyst|testing engineer|software testing|application testing|\bplay ?tester|game test|\bsdet\b|software development engineer in test|test automation|automation test|quality engineer|\blqa\b|testing (analyst|specialist|executive|associate))")
NOT_SOFTWARE = re.compile(r"(?i)\b(civil|construction|mechanical|electrical|hvac|pharma\w*|clinical|medical device|food|beverage|textile|garment|apparel|automotive|manufactur\w*|production line|plant|chemical|laboratory|lab technician|microbiolog\w*|welding|ndt|piping|structural|metallurg\w*|qa/qc|qa ?& ?qc|qc inspector|quality inspector|inspection|supplier quality|vendor quality|sqe|haccp|gmp|calibration|tool ?room|casting|forging|solar|steel|cement|packaging|dairy|meat|seafood|cosmetic|biotech|regulatory affairs|validation specialist)\b")
PROCESS_QA = re.compile(r"(?i)(voice process|non[- ]voice|call (quality|audit|cent(re|er))|\bbpo\b|\bkpo\b|customer (support|service|experience) quality|chat process|process quality|transaction (monitoring|quality)|claims|underwriting|medical coding|content moderat\w*|data annotation|annotation|data labell?ing|search quality|ads quality|\brater\b|quality auditor|collections|kyc|back office|mis executive|audit associate)")
HARDWARE = re.compile(r"(?i)\b(hardware|pcb|\brf\b|wireless|firmware|silicon|asic|fpga|\bsoc\b|semiconductor|battery|\bev\b|vehicle|drone|avionics|telecom|\b5g\b|\blte\b|embedded|iot device|device validation|electronics|mechanical qa|network test)\b")
SECURITY = re.compile(r"(?i)penetration|pen ?test|vapt|security test|ethical hack")
SENIOR = re.compile(r"(?i)\b(senior|sr\.?|lead|principal|staff|head|manager|director|architect|vp|chief|iii|iv)\b")
LANG = re.compile(r"(?i)\b(german|french|spanish|italian|portuguese|brazilian|japanese|korean|chinese|mandarin|cantonese|arabic|russian|ukrainian|polish|turkish|dutch|swedish|norwegian|danish|finnish|thai|vietnamese|indonesian|malay|hebrew|greek|czech|hungarian|romanian|bulgarian|serbian|croatian|slovak|filipino|tagalog|bengali|tamil|telugu|kannada|malayalam|marathi|gujarati|punjabi)\b")
EXCL_LEVEL = re.compile(r"(?i)\b(intern|internship|trainee|apprentice(ship)?|graduate program|student|summer analyst)\b")
EXCL_UNPAID = re.compile(r"(?i)\b(unpaid|volunteer|equity[- ]only|no salary)\b")
REGION_IN_TITLE = re.compile(r"(?i)(\((eu|us|usa|uk|latam|emea|canada|americas|europe|apac only|us only)\)|[-–|] ?(us|usa|uk|eu|latam|canada|europe|germany|spain|brazil|poland|romania|philippines|mexico)( only)?\s*$|\b(us|uk|eu)[- ]based\b|\bremote in (emea|europe|the us|us|austria|germany|the uk|uk|canada|latam)\b|\(remote in [a-z ]+\)|on-site in (vienna|london|berlin|warsaw|bucharest|manila))")
GAME_T = re.compile(r"(?i)\b(game|games|gaming|gameplay|play ?test\w*|slots?|casino|igaming|i-gaming|\brmg\b|real money|fantasy|esports|console|unity|unreal|xbox|playstation|ps5|nintendo|steam|vr game|metaverse)\b")
GAME_D = r"gameplay|game builds?|game mechanics|game design|level design|play ?test|\bslots?\b|casino|betting|\bbets?\b|payouts?|\brtp\b|\breels?\b|jackpot|\bunity\b|\bunreal\b|console games?|\bsteam\b|xbox|playstation|nintendo|\btrc\b|lotcheck|first[- ]party|game studio|mobile games?|game development|live ?ops|in-game|players?\b|game qa|game testing|igaming|real[- ]money|rummy|poker|ludo|fantasy sports"
CASINO = r"\bslots?\b|casino|igaming|i-gaming|\bbetting\b|\bbets?\b|\bpayouts?\b|\brtp\b (%|percent|values?|calculation)|return to player|jackpot|\breels?\b|\bwager|sportsbook|real[- ]money|rummy|poker|teen patti"
AUTOMATION = r"selenium|appium|cypress|playwright|webdriver|webdriverio|robot framework|testng|junit|pytest|rest ?assured|katalon|cucumber|bdd|automation framework|test automation|automated tests?|automation scripts?"
NON_SW = re.compile(r"(?i)transformers?\b|syringe|fire protection|plumbing|hydraulic|\bev\b|batter(y|ies)|end of line|\beol\b|mechanical|civil|construction|pharma|medical device|quality management system|\bqms\b|iso ?9001|regulatory compliance|winding|production line|manufactur\w*|welding|\bndt\b|calibration|testing instruments|deskripsi|pekerjaan|security guard|protection service|automotive|textile|garment|food safety|laboratory")
SW_EVIDENCE = re.compile(r"(?i)software|\bapplications?\b|\bapps?\b|web(site)?s?\b|mobile|\bbugs?\b|defects?|jira|test cases?|regression|selenium|appium|\bapi\b|\bsql\b|agile|scrum|manual testing|functional testing|\bsdlc\b|\bstlc\b|\bui\b")
LOWQ = re.compile(r"(?i)\bno (prior )?experience (needed|required)\b|work from home,? no e|\bvirtual (mobile )?game tester|earn (money )?(from|at) home|review writing|\bpart[- ]time.*\bwork from home\b")
LOWQ_CO = re.compile(r"(?i)tester|testing|remote ?pro|work ?from ?home|\bjobs?\b$")
CROWD = re.compile(r"(?i)testlio|utest|applause|test ?io\b|global app testing|rainforest|crowd ?test|testbirds|bugcrowd|userlytics|usertesting")

QA_SIGNALS = {
    "Functional testing": r"functional (testing|tests?)|functionality testing",
    "Regression testing": r"regression",
    "Smoke testing": r"smoke (tests?|testing)",
    "Sanity testing": r"sanity",
    "Compatibility testing": r"compatibility|cross[- ](browser|device|platform)|multiple devices",
    "Stress / load testing": r"stress test|load test|performance test|jmeter|gatling|locust|soak test",
    "UI testing": r"\bui\b (testing|tests?|issues|bugs)|user interface|\bgui\b|visual (testing|bugs)",
    "Exploratory testing": r"exploratory",
    "Gameplay testing": r"gameplay|game mechanics|play ?test|game ?play",
    "Usability testing": r"usability",
    "Test cases & scenarios": r"test cases?|test plans?|test scenarios?|test scripts?|test documentation",
    "Bug reporting": r"bug reports?|defects?|bug tracking|reproduc\w+|steps to reproduce|bug life ?cycle",
    "Jira": r"\bjira\b",
    "Bugzilla": r"bugzilla",
    "Google Sheets / Excel": r"google sheets|spreadsheets?|excel",
    "Android": r"android",
    "iOS": r"\bios\b|iphone|ipad",
    "Windows / PC": r"\bwindows\b|\bpc\b|desktop",
    "Mobile apps": r"mobile app|mobile application|app testing|play store|app store|mobile (testing|devices)",
    "Web apps": r"web application|web app|browsers?|websites?",
    "Slot / casino games": r"slot (games?|machines?|mechanics|titles?)|\bslots? (qa|testing|tester)|casino|igaming|i-gaming|\bbetting\b|sportsbook|real[- ]money|rummy|poker|teen patti|jackpot|\breels?\b|return to player",
    "Steam / PC games": r"\bsteam\b|pc games?",
    "Console": r"(game|gaming) consoles?|consoles? (games?|platforms?|titles?|certification)|playstation|\bps[45]\b|xbox|nintendo",
    "Unity / Unreal": r"\bunity\b|\bunreal\b",
    "Test management (TestRail/Zephyr)": r"testrail|zephyr|qtest|\bxray\b|test management tool",
    "API testing": r"\bapi (testing|tests)|postman|rest ?assured|swagger|soapui|api validation",
    "SQL / databases": r"\bsql\b|database testing|mysql|postgres|mongodb",
    "Test automation": AUTOMATION,
    "Programming": r"\bjava\b|python|javascript|typescript|c#|c\+\+|\bjs\b",
    "Agile / Scrum": r"agile|scrum|sprints?|kanban",
    "Device logs (ADB/Charles)": r"\badb\b|logcat|device logs|charles|fiddler|proxy",
    "Localization": r"locali[sz]ation|\blqa\b|linguistic|translation",
    "Certification (TRC/XR)": r"\btrc\b|\bxr\b|lotcheck|certification requirements|first[- ]party certification",
    "ISTQB": r"istqb",
}
PLATFORMS = ["Android", "iOS", "Windows / PC", "Mobile apps", "Web apps", "Slot / casino games", "Steam / PC games", "Console", "Unity / Unreal"]
CANDIDATE_TOOLS = {"Functional testing", "Regression testing", "Smoke testing", "Sanity testing", "Compatibility testing", "Stress / load testing",
                   "UI testing", "Exploratory testing", "Gameplay testing", "Usability testing", "Test cases & scenarios", "Bug reporting", "Jira",
                   "Bugzilla", "Google Sheets / Excel", "Android", "iOS", "Windows / PC", "Mobile apps", "Slot / casino games", "Steam / PC games"}
GAP_TOOLS = {"Test automation", "API testing", "SQL / databases", "Programming", "Test management (TestRail/Zephyr)", "Console",
             "Certification (TRC/XR)", "Localization", "ISTQB", "Device logs (ADB/Charles)"}
CANDIDATE_SPECS = set(PLATFORMS) - {"Console", "Unity / Unreal", "Web apps"}
# kept for compatibility with shared helpers
MOTION_SIGNALS = QA_SIGNALS
SPECIALIZATIONS = {k: QA_SIGNALS[k] for k in PLATFORMS}
TITLE_PRIMARY = TITLE_QA
TITLE_SECONDARY = re.compile(r"(?i)$^")

INDIA = re.compile(r"(?i)\b(india|bengaluru|bangalore|mumbai|bombay|navi mumbai|thane|vashi|airoli|kalyan|mira road|andheri|goregaon|powai|malad|borivali|lower parel|bkc|pune|delhi|new delhi|ncr|gurgaon|gurugram|noida|hyderabad|chennai|kolkata|ahmedabad|kochi|jaipur|chandigarh|indore|nagpur|nashik|vadodara|surat|lucknow|bhubaneswar|coimbatore|trivandrum|thiruvananthapuram|mohali|ranchi|udupi|mangalore|mysore|mysuru)\b")
INDIA_CITY = {"Mumbai": r"mumbai|bombay|navi mumbai|thane|vashi|airoli|kalyan|mira road|andheri|goregaon|powai|malad|borivali|lower parel|bkc", "Pune": r"\bpune\b",
              "Bengaluru": r"bengaluru|bangalore", "Delhi NCR": r"delhi|ncr|gurgaon|gurugram|noida", "Hyderabad": r"hyderabad", "Chennai": r"chennai"}
MUMBAI = re.compile(r"(?i)" + INDIA_CITY["Mumbai"])
WORLDWIDE = re.compile(r"(?i)(worldwide|anywhere|global(ly)?\b|all countries|work from anywhere|any location|remote[- ]first.*global|🌍|🌏)")
ASIA = re.compile(r"(?i)\b(apac|asia|asia[- ]pacific|south asia|ist\b|gmt\+5|utc\+5|utc ?\+ ?5:30)\b")
RESTRICTED = re.compile(r"(?i)\b(us only|usa only|u\.s\. only|united states|usa\b|\bus\b|canada|north america|americas|latam|latin america|uk only|united kingdom|\buk\b|england|scotland|europe|\beu\b|emea|germany|france|spain|netherlands|poland|portugal|ireland|italy|sweden|norway|denmark|finland|switzerland|austria|belgium|czech|czechia|hungary|romania|bulgaria|serbia|croatia|greece|estonia|latvia|lithuania|ukraine|turkey|türkiye|israel|egypt|uae|united arab emirates|saudi|qatar|nigeria|kenya|ghana|morocco|pakistan|bangladesh|sri lanka|nepal|vietnam|indonesia|malaysia|thailand|korea|china|hong kong|taiwan|australia|new zealand|brazil|mexico|argentina|colombia|chile|peru|uruguay|costa rica|philippines|japan|singapore|south africa|new york|san francisco|los angeles|seattle|austin|chicago|boston|toronto|vancouver|montreal|london|berlin|paris|amsterdam|barcelona|madrid|lisbon|warsaw|prague|vilnius|kyiv|kiev|istanbul|cairo|dubai|tel aviv|sydney|melbourne|tokyo|seoul|cet|est\b|pst\b|pt\b|et\b)\b")
DESC_REGION = re.compile(r"(?i)(\b(?:emea|latam|americas|north america|european|europe|eu|us|usa|uk)[- ](?:based|only|time ?zones?|hours)\b|\bcet ?(?:±|\+/?-|\+-)|\b(?:est|pst|cet|gmt|et|pt)\b[^.\n]{0,25}\b(?:overlap|time ?zone|working hours)|(?:must|should|need to) (?:be )?(?:located|based|reside|living) (?:in|within) (?:the )?(?:us|usa|united states|uk|eu|europe|canada|americas|emea)|remote \((?:emea|europe|eu|us|usa|uk|americas|latam)[^)]*\))")
VISA = re.compile(r"(?i)(visa sponsorship|sponsor (your |a )?visa|relocation (support|assistance|package|bonus)|we (will|can) sponsor|help(s)? (you )?relocate|relocate to)")
NO_VISA = re.compile(r"(?i)(unable to sponsor|not (able to )?sponsor|no (visa )?sponsorship|must (be )?(legally )?authori[sz]ed to work|without (the need for )?sponsorship)")
CONTRACT = re.compile(r"(?i)\b(contract|freelance|fixed[- ]term|ftc|temporary|temp\b|part[- ]time|\d+[- ]month)\b")

def find_tags(text, table):
    t = text.lower()
    return [k for k, rx in table.items() if re.search(rx, t)]

def seniority_of(title, desc):
    t = title.lower()
    if SENIOR.search(t) and not re.search(r"(?i)(tester|engineer|analyst)\s*/\s*(senior|sr)", t): s = "senior"
    elif re.search(r"\b(junior|jr\.?|associate|fresher|entry[- ]level|graduate|i)\b", t): s = "junior"
    elif re.search(r"\b(ii|2)\b", t): s = "mid"
    else: s = "unspecified"
    yrs = None
    m = re.search(r"(\d{1,2})\s*\+?\s*(?:(?:-|–|to)\s*(\d{1,2})\s*\+?\s*)?(?:years?|yrs?)\s+(?:of\s+)?(?:[a-z/&-]+\s+){0,4}?(?:experience|exp\b)", desc.lower())
    if not m:
        m = re.search(r"(?:experience|exp)\s*[:\-–]?\s*(\d{1,2})\s*(?:-|–|to)?\s*(\d{1,2})?\s*(?:years?|yrs?)", desc.lower())
    if m and 0 <= int(m.group(1)) < 25:
        yrs = int(m.group(1))
    return s, yrs

def classify_title(title, desc, company=""):
    """Return (include: bool|None, kind, track, reason)."""
    t = title; d = (desc or "").lower(); ctx = (title + " " + (company or "")).lower()
    if EXCL_UNPAID.search(t) or EXCL_UNPAID.search(d[:800]):
        return False, None, None, "Unpaid / volunteer"
    if not TITLE_QA.search(t):
        return False, None, None, "Not a QA / testing role"
    if EXCL_LEVEL.search(t):
        return False, None, None, "Internship / trainee"
    if NOT_SOFTWARE.search(t):
        return False, None, None, "Not software QA (manufacturing, pharma, construction…)"
    if PROCESS_QA.search(t) or (re.search(r"(?i)quality analyst|quality auditor", t) and re.search(PROCESS_QA.pattern, d[:3000]) and not re.search(r"test cases?|bugs?|defects?|regression|software|application|\bapp\b", d[:3000])):
        return False, None, None, "Call-centre / process quality role"
    if HARDWARE.search(t):
        return False, None, None, "Hardware / embedded testing"
    ctxd = (company or "") + "\n" + t + "\n" + d[:1800]
    nsw = len(set(m.group(0).lower() for m in NON_SW.finditer(ctxd)))
    sw = SW_EVIDENCE.search(ctxd)
    if not re.search(r"(?i)software|game|tester|manual|mobile|web|\bapp|functional|application|sdet|automation", t) and (nsw >= 3 or (nsw and not sw)):
        return False, None, None, "Not software QA (manufacturing, pharma, construction…)"
    if SECURITY.search(t):
        return False, None, None, "Security / penetration testing"
    if SENIOR.search(t) and not re.search(r"(?i)(tester|engineer|analyst)\s*/\s*(senior|sr)", t):
        return False, None, None, "Senior / lead level"
    if re.search(r"(?i)expression of interest|talent (pool|community|network)|general application|open application", t):
        return False, None, None, "Talent pool / not a specific opening"
    if re.search(r"(?i)\bearn\b|per day|daily pay|data entry|typing|copy ?paste|form filling|reputed compan|remote review|review(ing)? (apps|products|games) from home|part[- ]time.*(home|students)", t):
        return False, None, None, "Likely work-from-home scheme / not a QA job"
    if re.search(r"(?i)ai (qa )?trainer|llm evaluat|ai evaluat|model evaluat|rlhf|data annotat|labell?ing|transcription|search evaluator", t):
        return False, None, None, "AI training / data labelling"
    if re.search(r"(?i)campaign|ad quality|ads? qa|creative qa|email qa|marketing qa|seo|content qa", t):
        return False, None, None, "Marketing / ad-operations QA"
    if re.search(r"(?i)customer support|technical support|help ?desk|support engineer", t):
        return False, None, None, "Support role"
    if re.search(r"(?i)design for test|\bdft\b", t):
        return False, None, None, "Hardware / embedded testing"
    if re.search(r"(?i)\b(data|etl|dwh|data warehouse|bi|big data|database)\s*(tester|qa|testing|test engineer|quality)|data quality", t):
        return False, None, None, "Data / ETL testing (SQL-heavy)"
    if REGION_IN_TITLE.search(t):
        return False, None, None, "Not open to India: region in title"
    if LANG.search(t) and not re.search(r"(?i)\b(hindi|english)\b", t):
        return False, None, None, "Needs a language other than English or Hindi"
    game_hits = len(re.findall(GAME_D, d[:6000]))
    casino_hits = len(re.findall(CASINO, d[:6000]))
    auto_hits = len(set(re.findall(AUTOMATION, d)))
    game = bool(GAME_T.search(t)) or game_hits >= 4 or bool(re.search(r"(?i)\b(games?|gaming|studios?|interactive|entertainment|casino|play)\b", company or "") and game_hits >= 2)
    automation_title = bool(re.search(r"(?i)\bsdet\b|automat\w+|software development engineer in test|developer|performance test|load test", t))
    automation_heavy = automation_title or (auto_hits >= 4 and not re.search(r"(?i)manual", t + " " + d[:600]))
    if CROWD.search(ctx) or re.search(r"(?i)\bfreelance\b|crowd", t):
        return True, "crowd", "game" if game else "software", "Freelance / crowd-testing role"
    if game:
        if re.search(r"(?i)\blqa\b|locali[sz]ation|linguistic", t): return True, "game-lqa", "game", "Localization QA for games"
        if re.search(r"(?i)certification|compliance|\btrc\b|\bxr\b|lotcheck", t): return True, "game-cert", "game", "Game certification / compliance QA"
        if automation_heavy: return True, "game-automation", "game", "Game QA with test automation"
        if casino_hits >= 2 or re.search(r"(?i)slot|casino|igaming|rmg|real money|betting", t): return True, "slot-qa", "game", "Slot / casino / real-money game QA"
        if re.search(r"(?i)engineer|analyst", t): return True, "game-qa-engineer", "game", "Game QA engineer / analyst"
        return True, "game-tester", "game", "Game tester title"
    if automation_heavy:
        return True, "automation", "software", "Automation-heavy QA (SDET)"
    if re.search(r"(?i)mobile|android|\bios\b|\bapp\b", t):
        return True, "mobile-qa", "software", "Mobile app QA"
    if re.search(r"(?i)manual|tester|functional|application test|test analyst|qa analyst|quality assurance analyst|testing (analyst|specialist|executive|associate)", t):
        return True, "manual-qa", "software", "Manual / functional QA title"
    return True, "qa-engineer", "software", "QA / test engineer title"

def india_eligibility(loc_text, restrictions, desc, remote):
    """Return dict(status, reason, cities, visa, relocation)."""
    lt = " ".join([loc_text or ""] + list(restrictions or []))
    d = desc.lower()
    cities = [c for c, rx in INDIA_CITY.items() if re.search(rx, lt.lower())]
    visa = bool(VISA.search(desc)) and not bool(NO_VISA.search(desc))
    if INDIA.search(lt):
        if not cities and re.search(r"(?i)india", lt):
            cities = []
        return dict(status="confirmed", reason="Location lists India", cities=cities, visa=visa)
    if restrictions:
        rs = " ".join(restrictions)
        if INDIA.search(rs):
            return dict(status="confirmed", reason="India in allowed countries", cities=cities, visa=visa)
        if ASIA.search(rs):
            return dict(status="likely", reason=f"Open to {', '.join(restrictions)[:60]}", cities=cities, visa=visa)
        return dict(status="excluded", reason=f"Restricted to {', '.join(restrictions)[:60]}", cities=cities, visa=visa)
    desc_region = DESC_REGION.search(desc[:6000])
    desc_india = re.search(r"(?i)\b(india|bengaluru|bangalore|\bist\b|apac|asia[- ]pacific)\b", desc[:6000])
    if remote:
        if WORLDWIDE.search(lt) and RESTRICTED.search(lt):
            return dict(status="unconfirmed", reason=f"Says remote/anywhere but lists {lt.strip()[:50]}", cities=cities, visa=visa)
        if not WORLDWIDE.search(lt) and RESTRICTED.search(lt) and not ASIA.search(lt):
            return dict(status="excluded", reason=f"Remote but limited to {lt.strip()[:50]}", cities=cities, visa=visa)
        if re.search(r"(?i)(must|need to|should) (be )?(legally )?(based|reside|residing|located|living) (in|within) (the )?(us|usa|u\.s\.|united states|uk|united kingdom|europe|eu|canada|americas|emea|latam)\b", desc[:8000]) and not desc_india:
            return dict(status="excluded", reason="Description restricts candidate location", cities=cities, visa=visa)
        if desc_region and not desc_india:
            return dict(status="unconfirmed", reason=f"Description mentions {desc_region.group(0).strip()[:40]}", cities=cities, visa=visa)
        if WORLDWIDE.search(lt) or (not lt.strip() or re.fullmatch(r"(?i)\s*remote\s*", lt)) and re.search(r"(?i)work from anywhere|fully distributed|any time ?zone|global team|across the world|worldwide", d):
            return dict(status="likely", reason="Worldwide remote", cities=cities, visa=visa)
        if ASIA.search(lt):
            return dict(status="likely", reason=f"Remote · {lt.strip()[:50]}", cities=cities, visa=visa)
        if RESTRICTED.search(lt):
            return dict(status="excluded", reason=f"Remote but limited to {lt.strip()[:50]}", cities=cities, visa=visa)
        if re.search(r"(?i)(must|need to) (be )?(based|reside|located) in (the )?(us|usa|united states|uk|europe|eu|canada)", d):
            return dict(status="excluded", reason="Description restricts candidate location", cities=cities, visa=visa)
        return dict(status="unconfirmed", reason="Remote, region not stated", cities=cities, visa=visa)
    if not lt.strip():
        return dict(status="unconfirmed", reason="Location not stated", cities=cities, visa=visa)
    # on-site / hybrid overseas
    if visa:
        return dict(status="relocation", reason="Overseas office; mentions visa/relocation support", cities=cities, visa=True)
    return dict(status="excluded", reason=f"On-site outside India ({(lt or 'unspecified').strip()[:40]}), no sponsorship stated", cities=cities, visa=False)

# ---------------------------------------------------------------- salary
SAL_RX = re.compile(r"(?i)(₹|rs\.?|inr|\$|usd|€|eur|£|gbp|cad|aud|sgd)\s?([\d,.]+)\s?(k|lpa|lakh|l\b|m)?\s*(?:-|–|—|to)\s*(₹|rs\.?|inr|\$|usd|€|eur|£|gbp|cad|aud|sgd)?\s?([\d,.]+)\s?(k|lpa|lakh|l\b|m)?")
CUR_MAP = {"₹": "INR", "rs": "INR", "rs.": "INR", "inr": "INR", "$": "USD", "usd": "USD", "€": "EUR", "eur": "EUR", "£": "GBP", "gbp": "GBP", "cad": "CAD", "aud": "AUD", "sgd": "SGD"}

def _num(x, suf, cur):
    v = float(x.replace(",", ""))
    s = (suf or "").lower()
    if s == "k": v *= 1_000
    elif s == "m": v *= 1_000_000
    elif s in ("lpa", "lakh", "l"): v *= 100_000
    return v

LPA_RX = re.compile(r"(?i)(?<![\d.])(\d{1,2}(?:\.\d{1,2})?)\s*(?:-|–|to)\s*(\d{1,2}(?:\.\d{1,2})?)\s*(?:lpa|lakhs?(?: per annum| p\.?a\.?)?|lacs?)\b")
def parse_salary_text(text):
    if not text:
        return None
    m = SAL_RX.search(text)
    if not m:
        m2 = LPA_RX.search(text)
        if m2:
            lo, hi = float(m2.group(1)) * 1e5, float(m2.group(2)) * 1e5
            if 0 < lo <= hi <= 60e5: return dict(currency="INR", min=lo, max=hi, period="annual")
        return None
    cur = CUR_MAP.get((m.group(1) or "").lower().strip())
    lo = _num(m.group(2), m.group(3) or m.group(6), cur)
    hi = _num(m.group(5), m.group(6) or m.group(3), cur)
    if hi < lo: lo, hi = hi, lo
    period = "annual"
    after = text[m.end(): m.end() + 30].lower()
    if re.search(r"hour|/hr|per h", after) or hi < 500: period = "hourly"
    elif re.search(r"month|/mo|pm\b", after) and hi < 60000: period = "monthly"
    if cur == "INR" and hi < 1000:  # e.g. 30-45 (implied LPA)
        lo *= 100_000; hi *= 100_000
    if hi <= 0: return None
    return dict(currency=cur or "USD", min=lo, max=hi, period=period)

def annualize(v, period):
    return v * {"annual": 1, "monthly": 12, "hourly": 2000}.get(period, 1)

def load_fx():
    try:
        d = get_json("https://open.er-api.com/v6/latest/USD")
        return dict(base="USD", rates=d["rates"], as_of=d.get("time_last_update_utc"), fetched_at=iso(NOW), source="https://open.er-api.com/v6/latest/USD (ExchangeRate-API open access)")
    except Exception as e:
        log("fx failed", e)
        return None

def to_inr(amount, cur, fx):
    if amount is None or not fx: return None
    r = fx["rates"]
    if cur not in r: return None
    return amount * r["INR"] / r[cur]

# ---------------------------------------------------------------- record builder
def make_record(**kw):
    rec = dict(
        title=kw["title"].strip(), company=(kw.get("company") or "").strip(), logo=kw.get("logo") or None,
        url=kw.get("url"), apply_url=kw.get("apply_url"), official=bool(kw.get("official")),
        source_id=kw["source_id"], source_name=kw["source_name"], source_kind=kw.get("source_kind", "board"),
        source_job_id=str(kw.get("source_job_id") or kw.get("url")),
        posted_at=kw.get("posted_at"), updated_at=kw.get("updated_at"),
        location_raw=(kw.get("location") or "").strip(), restrictions=kw.get("restrictions") or [],
        remote=kw.get("remote"), workplace=kw.get("workplace"), employment=kw.get("employment") or "",
        desc=kw.get("desc") or "", salary_raw=kw.get("salary"), ats=kw.get("ats"), category_hint=kw.get("category_hint"),
        deadline=kw.get("deadline"), board_apply=kw.get("board_apply"),
    )
    return rec

# ---------------------------------------------------------------- connectors
SOURCES = {
    "workable": dict(name="Workable job marketplace", kind="board", method="Public job search API", url="https://jobs.workable.com/api/v1/jobs", cadence="1h", region="India · Worldwide"),
    "shine": dict(name="Shine.com", kind="board", method="Public search pages (robots.txt allows)", url="https://www.shine.com/job-search", cadence="1h", region="India"),
    "cutshort": dict(name="Cutshort", kind="board", method="Public category pages + JSON-LD (robots.txt allows)", url="https://cutshort.io/jobs", cadence="1h", region="India"),
    "himalayas": dict(name="Himalayas", kind="board", method="Official public API", url="https://himalayas.app/jobs/api", cadence="1h", region="Worldwide remote"),
    "remotive": dict(name="Remotive", kind="board", method="Official public API", url="https://remotive.com/api/remote-jobs", cadence="1h", region="Worldwide remote"),
    "remoteok": dict(name="Remote OK", kind="board", method="Official public API", url="https://remoteok.com/api", cadence="1h", region="Worldwide remote"),
    "workingnomads": dict(name="Working Nomads", kind="board", method="Public jobs API", url="https://www.workingnomads.com/api/exposed_jobs/", cadence="1h", region="Worldwide remote"),
    "jobicy": dict(name="Jobicy", kind="board", method="Official public API", url="https://jobicy.com/api/v2/remote-jobs", cadence="1h", region="Worldwide remote"),
    "hn": dict(name="Hacker News — Who is hiring", kind="community", method="Algolia HN API", url="https://hn.algolia.com/api", cadence="1h", region="Global"),
    "greenhouse": dict(name="Greenhouse (employer boards)", kind="ats", method="Official Job Board API", url="https://boards-api.greenhouse.io", cadence="1h", region="Employer"),
    "ashby": dict(name="Ashby (employer boards)", kind="ats", method="Official Posting API", url="https://api.ashbyhq.com/posting-api", cadence="1h", region="Employer"),
    "lever": dict(name="Lever (employer boards)", kind="ats", method="Official Postings API", url="https://api.lever.co/v0/postings", cadence="1h", region="Employer"),
    "smartrecruiters": dict(name="SmartRecruiters (employer boards)", kind="ats", method="Official Posting API", url="https://api.smartrecruiters.com/v1", cadence="1h", region="Employer"),
}
QA_QUERIES = ["qa tester", "software tester", "manual tester", "qa engineer", "qa analyst", "quality assurance", "test engineer", "functional tester",
              "game tester", "game qa", "mobile app tester", "application tester"]
QA_PRE = re.compile(TITLE_QA.pattern + r"|(?i:game test|playtest)")

def _wk_loc(j):
    L = j.get("location") or {}
    parts = [L.get("city"), L.get("subregion"), L.get("countryName")]
    s = ", ".join(x for x in parts if x)
    return s or ", ".join(j.get("locations") or [])

def c_workable():
    out, seen = [], set()
    plans = [(q, "India") for q in QA_QUERIES + ["keywords studios", "game", "testing"]] + [(q, None) for q in ["game tester", "qa tester remote", "manual qa remote", "software tester remote", "slot game qa", "casino qa"]]
    for q, loc in plans:
        url = "https://jobs.workable.com/api/v1/jobs?query=" + urllib.parse.quote(q) + (f"&location={urllib.parse.quote(loc)}" if loc else "")
        for page in range(2 if loc else 1):
            try:
                d = get_json(url)
            except Exception as e:
                log("workable", q, e); break
            for j in d.get("jobs", []):
                if j.get("id") in seen or j.get("state", "published") != "published": continue
                seen.add(j.get("id"))
                if not QA_PRE.search(j.get("title", "")): continue
                wp = (j.get("workplace") or "").lower()
                loc_s = _wk_loc(j)
                remote = wp == "remote"
                desc = strip_html((j.get("description") or "") + "\n" + (j.get("requirementsSection") or "") + "\n" + (j.get("benefitsSection") or ""))
                co = (j.get("company") or {})
                out.append(make_record(title=j["title"], company=co.get("title"), logo=co.get("image"), url=j.get("url"), apply_url=None,
                    source_id="workable", source_name="Workable job marketplace", source_job_id=j.get("id"), posted_at=parse_date(j.get("created")),
                    updated_at=parse_date(j.get("updated")), location=(loc_s + (" (Remote)" if remote else "")).strip(),
                    restrictions=[] if not remote else ([L.get("countryName")] if (L := (j.get("location") or {})).get("countryName") else []),
                    remote=remote, workplace={"remote": "Remote", "hybrid": "Hybrid", "on_site": "OnSite"}.get(wp), employment=j.get("employmentType") or "",
                    desc=desc, salary=parse_salary_text(desc), board_apply=j.get("url")))
                out[-1]["company_site"] = co.get("website")
            tok = d.get("nextPageToken")
            if not tok: break
            url = url.split("&pageToken=")[0] + "&pageToken=" + urllib.parse.quote(tok)
            time.sleep(0.8)
        time.sleep(0.8)
    return out

def c_remotive():
    out, seen = [], set()
    urls = ["https://remotive.com/api/remote-jobs?category=qa"] + ["https://remotive.com/api/remote-jobs?search=" + urllib.parse.quote(q) for q in ["qa tester", "game tester", "manual qa", "software tester"]]
    for u in urls:
        d = get_json(u)
        for j in d.get("jobs", []):
            if j["id"] in seen: continue
            seen.add(j["id"])
            if not QA_PRE.search(j["title"]): continue
            loc = j.get("candidate_required_location") or ""
            out.append(make_record(title=j["title"], company=j["company_name"], logo=j.get("company_logo"), url=j["url"],
                apply_url=None, source_id="remotive", source_name="Remotive", source_job_id=j["id"],
                posted_at=parse_date(j.get("publication_date")), location=loc, restrictions=[x.strip() for x in re.split(r",|/|;", loc) if x.strip() and not WORLDWIDE.search(x)] if loc and not WORLDWIDE.search(loc) else [],
                remote=True, employment=j.get("job_type", ""), desc=strip_html(j.get("description")), salary=j.get("salary"), category_hint=j.get("category")))
        time.sleep(0.4)
    return out

def c_himalayas():
    out, seen = [], set()
    plans = [(q, "&country=India&sort=recent", 3) for q in ["qa tester", "software tester", "qa engineer", "manual testing", "quality assurance", "game tester", "qa analyst", "test engineer"]]
    plans += [(q, "&sort=recent", 2) for q in ["game tester", "game qa", "manual qa tester", "qa tester"]]
    for q, extra, pages in plans:
        for page in range(1, pages + 1):
            url = "https://himalayas.app/jobs/api/search?q=" + urllib.parse.quote(q) + extra + f"&page={page}"
            try:
                d = get_json(url)
            except Exception as e:
                log("himalayas", q, e); break
            jobs = d.get("jobs", [])
            for j in jobs:
                g = j.get("guid") or j.get("applicationLink")
                if g in seen: continue
                seen.add(g)
                if not QA_PRE.search(j.get("title", "")): continue
                sal = None
                if j.get("minSalary") or j.get("maxSalary"):
                    sal = dict(currency=j.get("currency") or "USD", min=float(j.get("minSalary") or j.get("maxSalary")), max=float(j.get("maxSalary") or j.get("minSalary")), period=(j.get("salaryPeriod") or "annual"))
                out.append(make_record(title=j["title"], company=j.get("companyName"), logo=j.get("companyLogo"), url=j.get("applicationLink") or g,
                    apply_url=None, source_id="himalayas", source_name="Himalayas", source_job_id=g, posted_at=parse_date(j.get("pubDate")),
                    location=", ".join(j.get("locationRestrictions") or []) or "Worldwide", restrictions=j.get("locationRestrictions") or [],
                    remote=True, employment=j.get("employmentType", ""), desc=strip_html(j.get("description")), salary=sal,
                    deadline=iso(parse_date(j.get("expiryDate"))), board_apply=j.get("applicationLink")))
            if len(jobs) < 20: break
            time.sleep(0.6)
    return out

def c_remoteok():
    out, seen = [], set()
    for tag in ["qa", "testing", "quality-assurance", "tester"]:
        try:
            d = get_json(f"https://remoteok.com/api?tag={tag}")
        except Exception as e:
            log("remoteok", tag, e); continue
        for j in d[1:]:
            if j.get("id") in seen: continue
            seen.add(j.get("id"))
            if not QA_PRE.search(j.get("position", "")): continue
            sal = None
            if (j.get("salary_min") or 0) > 0:
                sal = dict(currency="USD", min=float(j["salary_min"]), max=float(j.get("salary_max") or j["salary_min"]), period="annual")
            loc = j.get("location") or ""
            out.append(make_record(title=j.get("position", ""), company=j.get("company"), logo=j.get("company_logo") or j.get("logo"), url=j.get("url"),
                apply_url=None, source_id="remoteok", source_name="Remote OK", source_job_id=j.get("id"),
                posted_at=parse_date(j.get("date") or j.get("epoch")), location=loc or "Worldwide", restrictions=[],
                remote=True, desc=strip_html(j.get("description")), salary=sal))
        time.sleep(1.0)
    return out

def c_workingnomads():
    d = get_json("https://www.workingnomads.com/api/exposed_jobs/")
    out = []
    for j in d:
        if not QA_PRE.search(j.get("title", "")): continue
        loc = j.get("location") or ""
        out.append(make_record(title=j["title"], company=j.get("company_name"), url=j["url"], apply_url=None, source_id="workingnomads",
            source_name="Working Nomads", source_job_id=j["url"], posted_at=parse_date(j.get("pub_date")), location=loc,
            restrictions=[x.strip() for x in loc.split(",") if x.strip()] if loc and not WORLDWIDE.search(loc) else [],
            remote=True, desc=strip_html(j.get("description"))))
    return out

def c_jobicy():
    out, seen = [], set()
    for qs in ["tag=tester", "tag=testing", "tag=quality%20assurance", "tag=game%20tester"]:
        try:
            d = get_json(f"https://jobicy.com/api/v2/remote-jobs?count=100&{qs}")
        except Exception as e:
            log("jobicy", qs, e); continue
        for j in d.get("jobs", []):
            if j["id"] in seen: continue
            seen.add(j["id"])
            if not QA_PRE.search(html.unescape(j.get("jobTitle", ""))): continue
            geo = j.get("jobGeo") or ""
            sal = None
            if j.get("annualSalaryMin") or j.get("salaryMin"):
                lo = float(j.get("annualSalaryMin") or j.get("salaryMin")); hi = float(j.get("annualSalaryMax") or j.get("salaryMax") or lo)
                sal = dict(currency=j.get("salaryCurrency") or "USD", min=lo, max=hi, period="annual" if j.get("annualSalaryMin") else (j.get("salaryPeriod") or "annual"))
            out.append(make_record(title=html.unescape(j["jobTitle"]), company=j.get("companyName"), logo=j.get("companyLogo"), url=j["url"],
                apply_url=None, source_id="jobicy", source_name="Jobicy", source_job_id=j["id"], posted_at=parse_date(j.get("pubDate")),
                location=geo, restrictions=[x.strip() for x in geo.split(",") if x.strip()] if geo and not WORLDWIDE.search(geo) else [],
                remote=True, employment=", ".join(j.get("jobType") or []) if isinstance(j.get("jobType"), list) else str(j.get("jobType") or ""),
                desc=strip_html(j.get("jobDescription")), salary=sal))
        time.sleep(0.5)
    return out

def c_hn():
    d = get_json("https://hn.algolia.com/api/v1/search_by_date?query=%22who%20is%20hiring%22&tags=story,author_whoishiring&hitsPerPage=6")
    threads = [h for h in d.get("hits", []) if "who is hiring" in (h.get("title") or "").lower()][:2]
    out = []
    for th in threads:
        for term in ["qa engineer", "qa tester", "manual qa", "game tester", "test engineer"]:
            try:
                c = get_json(f"https://hn.algolia.com/api/v1/search?tags=comment,story_{th['objectID']}&query={urllib.parse.quote(term)}&hitsPerPage=40")
            except Exception as e:
                log("hn", e); continue
            for h in c.get("hits", []):
                if str(h.get("parent_id")) != str(th["objectID"]): continue
                text = strip_html(h.get("comment_text") or "")
                first = text.split("\n")[0][:200]
                if first.count("|") < 2: continue
                parts = [p.strip() for p in first.split("|")]
                company = parts[0] if parts else "Unknown"
                titles = [p for p in parts if QA_PRE.search(p)]
                if not titles: continue
                loc = " | ".join(p for p in parts[1:] if re.search(r"(?i)remote|onsite|hybrid|india|worldwide|anywhere|,", p))[:120]
                out.append(make_record(title=titles[0][:120], company=company[:80], url=f"https://news.ycombinator.com/item?id={h['objectID']}",
                    apply_url=None, source_id="hn", source_name="Hacker News — Who is hiring", source_kind="community", source_job_id=h["objectID"],
                    posted_at=parse_date(h.get("created_at")), location=loc, remote=bool(re.search(r"(?i)remote", first)), desc=text))
            time.sleep(0.3)
    return out

CUTSHORT_PAGES = ["game-testing-qa-jobs", "manual-testing-jobs-in-mumbai", "manual-testing-jobs", "mobile-app-testing-qa-jobs", "functional-testing-jobs", "regression-testing-jobs"]
def _next_jobs(t):
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', t, re.S)
    if not m: return []
    def find(o):
        if isinstance(o, dict):
            pd_ = o.get("pageData")
            if isinstance(pd_, dict) and isinstance(pd_.get("jobs"), list): return pd_["jobs"]
            for v in o.values():
                r = find(v)
                if r is not None: return r
        elif isinstance(o, list):
            for v in o:
                r = find(v)
                if r is not None: return r
        return None
    try: return find(json.loads(m.group(1))) or []
    except Exception: return []

def c_cutshort():
    """Cutshort category pages (robots.txt allows /jobs/ and /job/); each QA posting's own page carries JSON-LD with its date and text."""
    cands, seen = [], set()
    for slug in CUTSHORT_PAGES:
        s_, b = http_get(f"https://cutshort.io/jobs/{slug}", accept="text/html")
        if s_ != 200: continue
        for j in _next_jobs(b.decode("utf-8", "ignore")):
            u = j.get("publicUrl") or ""
            if not u.startswith("https://cutshort.io/job/") or u in seen: continue
            seen.add(u)
            if QA_PRE.search(j.get("headline") or ""): cands.append(j)
        time.sleep(0.8)
    out = []
    for j in cands[:45]:
        s_, b = http_get(j["publicUrl"], accept="text/html")
        if s_ != 200: continue
        t = b.decode("utf-8", "ignore"); jp = None
        for m in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', t, re.S):
            if '"JobPosting"' in m.group(1):
                try: jp = json.loads(m.group(1)); break
                except Exception: pass
        if not jp: continue
        vt = parse_date(jp.get("validThrough"))
        if vt and vt < NOW: continue
        locs = j.get("locations") or []
        remote = bool(re.search(r"(?i)remote", json.dumps(j.get("remoteType") or "") + " " + (j.get("locationsText") or "")))
        bs = ((jp.get("baseSalary") or {}).get("value") or {})
        sal = None
        if bs.get("maxValue") and 1e5 <= float(bs["maxValue"]) <= 6e6:
            sal = dict(currency=(jp.get("baseSalary") or {}).get("currency") or "INR", min=float(bs.get("minValue") or bs["maxValue"]), max=float(bs["maxValue"]), period="annual")
        months = ((jp.get("experienceRequirements") or {}).get("monthsOfExperience"))
        desc = strip_html(jp.get("description") or "")
        if months and not re.search(r"\d+\s*\+?\s*(?:-|to)?\s*\d*\s*years?", desc[:3000]):
            desc = f"Experience: {int(months) // 12} years\n" + desc
        co = (j.get("companyDetails") or {}).get("name") or ((jp.get("hiringOrganization") or {}).get("name"))
        rec = make_record(title=jp.get("title") or j.get("headline"), company=co, url=j["publicUrl"], apply_url=None, source_id="cutshort", source_name="Cutshort",
            source_job_id=j.get("_id") or j["publicUrl"], posted_at=parse_date(jp.get("datePosted")), location=", ".join(locs) + (", India" if locs else "India"),
            remote=remote, employment=", ".join(jp.get("employmentType") or []) if isinstance(jp.get("employmentType"), list) else str(jp.get("employmentType") or ""),
            desc=desc, salary=sal, deadline=iso(vt) if vt else None, board_apply=j["publicUrl"])
        if j.get("hiringForClient"): rec["recruiter"] = (jp.get("hiringOrganization") or {}).get("name")
        out.append(rec)
        time.sleep(0.7)
    return out

SHINE_PAGES = ["game-tester-jobs-in-mumbai", "game-testing-jobs-in-mumbai", "qa-tester-jobs-in-mumbai", "software-tester-jobs-in-mumbai", "manual-testing-jobs-in-mumbai",
               "manual-tester-jobs-in-mumbai", "qa-engineer-jobs-in-mumbai", "test-engineer-jobs-in-mumbai", "qa-tester-jobs-in-navi-mumbai", "qa-tester-jobs-in-thane",
               "game-tester-jobs", "game-testing-jobs", "game-qa-jobs", "qa-tester-jobs-in-pune", "game-tester-jobs-in-pune", "qa-tester-work-from-home-jobs"]
RECRUITER_NAME = re.compile(r"(?i)consult|staffing|hr solutions|hr services|placement|manpower|recruit|talent (stock|solutions)|hiring for|jobs consultancy|outsourc|human resource|career(s)? (point|zone)")
def _shine_jobs(t):
    s = ""
    for c in re.findall(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)', t):
        try: s += json.loads('"' + c + '"')
        except Exception: pass
    out, dec = [], json.JSONDecoder()
    for m in re.finditer(r'\{"badges":', s):
        try:
            o, _ = dec.raw_decode(s, m.start())
            if o.get("detailUrl") and o.get("title"): out.append(o)
        except Exception: pass
    return out

def c_shine():
    """Shine.com search pages (robots.txt allows /job-search/<slug>); listings carry title, company, experience, locations, date and pay label."""
    out, seen = [], set()
    for slug in SHINE_PAGES:
        s_, b = http_get(f"https://www.shine.com/job-search/{slug}", accept="text/html")
        if s_ != 200: continue
        for o in _shine_jobs(b.decode("utf-8", "ignore")):
            jid = str(o.get("id") or o["detailUrl"])
            if jid in seen: continue
            seen.add(jid)
            if not QA_PRE.search(o["title"]): continue
            locs = [l for l in (o.get("locations") or []) if l]
            wfh = any(re.search(r"(?i)work from home|remote", l) for l in locs)
            loc = ", ".join(locs[:6]) + (", India" if locs and not any(re.search(r"(?i)india", l) for l in locs) else ("India" if not locs else ""))
            skills = ", ".join(o.get("skills") or [])
            desc = f"Experience: {o.get('experienceLabel') or 'not stated'}\nSkills: {skills}\n(Summary from the Shine.com listing; open the posting for the full description.)"
            sal = parse_salary_text(o.get("salaryLabel") or "") if re.search(r"\d", o.get("salaryLabel") or "") else None
            rec = make_record(title=o["title"], company=o.get("companyName"), logo=o.get("listingUrl") if (o.get("listingUrl") or "").startswith("https://") else None,
                url="https://www.shine.com" + o["detailUrl"], apply_url=None, source_id="shine", source_name="Shine.com", source_job_id=jid,
                posted_at=parse_date(o.get("postedAt")), location=loc, remote=wfh, desc=desc, salary=sal, board_apply="https://www.shine.com" + o["detailUrl"])
            if RECRUITER_NAME.search(o.get("companyName") or ""): rec["recruiter"] = o.get("companyName")
            out.append(rec)
        time.sleep(1.0)
    return out

# ---- ATS
ATS_TITLE_PREFILTER = QA_PRE

def ats_greenhouse(slug, cname):
    d = get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    BOARD_CACHE[("greenhouse", slug)] = [dict(title=j.get("title", ""), loc=(j.get("location") or {}).get("name", ""), url=j.get("absolute_url"), cc=None,
                                              remote=bool(re.search(r"(?i)remote", (j.get("location") or {}).get("name", "")))) for j in d.get("jobs", [])]
    out = []
    for j in d.get("jobs", []):
        if not ATS_TITLE_PREFILTER.search(j["title"]): continue
        try:
            det = get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{j['id']}?pay_transparency=true")
        except Exception:
            det = j
        content = strip_html(det.get("content") or "")
        loc = (det.get("location") or {}).get("name", "")
        sal = None
        pr = det.get("pay_input_ranges") or []
        if pr:
            p = pr[0]
            try:
                sal = dict(currency=p.get("currency_type") or "USD", min=float(p["min_cents"]) / 100, max=float(p["max_cents"]) / 100, period="annual", note=p.get("title") or "")
            except Exception:
                sal = None
        out.append(make_record(title=j["title"], company=cname, url=j["absolute_url"], apply_url=j["absolute_url"], official=True,
            source_id="greenhouse", source_name=f"{cname} careers (Greenhouse)", source_kind="ats", source_job_id=f"gh:{slug}:{j['id']}",
            posted_at=parse_date(det.get("first_published") or j.get("first_published")), updated_at=parse_date(j.get("updated_at")),
            location=loc, remote=bool(re.search(r"(?i)remote", loc)), desc=content, salary=sal or parse_salary_text(content),
            ats=dict(type="greenhouse", slug=slug), deadline=det.get("application_deadline")))
        time.sleep(0.15)
    return out

def ats_ashby(slug, cname):
    d = get_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true")
    BOARD_CACHE[("ashby", slug)] = [dict(title=j.get("title", ""), url=j.get("jobUrl"),
        loc=" • ".join([x for x in [j.get("location") or ""] + [y.get("location", "") for y in (j.get("secondaryLocations") or [])] if x]),
        cc=(((j.get("address") or {}).get("postalAddress") or {}).get("addressCountry")), remote=bool(j.get("isRemote")) or j.get("workplaceType") == "Remote")
        for j in d.get("jobs", []) if j.get("isListed", True)]
    out = []
    for j in d.get("jobs", []):
        if not j.get("isListed", True) or not ATS_TITLE_PREFILTER.search(j["title"]): continue
        locs = [j.get("location") or ""] + [x.get("location", "") for x in (j.get("secondaryLocations") or [])]
        addr = ((j.get("address") or {}).get("postalAddress") or {})
        loc = " • ".join([l for l in locs if l]) + (f" ({addr.get('addressCountry')})" if addr.get("addressCountry") and addr.get("addressCountry") not in " ".join(locs) else "")
        sal = None
        comp = j.get("compensation") or {}
        for tier in comp.get("compensationTiers") or []:
            for c in tier.get("components") or []:
                if c.get("compensationType") == "Salary" and (c.get("minValue") or c.get("maxValue")):
                    sal = dict(currency=c.get("currencyCode") or "USD", min=float(c.get("minValue") or c.get("maxValue")), max=float(c.get("maxValue") or c.get("minValue")),
                               period={"1 YEAR": "annual", "1 MONTH": "monthly", "1 HOUR": "hourly"}.get(c.get("interval"), "annual"), note=tier.get("title") or "")
                    break
            if sal: break
        if not sal and comp.get("scrapeableCompensationSalarySummary"):
            sal = parse_salary_text(comp["scrapeableCompensationSalarySummary"])
        desc = j.get("descriptionPlain") or strip_html(j.get("descriptionHtml"))
        out.append(make_record(title=j["title"], company=cname, url=j["jobUrl"], apply_url=j.get("applyUrl") or j["jobUrl"], official=True,
            source_id="ashby", source_name=f"{cname} careers (Ashby)", source_kind="ats", source_job_id=f"ashby:{slug}:{j['id']}",
            posted_at=parse_date(j.get("publishedAt")), location=loc, remote=bool(j.get("isRemote")) or j.get("workplaceType") == "Remote",
            workplace=j.get("workplaceType"), employment=j.get("employmentType", ""), desc=desc, salary=sal or parse_salary_text(desc),
            ats=dict(type="ashby", slug=slug)))
    return out

def ats_lever(slug, cname):
    d = get_json(f"https://api.lever.co/v0/postings/{slug}?mode=json")
    BOARD_CACHE[("lever", slug)] = [dict(title=j.get("text", ""), url=j.get("hostedUrl"), cc=j.get("country"),
        loc=" • ".join((j.get("categories") or {}).get("allLocations") or [(j.get("categories") or {}).get("location") or ""]),
        remote=(j.get("workplaceType") == "remote")) for j in d]
    out = []
    for j in d:
        if not ATS_TITLE_PREFILTER.search(j.get("text", "")): continue
        cat = j.get("categories") or {}
        loc = " • ".join(cat.get("allLocations") or [cat.get("location") or ""])
        desc = strip_html((j.get("description") or "") + "\n" + "\n".join((l.get("text", "") + "\n" + (l.get("content") or "")) for l in (j.get("lists") or [])) + "\n" + (j.get("additional") or ""))
        sal = None
        sr = j.get("salaryRange")
        if sr and (sr.get("min") or sr.get("max")):
            sal = dict(currency=sr.get("currency") or "USD", min=float(sr.get("min") or sr.get("max")), max=float(sr.get("max") or sr.get("min")),
                       period={"per-year-salary": "annual", "per-month-salary": "monthly", "per-hour-wage": "hourly"}.get(sr.get("interval"), "annual"))
        out.append(make_record(title=j["text"], company=cname, url=j["hostedUrl"], apply_url=j.get("applyUrl") or j["hostedUrl"], official=True,
            source_id="lever", source_name=f"{cname} careers (Lever)", source_kind="ats", source_job_id=f"lever:{slug}:{j['id']}",
            posted_at=parse_date(j.get("createdAt")), location=loc, remote=(j.get("workplaceType") == "remote") or bool(re.search(r"(?i)remote", loc)),
            workplace=j.get("workplaceType"), employment=cat.get("commitment", ""), desc=desc, salary=sal or parse_salary_text(desc),
            ats=dict(type="lever", slug=slug)))
    return out

def ats_smartrecruiters(slug, cname):
    out = []
    for q in ["qa", "test", "quality", "tester"]:
        d = get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?q={urllib.parse.quote(q)}&limit=100")
        for j in d.get("content", []):
            if not ATS_TITLE_PREFILTER.search(j.get("name", "")): continue
            if any(o["source_job_id"] == f"sr:{slug}:{j['id']}" for o in out): continue
            L = j.get("location") or {}
            loc = L.get("fullLocation") or ", ".join(x for x in [L.get("city"), L.get("country")] if x)
            try:
                det = get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{j['id']}")
                secs = (det.get("jobAd") or {}).get("sections") or {}
                desc = strip_html("\n".join((v or {}).get("text", "") for v in secs.values()))
                url = det.get("postingUrl") or det.get("applyUrl")
                apply = det.get("applyUrl") or url
            except Exception:
                desc, url, apply = "", f"https://jobs.smartrecruiters.com/{slug}/{j['id']}", f"https://jobs.smartrecruiters.com/{slug}/{j['id']}"
            out.append(make_record(title=j["name"], company=cname, url=url, apply_url=apply, official=True,
                source_id="smartrecruiters", source_name=f"{cname} careers (SmartRecruiters)", source_kind="ats", source_job_id=f"sr:{slug}:{j['id']}",
                posted_at=parse_date(j.get("releasedDate")), location=loc + (" (Remote)" if L.get("remote") else (" (Hybrid)" if L.get("hybrid") else "")),
                remote=bool(L.get("remote")), workplace="Hybrid" if L.get("hybrid") else ("Remote" if L.get("remote") else "OnSite"),
                employment=(j.get("typeOfEmployment") or {}).get("label", ""), desc=desc, salary=parse_salary_text(desc),
                ats=dict(type="smartrecruiters", slug=slug)))
        time.sleep(0.3)
    return out

ATS_FN = {"greenhouse": ats_greenhouse, "ashby": ats_ashby, "lever": ats_lever, "smartrecruiters": ats_smartrecruiters}

def ats_probe_company(name):
    """Try to find an official ATS board for a company seen on a job board."""
    k = company_key(name)
    cands = list(dict.fromkeys([k, slugify(name).replace("-", ""), slugify(name)]))
    for slug in cands:
        for typ, url in [("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"),
                         ("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}"),
                         ("lever", f"https://api.lever.co/v0/postings/{slug}?mode=json")]:
            s, b = http_get(url, timeout=15, tries=1)
            if s == 200 and b:
                try:
                    d = json.loads(b)
                    n = len(d["jobs"]) if isinstance(d, dict) else len(d)
                    if n > 0:
                        return dict(type=typ, slug=slug)
                except Exception:
                    pass
    return None

def _tkey(t):
    t = norm(re.sub(r"(?i)\(.*?\)|- *(remote|contract|12 month.*)$", " ", t))
    t = re.sub(r"\b(sr|snr)\b", "senior", t)
    t = re.sub(r"\b(graphics|graphic)\b", "graphic", t)
    return re.sub(r"\s+", " ", t).strip()

def job_id_for(company, title):
    return hid(company_key(company), _tkey(title))

ATS_URL = [
    (re.compile(r"^https?://jobs\.ashbyhq\.com/([^/?#]+)/([0-9a-f-]{36})"), "ashby"),
    (re.compile(r"^https?://(?:job-boards|boards)(?:\.eu)?\.greenhouse\.io/([^/?#]+)/jobs/(\d+)"), "greenhouse"),
    (re.compile(r"^https?://jobs\.lever\.co/([^/?#]+)/([0-9a-f-]{36})"), "lever"),
]

def resolve_official_leads(leads):
    """Replace search-found ATS links with the employer's own posting record (location, pay, text).
    Returns (records_to_add, unresolved_leads, dropped)."""
    out, keep, dropped, boards = [], [], [], {}
    for L in leads:
        url = L.get("url") or ""
        hit = None
        for rx, typ in ATS_URL:
            m = rx.match(url)
            if m: hit = (typ, urllib.parse.unquote(m.group(1)), m.group(2)); break
        if not hit:
            keep.append(L); continue
        typ, slug, pid = hit
        if (typ, slug) not in boards:
            try: boards[(typ, slug)] = ATS_FN[typ](slug, L.get("company") or slug)
            except Exception: boards[(typ, slug)] = None
        recs = boards[(typ, slug)]
        if recs is None:
            keep.append(L); continue
        match = [r for r in recs if pid in str(r.get("source_job_id")) or pid in (r.get("url") or "")]
        if match:
            r = dict(match[0]); r["company"] = L.get("company") or r["company"]; out.append(r)
        else:
            dropped.append(dict(title=L.get("title"), company=L.get("company"), url=url, reason="Not found on the employer's live job board (closed or filtered)"))
    return out, keep, dropped

# ---------------------------------------------------------------- hires-from-India check (company level)
BOARD_CACHE = {}

COUNTRY_RX = [
    ("India", r"\bindia\b|bengaluru|bangalore|mumbai|\bpune\b|new delhi|\bdelhi\b|gurugram|gurgaon|noida|hyderabad|chennai|kolkata|ahmedabad|kochi"),
    ("United States", r"united states|\busa\b|u\.s\.a?\.?|\bus\b|new york|\bnyc\b|san francisco|\bsf\b|seattle|austin|chicago|boston|los angeles|denver|atlanta|miami|palo alto|menlo park|mountain view|san jose|san mateo|redwood city|sunnyvale|santa clara|washington,? d\.?c|philadelphia|portland|salt lake|pittsburgh|cambridge, ma|raleigh|dallas|houston|san diego|\bnc\b|\bca\b, us"),
    ("Canada", r"canada|toronto|vancouver|montreal|ottawa|calgary|waterloo"),
    ("United Kingdom", r"united kingdom|\buk\b|england|scotland|london|manchester|edinburgh|bristol"),
    ("Ireland", r"ireland|dublin"), ("Germany", r"germany|berlin|munich|hamburg|frankfurt|cologne"), ("France", r"france|paris"),
    ("Netherlands", r"netherlands|amsterdam|rotterdam|utrecht"), ("Spain", r"spain|madrid|barcelona"), ("Portugal", r"portugal|lisbon|porto"),
    ("Poland", r"poland|warsaw|krakow|wroclaw"), ("Sweden", r"sweden|stockholm"), ("Denmark", r"denmark|copenhagen"), ("Norway", r"norway|oslo"),
    ("Finland", r"finland|helsinki"), ("Switzerland", r"switzerland|zurich|geneva"), ("Austria", r"austria|vienna"), ("Italy", r"italy|milan|rome"),
    ("Belgium", r"belgium|brussels"), ("Czechia", r"czech|prague"), ("Estonia", r"estonia|tallinn"), ("Lithuania", r"lithuania|vilnius"),
    ("Ukraine", r"ukraine|kyiv|kiev|lviv"), ("Romania", r"romania|bucharest"), ("Greece", r"greece|athens"), ("Turkey", r"turkey|türkiye|istanbul"),
    ("Israel", r"israel|tel aviv"), ("UAE", r"\buae\b|united arab emirates|dubai|abu dhabi"), ("Singapore", r"singapore"),
    ("Japan", r"japan|tokyo"), ("South Korea", r"korea|seoul"), ("China", r"\bchina\b|shanghai|beijing|shenzhen"), ("Hong Kong", r"hong kong"),
    ("Taiwan", r"taiwan|taipei"), ("Philippines", r"philippines|manila"), ("Indonesia", r"indonesia|jakarta"), ("Vietnam", r"vietnam|ho chi minh|hanoi"),
    ("Malaysia", r"malaysia|kuala lumpur"), ("Thailand", r"thailand|bangkok"), ("Australia", r"australia|sydney|melbourne|brisbane"),
    ("New Zealand", r"new zealand|auckland"), ("Brazil", r"brazil|são paulo|sao paulo"), ("Mexico", r"mexico"), ("Argentina", r"argentina|buenos aires"),
    ("Colombia", r"colombia|bogot"), ("Chile", r"chile|santiago"), ("South Africa", r"south africa|cape town|johannesburg"), ("Nigeria", r"nigeria|lagos"),
    ("Kenya", r"kenya|nairobi"), ("Egypt", r"egypt|cairo"), ("Pakistan", r"pakistan|karachi|lahore"),
    ("Armenia", r"armenia|yerevan"), ("Serbia", r"serbia|belgrade"), ("Croatia", r"croatia|zagreb"), ("Bulgaria", r"bulgaria|sofia"),
    ("Hungary", r"hungary|budapest"), ("Slovakia", r"slovakia|bratislava"), ("Slovenia", r"slovenia|ljubljana"), ("Latvia", r"latvia|riga"),
    ("Cyprus", r"cyprus|limassol|nicosia"), ("Malta", r"\bmalta\b"), ("Luxembourg", r"luxembourg"), ("Iceland", r"iceland|reykjavik"),
    ("Morocco", r"morocco|casablanca"), ("Ghana", r"ghana|accra"), ("Bangladesh", r"bangladesh|dhaka"), ("Sri Lanka", r"sri lanka|colombo"),
    ("Nepal", r"nepal|kathmandu"), ("Saudi Arabia", r"saudi|riyadh"), ("Qatar", r"qatar|doha"), ("Peru", r"\bperu\b|lima, peru"),
    ("Uruguay", r"uruguay|montevideo"), ("Costa Rica", r"costa rica"), ("Kazakhstan", r"kazakhstan|almaty"),
]
COUNTRY_RX = [(n, re.compile(r"(?i)(" + rx + r")")) for n, rx in COUNTRY_RX]
CC_NAME = {"IN": "India", "US": "United States", "CA": "Canada", "GB": "United Kingdom", "UK": "United Kingdom", "IE": "Ireland", "DE": "Germany", "FR": "France",
           "NL": "Netherlands", "ES": "Spain", "PT": "Portugal", "PL": "Poland", "SE": "Sweden", "DK": "Denmark", "NO": "Norway", "FI": "Finland", "CH": "Switzerland",
           "AT": "Austria", "IT": "Italy", "BE": "Belgium", "CZ": "Czechia", "EE": "Estonia", "LT": "Lithuania", "UA": "Ukraine", "RO": "Romania", "GR": "Greece",
           "TR": "Turkey", "IL": "Israel", "AE": "UAE", "SG": "Singapore", "JP": "Japan", "KR": "South Korea", "CN": "China", "HK": "Hong Kong", "TW": "Taiwan",
           "PH": "Philippines", "ID": "Indonesia", "VN": "Vietnam", "MY": "Malaysia", "TH": "Thailand", "AU": "Australia", "NZ": "New Zealand", "BR": "Brazil",
           "MX": "Mexico", "AR": "Argentina", "CO": "Colombia", "CL": "Chile", "ZA": "South Africa", "NG": "Nigeria", "KE": "Kenya", "EG": "Egypt", "PK": "Pakistan"}

GLOBAL_HIRING = re.compile(r"(?i)((?:team|people|teammates|employees|members|colleagues)[^.\n]{0,30}\b(?:all over the world|around the world|across the globe|across the world|worldwide)|\b(?:in|across|from)\s+(?:over\s+|more than\s+)?\d{2,3}\+?\s+countries\b|employer of record|\beor\b|\bdeel\b|remote\.com\b|\boyster\s?hr\b|velocity global|papaya global|\bmultiplier\b|hire (?:talent |people )?(?:from )?anywhere|work from anywhere in the world|we hire globally|hiring globally|hire globally|globally distributed (?:team|company)|fully distributed (?:team|company)|location[- ]independent)")
ONLY_HIRE = re.compile(r"(?i)((?:we\s+(?:can|are able to)\s+only|can only|only able to|only eligible to)\s+(?:hire|employ|consider)[^.\n]{0,80}|(?:candidates|applicants|you)\s+(?:must|need to)\s+(?:be\s+)?(?:legally\s+)?(?:based|located|residing|reside|live)\s+in[^.\n]{0,60}|not able to (?:hire|employ) (?:outside|in)[^.\n]{0,60})")
INDIA_PRESENCE = re.compile(r"(?i)(\b(?:office|offices|team|hub|centre|center|studio|entity|engineering center)\b[^.\n]{0,40}\b(?:india|bengaluru|bangalore|hyderabad|pune|mumbai|gurugram|gurgaon|noida|chennai|new delhi)\b|\b(?:india|bengaluru|bangalore|hyderabad|pune|mumbai|gurugram|gurgaon|chennai)\b[^.\n]{0,25}\b(?:office|team|hub|centre|center|entity)\b|(?:hire|hiring|recruit\w*|jobs?|roles?|openings?)\b[^.\n]{0,50}\bindia\b|\bindia\b[^.\n]{0,40}\b(?:hire|hiring|jobs?|roles?|openings?|remote|careers?)\b)")
INDIA_HQ = re.compile(r"(?i)(,\s*IN\b|\bindia\b|bengaluru|bangalore|mumbai|pune|gurugram|gurgaon|noida|hyderabad|chennai|new delhi)")


def ats_footprint(typ, slug):
    """All open roles on a company's own job board, reduced to where each role can be done from."""
    key = (typ, slug)
    if key in BOARD_CACHE:
        return BOARD_CACHE[key]
    rows = []
    try:
        if typ == "greenhouse":
            d = get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            rows = [dict(title=j.get("title", ""), loc=(j.get("location") or {}).get("name", ""), url=j.get("absolute_url"), cc=None,
                         remote=bool(re.search(r"(?i)remote", (j.get("location") or {}).get("name", "")))) for j in d.get("jobs", [])]
        elif typ == "ashby":
            d = get_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            rows = [dict(title=j.get("title", ""), url=j.get("jobUrl"), loc=" • ".join([x for x in [j.get("location") or ""] + [y.get("location", "") for y in (j.get("secondaryLocations") or [])] if x]),
                         cc=(((j.get("address") or {}).get("postalAddress") or {}).get("addressCountry")), remote=bool(j.get("isRemote")) or j.get("workplaceType") == "Remote")
                    for j in d.get("jobs", []) if j.get("isListed", True)]
        elif typ == "lever":
            d = get_json(f"https://api.lever.co/v0/postings/{slug}?mode=json")
            rows = [dict(title=j.get("text", ""), url=j.get("hostedUrl"), cc=j.get("country"), loc=" • ".join((j.get("categories") or {}).get("allLocations") or [(j.get("categories") or {}).get("location") or ""]),
                         remote=(j.get("workplaceType") == "remote")) for j in d]
        elif typ == "smartrecruiters":
            for off in (0, 100, 200):
                d = get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100&offset={off}")
                for j in d.get("content", []):
                    L = j.get("location") or {}
                    rows.append(dict(title=j.get("name", ""), url=f"https://jobs.smartrecruiters.com/{slug}/{j.get('id')}", cc=(L.get("country") or "").upper() or None,
                                     loc=L.get("fullLocation") or ", ".join(x for x in [L.get("city"), L.get("country")] if x), remote=bool(L.get("remote"))))
                if len(d.get("content", [])) < 100:
                    break
    except Exception as e:
        log("footprint", key, str(e)[:120])
        rows = None
    BOARD_CACHE[key] = rows
    return rows


def _countries(text, cc=None):
    out = []
    if cc:
        n = CC_NAME.get(str(cc).upper()) or (str(cc) if len(str(cc)) > 3 else None)
        if n: out.append(n)
    for n, rx in COUNTRY_RX:
        if rx.search(text or "") and n not in out:
            out.append(n)
    return out


def summarize_board(rows):
    if rows is None:
        return None
    india, apac, world, restricted, remote_n = [], [], [], [], 0
    cnt = {}
    for r in rows:
        loc = r.get("loc") or ""
        cs = _countries(loc, r.get("cc"))
        for c in cs: cnt[c] = cnt.get(c, 0) + 1
        if r.get("remote") or re.search(r"(?i)remote", loc): remote_n += 1
        if "India" in cs or INDIA.search(loc):
            india.append(r)
        elif ASIA.search(loc):
            apac.append(r)
        elif WORLDWIDE.search(loc) and not [c for c in cs if c != "India"]:
            world.append(r)
        elif (r.get("remote") or re.search(r"(?i)remote", loc)) and cs:
            restricted.append(r)
    top = [k for k, _ in sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))]
    pick = lambda xs: [dict(title=x["title"][:90], location=(x.get("loc") or "")[:80], url=x.get("url")) for x in sorted(xs, key=lambda x: x["title"])[:4]]
    return dict(total=len(rows), india=len(india), apac=len(apac), worldwide=len(world), remote=remote_n, restricted_remote=len(restricted),
                countries=top[:8], india_samples=pick(india), open_samples=pick(apac + world), restricted_samples=pick(restricted))


BOARD_HOST = re.compile(r"(?i)^(?:apply\.workable\.com|jobs\.ashbyhq\.com|(?:job-)?boards\.greenhouse\.io|jobs\.lever\.co|[a-z0-9-]+\.recruitee\.com|jobs\.smartrecruiters\.com|[a-z0-9-]+\.breezy\.hr|[a-z0-9-]+\.teamtailor\.com|[a-z0-9-]+\.bamboohr\.com|[a-z0-9-]+\.personio\.(?:de|com))$")
LIMITED_TO = re.compile(r"(?i)\bhiring (?:only )?in (?:the )?(united states|usa|us|uk|united kingdom|canada|europe|eu|germany|australia)\b")


def _about_company(h, name):
    """True when a search result is clearly about this company: its domain or job-board path is the company's name, or the page title names it."""
    key = re.sub(r"[^a-z0-9]", "", company_key(name or ""))
    url = h.get("url") or ""
    host = re.sub(r"^https?://", "", url).split("/")[0].lower()
    path = (re.sub(r"^https?://[^/]+/?", "", url).split("/") or [""])[0].lower()
    labels = [x.replace("-", "") for x in host.split(".") if x not in ("www", "careers", "jobs", "apply", "boards", "job-boards", "com", "io", "co", "ai", "app", "org", "net", "in", "uk", "de")]
    if key and len(key) >= 3 and (key in labels or (BOARD_HOST.match(host) and (path.replace("-", "") == key or host.split(".")[0].replace("-", "") == key))):
        return True
    title = h.get("title") or ""
    if not name: return False
    flags = 0 if (name.isupper() or len(name) <= 4) else re.I
    return bool(re.search(r"(?<![A-Za-z0-9])" + re.escape(name) + r"(?![A-Za-z0-9])", title, flags))


def classify_web_hits(hits, name=""):
    """Search-result snippets about a company's hiring footprint -> (kind, hit, quote, weight).
    Only results that are clearly about this company count."""
    out = []
    for h in hits or []:
        t = f"{h.get('title', '')} — {h.get('description', '')}"
        if not safe_http(h.get("url")) or not _about_company(h, name):
            continue
        host = re.sub(r"^https?://", "", h["url"]).split("/")[0].lower()
        m = INDIA_PRESENCE.search(t)
        if m: out.append(("india", h, m.group(0), 3)); continue
        m = GLOBAL_HIRING.search(t) or re.search(r"(?i)(?:\bin anywhere\b|remote[- ]anywhere|\(anywhere\)|— anywhere)", h.get("title", ""))
        if m: out.append(("global", h, m.group(0), 2)); continue
        if BOARD_HOST.match(host):
            hits_c = [n for n, rx in COUNTRY_RX for _ in rx.finditer(h.get("description", ""))]
            names = list(dict.fromkeys(hits_c))
            if names and "India" not in names:
                out.append(("restricted", h, "Lists roles only in " + _list_names(names), -2 if len(hits_c) >= 3 else -1)); continue
        m = ONLY_HIRE.search(t)
        if m and not INDIA.search(m.group(0)): out.append(("restricted", h, m.group(0), -2 if BOARD_HOST.match(host) else -1)); continue
        m = LIMITED_TO.search(t)
        if m: out.append(("restricted", h, m.group(0), -1))
    return out


def safe_http(u):
    return bool(u) and bool(re.match(r"^https?://", str(u)))


def _list_names(xs, n=4):
    xs = list(xs)
    if not xs: return ""
    if len(xs) <= n: return ", ".join(xs[:-1]) + (" and " if len(xs) > 1 else "") + xs[-1]
    return ", ".join(xs[:n]) + f" and {len(xs) - n} more"


def india_hiring_check(name, company, jobs, descs, web_hits=None, app_check=None, prev=None):
    """Company-level verdict: does this employer hire people based in India, or only in its own countries?
    Returns a dict with verdict yes | likely | no | unclear plus the public evidence behind it."""
    ev = []
    def add(kind, label, detail="", url=None, weight=0):
        ev.append(dict(kind=kind, label=label, detail=detail[:220], url=url if safe_http(url) else None, weight=weight))
    hq = (company.get("hq") or "")
    curated = company.get("classification", "").startswith("QA Intelligence registry")
    india_hq = bool(hq and INDIA_HQ.search(re.split(r"\s*/\s*", hq)[0])) and curated
    if india_hq:
        add("registry", "Headquartered in India", hq, company.get("careers"), 5)
    elif curated and hq and INDIA_HQ.search(hq):
        add("registry", "Has an office in India", hq, company.get("careers"), 5)
    board = None
    ats = company.get("ats")
    if ats and ats.get("type") in ("greenhouse", "ashby", "lever", "smartrecruiters"):
        board = summarize_board(ats_footprint(ats["type"], ats["slug"]))
        if board:
            board["type"], board["slug"] = ats["type"], ats["slug"]
            src = {"greenhouse": f"https://boards.greenhouse.io/{ats['slug']}", "ashby": f"https://jobs.ashbyhq.com/{ats['slug']}",
                   "lever": f"https://jobs.lever.co/{ats['slug']}", "smartrecruiters": f"https://jobs.smartrecruiters.com/{ats['slug']}"}[ats["type"]]
            board["url"] = src
            if board["india"]:
                add("board", f"{board['india']} of {board['total']} open roles {'is' if board['india'] == 1 else 'are'} located in India", "; ".join(f"{x['title']} ({x['location']})" for x in board["india_samples"][:3]), src, 5)
            if board["apac"] + board["worldwide"]:
                add("board", f"{board['apac'] + board['worldwide']} open role{'' if board['apac'] + board['worldwide'] == 1 else 's'} {'is' if board['apac'] + board['worldwide'] == 1 else 'are'} open worldwide or to APAC", "; ".join(f"{x['title']} ({x['location']})" for x in board["open_samples"][:3]), src, 3)
            if not board["india"] and not board["apac"] and not board["worldwide"] and board["total"] >= 3:
                cs = [c for c in board["countries"] if c != "India"]
                if cs:
                    add("board", f"All {board['total']} open roles are in {_list_names(cs)}", "None are located in India or open worldwide" + (f"; {board['restricted_remote']} remote role{'' if board['restricted_remote'] == 1 else 's'} {'is' if board['restricted_remote'] == 1 else 'are'} limited to named countries" if board["restricted_remote"] else ""), src, -3)
    # current qualifying postings at this company
    conf = [j for j in jobs if j["india"]["status"] == "confirmed"]
    world = [j for j in jobs if j["india"]["status"] == "likely" and j["india"].get("basis") != "company"]
    if conf:
        add("posting", "A current posting lists India as a location", f"{conf[0]['title']} — {conf[0]['location'] or conf[0]['india']['reason']}", conf[0]["url"], 5)
    if world:
        add("posting", "A current posting is open worldwide", f"{world[0]['title']} — {world[0]['location'] or 'Remote'}", world[0]["url"], 3)
    seen_g = seen_o = False
    for j in jobs:
        t = descs.get(j["id"]) or ""
        m = GLOBAL_HIRING.search(t)
        if m and not seen_g:
            s = max(0, m.start() - 70); add("posting", "Posting describes hiring across countries", "…" + t[s:m.end() + 70].replace("\n", " ").strip() + "…", j["url"], 3); seen_g = True
        m = ONLY_HIRE.search(t)
        if m and not seen_o and not INDIA.search(m.group(0)):
            add("posting", "Posting limits where candidates can live", m.group(0).strip(), j["url"], -2); seen_o = True
        p = INDIA_PRESENCE.search(t)
        if p and not any(e["kind"] == "posting" and "India" in e["label"] for e in ev):
            add("posting", "Posting mentions an India team or office", "…" + p.group(0) + "…", j["url"], 4)
    # public web (monitor searches) and checks run from the app
    for kind, h, quote, w in classify_web_hits(web_hits, name)[:6]:
        if kind == "india": add("web", "Web mentions India hiring or an India office", f"{h.get('title', '')[:90]} — “{quote[:120]}”", h.get("url"), w)
        elif kind == "global": add("web", "Web shows it hires across countries", f"{h.get('title', '')[:90]} — “{quote[:120]}”", h.get("url"), w)
        else: add("web", "Web suggests hiring is limited to named countries", f"{h.get('title', '')[:90]} — “{quote[:120]}”", h.get("url"), w)
    if app_check and app_check.get("verdict") in ("yes", "likely", "no"):
        w = {"yes": 4, "likely": 2, "no": -2}[app_check["verdict"]]
        for e in (app_check.get("evidence") or [])[:4]:
            add("app", "Web check in the app", (e.get("quote") or e.get("title") or "")[:200], e.get("url"), w)
        if not app_check.get("evidence"):
            add("app", "Web check in the app", app_check.get("summary", "")[:200], None, w)
    # verdict
    top = max([e["weight"] for e in ev], default=0)
    neg = min([e["weight"] for e in ev], default=0)
    if top >= 4:
        verdict = "yes"
    elif top >= 2:
        verdict = "likely"
    elif neg <= -2:
        verdict = "no"
    else:
        verdict = "unclear"
    order = {"board": 0, "posting": 1, "registry": 2, "app": 3, "web": 4}
    ev_sorted = sorted(ev, key=lambda e: order.get(e["kind"], 9))
    if india_hq: summary = f"{name} is headquartered in India."
    elif verdict == "yes":
        b = next((e for e in ev_sorted if e["weight"] >= 4), None); summary = f"{b['label']}." if b else "Evidence shows it hires people in India."
    elif verdict == "likely":
        b = next((e for e in ev_sorted if e["weight"] >= 2), None); summary = f"No India-located roles found, but {b['label'][0].lower() + b['label'][1:]}." if b else "Hires across countries."
    elif verdict == "no":
        b = next((e for e in ev_sorted if e["weight"] <= -2), None); summary = f"{b['label']}. {b['detail']}." if b and b["kind"] == "board" else (f"{b['label']}: “{b['detail'][:90]}”" if b else "Hires only in named countries.")
    else:
        summary = "No public evidence yet about hiring from India." if not board else f"Its careers board shows {board['total']} open roles without a clear location pattern."
    ev.sort(key=lambda e: (-abs(e["weight"]), e["kind"], e["label"]))
    res = dict(verdict=verdict, summary=summary.replace("..", "."), evidence=ev[:8], board=board, india_based=india_hq,
               method="Careers board, posting text" + (", web search" if web_hits else "") + (", app check" if app_check else ""))
    # keep timestamps stable unless the result changed
    sig = json.dumps([res["verdict"], res["summary"], [(e["label"], e["url"]) for e in res["evidence"]], (board or {}).get("total"), (board or {}).get("india")], sort_keys=True)
    res["sig"] = hashlib.sha1(sig.encode()).hexdigest()[:10]
    if prev and prev.get("sig") == res["sig"]:
        res["changed_at"] = prev.get("changed_at") or iso(NOW)
    else:
        res["changed_at"] = iso(NOW)
    res["first_checked_at"] = (prev or {}).get("first_checked_at") or iso(NOW)
    return res


INDIA_LABEL = {"yes": "hires in India", "likely": "hires across countries", "no": "hires only in named countries", "unclear": "not yet clear"}


def apply_india_checks(docs, comps, descs, existing, web, companies_reg):
    """Run the company check for every employer with a qualifying role, then use it to clarify roles that do not say where they hire.
    Employers whose roles were not seen this run (search leads kept from earlier runs) are checked too; their role records are left as they are."""
    reg = {c["slug"]: c for c in companies_reg}
    ex_comp = existing.get("companies", {})
    app_checks = existing.get("india_checks", {})
    by = {}
    for d in docs: by.setdefault(d["company_slug"], []).append(d)
    verify = ((existing.get("meta", {}).get("verify") or {}).get("map")) or {}
    seen = {d["id"] for d in docs}
    ex_text = existing.get("job_text", {})
    for jid, j in existing.get("jobs", {}).items():
        if jid in seen or not isinstance(j, dict) or not j.get("company_slug") or not j.get("india"): continue
        if ((verify.get(jid) or {}).get("s") or j.get("status")) in ("closed", "stale", "excluded"): continue
        slug = j["company_slug"]
        if slug not in comps:
            base = dict(ex_comp.get(slug) or dict(name=j.get("company"), slug=slug, tier=j.get("tier"), category=j.get("category"), inferred=True))
            base.pop("india_hiring", None); comps[slug] = base
        by.setdefault(slug, []).append(j)
        if jid not in descs and isinstance(ex_text.get(jid), dict): descs[jid] = ex_text[jid].get("text") or ""
    checks = {}
    def one(slug):
        c = dict(reg.get(slug) or {}); c.update({k: v for k, v in (comps.get(slug) or {}).items() if v is not None})
        prev = (ex_comp.get(slug) or {}).get("india_hiring")
        w = (web or {}).get(slug)
        hits = (w or {}).get("results")
        web_rec = dict(checked_at=iso(NOW), query=w.get("query"), results=hits[:6]) if w else (prev or {}).get("web")
        res = india_hiring_check(c.get("name") or slug, c, by[slug], descs, (web_rec or {}).get("results"), app_checks.get(slug), prev)
        if web_rec: res["web"] = web_rec
        return slug, res
    with cf.ThreadPoolExecutor(8) as ex:
        for slug, res in ex.map(one, list(by)):
            checks[slug] = res
    for slug, res in checks.items():
        if slug in comps: comps[slug]["india_hiring"] = res
    changed = 0
    for d in docs:
        res = checks.get(d["company_slug"])
        if not res: continue
        ind = d["india"]; v = res["verdict"]
        ind["company"] = v
        before = ind["status"]
        if ind["status"] == "unconfirmed" and d["work_mode"] == "remote" and ind["reason"] == "Remote, region not stated" and v in ("yes", "likely"):
            ind.update(status="likely", basis="company", reason=f"Region not stated; {d['company']} {INDIA_LABEL[v]}")
        elif ind["status"] == "unconfirmed" and ind["reason"] == "Location not stated" and res.get("india_based"):
            ind.update(status="likely", basis="company", reason=f"Location not stated; {d['company']} is based in India")
        elif ind["status"] == "unconfirmed" and v in ("yes", "likely") and ind["reason"].startswith(("Description mentions", "Says remote")):
            ind["reason"] = ind["reason"] + f" · {d['company']} {INDIA_LABEL[v]}"
        elif ind["status"] == "unconfirmed" and v == "no":
            ind["reason"] = ind["reason"] + f" · {d['company']} {INDIA_LABEL['no']}"
        if ind["status"] != before: changed += 1
    return checks, changed

# ---------------------------------------------------------------- scoring
TIER_SCORE = {1: 95, 2: 76, 3: 66}
LOC_SCORE = {"confirmed": 100, "likely": 86, "relocation": 30, "unconfirmed": 36}
KIND_SCORE = {"game-tester": 100, "slot-qa": 100, "game-qa-engineer": 92, "game-cert": 78, "game-lqa": 60, "game-automation": 52,
              "manual-qa": 100, "mobile-qa": 96, "qa-engineer": 86, "crowd": 72, "automation": 48}

def comp_bucket(rec):
    s = rec["salary"]
    if s["state"] == "verified" and s.get("inr_max"):
        mx = s["inr_max"]
        if s.get("currency") != "INR": return "listed_global", mx
        return ("listed_good" if mx >= 450_000 else "listed"), mx
    if s["state"] == "estimate": return "estimate", None
    return "unknown", None

def comp_score(bucket, inr):
    if bucket == "listed_global": return 96
    if bucket in ("listed_good", "listed"):
        return 100 if inr >= 700_000 else 90 if inr >= 500_000 else 78 if inr >= 350_000 else 58
    return {"estimate": 62, "unknown": 60}.get(bucket, 60)

CANDIDATE_YEARS = 2.5
def years_max_of(desc):
    m = re.search(r"(\d{1,2})\s*\+?\s*(?:-|–|to)\s*(\d{1,2})\s*\+?\s*(?:years?|yrs?)", (desc or "").lower())
    return int(m.group(2)) if m and int(m.group(2)) >= int(m.group(1)) else None

def exp_score(sen, yrs, ymax=None):
    if yrs is not None and ymax is not None and yrs <= CANDIDATE_YEARS <= ymax:
        return 100
    if yrs is not None:
        return {0: 80, 1: 90, 2: 100, 3: 94, 4: 72}.get(yrs, 40)
    return {"junior": 86, "mid": 92, "unspecified": 84, "senior": 30}.get(sen, 80)

def loc_points(india, work_mode, cities, loc_text=""):
    if india == "confirmed":
        if work_mode == "remote": return 100
        if "Mumbai" in cities or MUMBAI.search(loc_text or ""): return 100
        if "Pune" in cities: return 84 if work_mode == "hybrid" else 78
        if cities: return 66 if work_mode == "hybrid" else 60
        return 80
    return LOC_SCORE.get(india, 30)

def score(rec, company):
    text = (rec["title"] + "\n" + rec["desc"]).lower()
    sig = set(rec["tools"]) | set(rec["specializations"])
    cand = sig & (CANDIDATE_TOOLS | CANDIDATE_SPECS); gaps = sig & GAP_TOOLS
    match = len(cand) / max(4, len(cand) + len(gaps))
    base = KIND_SCORE.get(rec["kind"], 70)
    skill = 0.5 * base + 0.5 * min(1, match * 1.15) * 100
    if "Test automation" in gaps and rec["kind"] not in ("automation", "game-automation"): skill -= 5
    if not rec["desc"]: skill = 0.86 * base
    tier = company.get("tier", 3)
    comp_s = TIER_SCORE.get(tier, 56)
    if LOWQ.search(text) or LOWQ_CO.search(rec.get("company") or ""): comp_s = min(comp_s, 40)
    if rec.get("track") == "game" and re.search(r"(?i)game|gaming|igaming|casino", company.get("category") or ""): comp_s = max(comp_s, 96)
    sen = exp_score(rec["seniority"], rec.get("years_min"), rec.get("years_max"))
    dom = 58
    if rec["desc"]:
        if {"Android", "iOS", "Mobile apps"} & sig: dom += 16
        if "Windows / PC" in sig or "Steam / PC games" in sig: dom += 8
        if rec.get("track") == "game":
            if "Slot / casino games" in sig: dom += 16
            if "Gameplay testing" in sig: dom += 8
            if "Console" in sig and not ({"Android", "iOS", "Mobile apps", "Windows / PC"} & sig): dom -= 14
        else:
            if "Web apps" in sig: dom += 4
            if {"Exploratory testing", "Compatibility testing"} & sig: dom += 6
    else:
        dom = 55
    bucket, inr = comp_bucket(rec)
    cs = comp_score(bucket, inr)
    india = rec["india"]["status"]
    loc = loc_points(india, rec["work_mode"], rec["india"].get("cities") or [], rec.get("location") or rec.get("location_raw") or "")
    sub = dict(skill=round(max(0, min(100, skill))), company=round(comp_s), seniority=round(sen), ai=round(max(0, min(100, dom))), comp=round(cs), location=round(loc))
    rec["comp_potential"] = bucket
    return sub

DEFAULT_WEIGHTS = dict(skill=30, company=15, seniority=20, ai=10, comp=10, location=15)
def fit(sub, w=DEFAULT_WEIGHTS):
    tot = sum(w.values())
    return round(sum(sub[k] * w[k] for k in w) / tot)

def explain(rec, company):
    why, concerns = [], []
    k = rec["kind"]; tr = rec.get("track")
    label = {"game-tester": "Game tester role — matches your game QA work at Webmobril and Golden Horde",
             "slot-qa": "Slot / casino game QA — you have tested slot mechanics, betting, payouts and bonus features (Savannah)",
             "game-qa-engineer": "Game QA role at engineer/analyst level", "game-cert": "Game certification / compliance testing",
             "manual-qa": "Manual / functional QA — the core of your resume", "mobile-qa": "Mobile app testing on Android and iOS, like your current work",
             "qa-engineer": "QA / test engineer role", "crowd": "Freelance / crowd testing — paid per test cycle, remote"}.get(k)
    if label: why.append(label)
    tl = [t for t in rec["tools"] if t in CANDIDATE_TOOLS][:6]
    if tl: why.append("Already on your resume: " + ", ".join(tl))
    pl = [p for p in rec["specializations"] if p in CANDIDATE_SPECS]
    if pl: why.append("Platforms you test on: " + ", ".join(pl[:4]))
    if company.get("tier") == 1: why.append(f"{company.get('category', 'Product')} company")
    ymn, ymx = rec.get("years_min"), rec.get("years_max")
    if ymn is not None and ymx is not None and ymn <= 2.5 <= ymx: why.append(f"Asks for {ymn}–{ymx} years — you have about 2.5")
    elif ymn is not None and ymn <= 3: why.append(f"Asks for {ymn}+ years — you have about 2.5")
    if rec["salary"]["state"] == "verified": why.append("Pay is listed in the posting")
    if rec["india"]["status"] in ("confirmed", "likely"): why.append(rec["india"]["reason"])
    gaps = [t for t in rec["tools"] if t in GAP_TOOLS]
    if k in ("automation", "game-automation"): concerns.append("Automation-heavy role (Selenium/Appium-type scripting) — not on your resume yet")
    elif "Test automation" in gaps: concerns.append("Mentions test automation — be ready to discuss it or show a small script")
    other = [g for g in gaps if g != "Test automation"]
    if other: concerns.append("Also asks for: " + ", ".join(other[:3]))
    if rec.get("years_min") is not None and rec["years_min"] >= 4: concerns.append(f"Asks for {rec['years_min']}+ years — you have about 2.5")
    if (rec.get("years_min") == 0 and (rec.get("years_max") or 0) <= 2) or rec["seniority"] == "junior": concerns.append("Entry-level role — pay may be at the lower end")
    if LOWQ.search(rec["title"] + "\n" + (rec.get("desc") or "")) or LOWQ_CO.search(rec.get("company") or ""): concerns.insert(0, "Generic work-from-home listing from an unverified poster — check the employer independently, and never pay a fee to apply")
    if rec["india"]["status"] == "unconfirmed": concerns.append("Employer has not confirmed hiring from India")
    if rec["india"]["status"] == "relocation": concerns.append("Requires relocation abroad")
    if rec["india"]["status"] == "confirmed" and rec["work_mode"] != "remote" and not ("Mumbai" in (rec["india"].get("cities") or []) or MUMBAI.search(rec.get("location") or "")):
        concerns.append("Office outside Mumbai — relocation within India")
    if rec["salary"]["state"] == "unavailable": concerns.append("Pay not disclosed — ask for the range early")
    elif rec["salary"]["state"] == "verified" and rec["salary"].get("applies_india") is False: concerns.append("Listed range is for another country")
    if company.get("tier") == 2: concerns.append(f"{company.get('category') or 'Services'} company (client projects)")
    if rec["employment"] in ("contract", "freelance", "part-time"): concerns.append(f"{rec['employment'].capitalize()} role")
    if k == "game-lqa": concerns.append("Localization QA usually needs fluency in a specific language")
    if not rec["desc"]: concerns.append("Full description not available from this source")
    if rec.get("repost"): concerns.append("Reposted / refreshed listing — original posting is older")
    return why[:6], concerns[:6]

# ---------------------------------------------------------------- main assembly
def extract_bullets(desc, heads):
    lines = [l.strip(" •-*\t") for l in desc.split("\n")]
    out, grab = [], False
    for l in lines:
        if not l: continue
        low = l.lower()
        if any(h in low for h in heads) and len(l) < 90:
            grab = True; continue
        if grab:
            if len(l) < 70 and l.endswith(":") or (len(l) < 50 and re.match(r"^[A-Z][A-Za-z ,&'/]+$", l) and not l.endswith(".")):
                if out: break
                continue
            if 18 < len(l) < 400: out.append(l)
            if len(out) >= 8: break
    return out

def build(raw, companies, fx, benchmarks, existing):
    excluded = []
    recs = []
    comparables = []
    for r in raw:
        if not r["title"] or not r.get("company") or len(r["company"]) > 60 or re.search(r"https?://|www\.", r["company"]):
            excluded.append((r, "No identifiable employer")); continue
        pd_ = r["posted_at"]
        if pd_ and (NOW - pd_).days > WINDOW_DAYS:
            excluded.append((r, "Older than window")); continue
        if r.get("staffing"):
            excluded.append((r, "Staffing agency listing — employer not identified")); continue
        if r.get("level") and re.search(r"(?i)intern|trainee", r["level"]):
            excluded.append((r, "Internship / trainee")); continue
        if r.get("level") and re.search(r"(?i)mid-senior|senior|director|executive", r["level"]) and not re.search(r"(?i)associate|entry", r["level"]):
            excluded.append((r, "Senior / lead level")); continue
        inc, kind, track, reason = classify_title(r["title"], r["desc"], r.get("company") or "")
        if inc is False:
            excluded.append((r, reason)); continue
        sen, yrs = seniority_of(r["title"], r["desc"])
        if yrs is not None and yrs >= 5:
            excluded.append((r, f"Asks for {yrs}+ years")); continue
        sr_ = r.get("salary_raw")
        if isinstance(sr_, str): sr_ = parse_salary_text(sr_)
        if sr_ and sr_.get("max") and sr_.get("currency") != "INR" and (r["remote"] or (r.get("workplace") or "").lower() == "remote") and inc and fx \
                and not CONTRACT.search(r["title"] + " " + (r["employment"] or "")):
            lo_a, hi_a = annualize(sr_["min"], sr_["period"]), annualize(sr_["max"], sr_["period"])
            if hi_a > 6000:
                comparables.append(dict(mid=to_inr((lo_a + hi_a) / 2, sr_["currency"], fx), title=r["title"], company=r["company"], url=r["url"], currency=sr_["currency"], min=lo_a, max=hi_a, location=r["location_raw"]))
        wp = (r.get("workplace") or "").lower()
        work_mode = "remote" if (r["remote"] or wp == "remote") else ("hybrid" if ("hybrid" in wp or re.search(r"(?i)hybrid", r["location_raw"])) else "onsite")
        if work_mode == "onsite" and not r["location_raw"].strip() and not r.get("workplace"):
            work_mode = "unspecified"
        india = india_eligibility(r["location_raw"], r["restrictions"], r["desc"], work_mode == "remote")
        if india["status"] == "excluded":
            excluded.append((r, "Not open to India: " + india["reason"])); continue
        r.update(kind=kind, track=track, include_reason=reason, needs_review=inc is None, seniority=sen, years_min=yrs, years_max=years_max_of(r["desc"]), work_mode=work_mode, india=india)
        emp = (r["employment"] or "").lower()
        r["employment"] = "contract" if re.search(r"contract|temporary|fixed", emp) or re.search(r"(?i)\b(contract|ftc|fixed[- ]term|\d+[- ]month)\b", r["title"]) else ("freelance" if "freelance" in emp else ("part-time" if "part" in emp else "full-time"))
        text = r["title"] + "\n" + r["desc"]
        r["tools"] = [k for k in find_tags(text, QA_SIGNALS) if k not in PLATFORMS]
        r["specializations"] = find_tags(text, SPECIALIZATIONS)
        recs.append(r)

    # ---- dedupe & merge
    def tkey(t):
        t = norm(re.sub(r"(?i)\(.*?\)|- *(remote|contract|12 month.*)$", " ", t))
        t = re.sub(r"\b(sr|snr)\b", "senior", t)
        t = re.sub(r"\b(graphics|graphic)\b", "graphic", t)
        return re.sub(r"\s+", " ", t).strip()
    def toks(t):
        return set(norm(re.sub(r"\b(sr|snr)\b", "senior", t.lower())).split()) - {"and", "the", "a", "of", "remote", "contract", "full", "time"}
    groups = {}
    by_company = {}
    for r in recs:
        ck = company_key(r["company"])
        placed = False
        for key in by_company.get(ck, []):
            a, b = toks(r["title"]), toks(groups[key][0]["title"])
            if a and b and len(a & b) / len(a | b) >= 0.6:
                groups[key].append(r); placed = True; break
        if not placed:
            key = ck + "|" + tkey(r["title"])
            groups.setdefault(key, []).append(r)
            by_company.setdefault(ck, []).append(key)
    jobs = []
    for key, items in groups.items():
        items.sort(key=lambda x: (0 if x["official"] else 1, 0 if x["desc"] else 1, -(x["posted_at"] or NOW).timestamp()))
        p = dict(items[0])
        srcs = []
        for it in items:
            srcs.append(dict(id=it["source_id"], name=it["source_name"], kind=it["source_kind"], url=it["url"], job_id=it["source_job_id"],
                             posted_at=iso(it["posted_at"]), seen_at=iso(NOW)))
            if not p["desc"] and it["desc"]: p["desc"] = it["desc"]
            if not p.get("salary_raw") and it.get("salary_raw"): p["salary_raw"] = it["salary_raw"]
            if not p.get("logo") and it.get("logo"): p["logo"] = it["logo"]
            if not p.get("contact") and it.get("contact"): p["contact"] = it["contact"]
            if not p.get("deadline") and it.get("deadline"): p["deadline"] = it["deadline"]
        # unique sources by url
        seen = set(); uniq = []
        for s in srcs:
            if s["url"] in seen: continue
            seen.add(s["url"]); uniq.append(s)
        p["sources"] = uniq
        dates = [it["posted_at"] for it in items if it["posted_at"]]
        p["posted_at"] = min(dates) if dates else None
        p["updated_latest"] = max([it.get("updated_at") or it["posted_at"] for it in items if (it.get("updated_at") or it["posted_at"])], default=None)
        jobs.append(p)

    # ---- company resolution + ATS corroboration for board-only companies
    reg = {company_key(c["name"]): c for c in companies}
    for c in companies:
        for a in c.get("aliases", []):
            reg[company_key(a)] = c
    corroborate = [j for j in jobs if not j["official"]]
    probes = {}
    with cf.ThreadPoolExecutor(8) as ex:
        futs = {}
        for j in corroborate:
            ck = company_key(j["company"])
            if ck in probes or ck in futs.values(): continue
            c = reg.get(ck)
            if c and c.get("ats"):
                probes[ck] = c["ats"]
                continue
            futs[ex.submit(ats_probe_company, j["company"])] = ck
        for f in cf.as_completed(futs):
            try: probes[futs[f]] = f.result()
            except Exception: probes[futs[f]] = None
    ats_cache = {}
    for j in corroborate:
        ck = company_key(j["company"])
        a = probes.get(ck)
        if not a: continue
        k2 = (a["type"], a["slug"])
        if k2 not in ats_cache:
            try: ats_cache[k2] = ATS_FN[a["type"]](a["slug"], j["company"])
            except Exception as e: ats_cache[k2] = []; log("corroborate", k2, e)
        best = None
        for o in ats_cache[k2]:
            if tkey(o["title"]) == tkey(j["title"]) or (set(tkey(o["title"]).split()) >= set(tkey(j["title"]).split()) and len(tkey(j["title"]).split()) >= 2):
                best = o; break
        j["ats_board"] = a
        if best:
            j["official"] = True
            j["apply_url"] = best["apply_url"]
            j["sources"].insert(0, dict(id=best["source_id"], name=best["source_name"], kind="ats", url=best["url"], job_id=best["source_job_id"], posted_at=iso(best["posted_at"]), seen_at=iso(NOW)))
            if best["desc"] and len(best["desc"]) > len(j["desc"]): j["desc"] = best["desc"]
            if best.get("salary_raw") and not j.get("salary_raw"): j["salary_raw"] = best["salary_raw"]
            if best["posted_at"] and (not j["posted_at"] or best["posted_at"] < j["posted_at"]): j["posted_at"] = best["posted_at"]

    # ---- finalize docs
    ex_jobs = existing.get("jobs", {})
    docs, texts = [], []
    company_docs = {}
    for j in jobs:
        ck = company_key(j["company"])
        c = reg.get(ck)
        if not c:
            c = dict(name=j["company"], slug=slugify(j["company"]), tier=None, category=None, inferred=True)
        c = dict(c)
        if c.get("tier") is None:
            d = (j["desc"] or "").lower(); nm = j["company"].lower()
            if j.get("recruiter") or re.search(r"(?i)^hiring for|confidential|stealth|client of|leading (mnc|company)|reputed", nm):
                c.update(tier=3, category="Recruiter (client not named)")
            elif re.search(r"(?i)testlio|utest|applause|test ?io|global app testing|rainforest|crowd ?test|testbirds", nm + " " + d[:600]):
                c.update(tier=2, category="Crowd testing platform")
            elif re.search(r"casino|igaming|i-gaming|sportsbook|real[- ]money|rummy|poker|betting|slot games?", d[:3000]):
                c.update(tier=1, category="iGaming / real-money games")
            elif re.search(r"\b(game studio|games studio|video games?|mobile games?|game development|our games|gaming company|game publisher)\b", d[:3000]) or re.search(r"(?i)\b(games?|gaming|studios?)\b", nm):
                c.update(tier=1, category="Game studio")
            elif re.search(r"\b(testing services|qa services|software testing company|quality engineering services|qa outsourcing|testing partner)\b", d[:3000]):
                c.update(tier=2, category="QA services")
            elif re.search(r"\b(it services|it consulting|staffing|our clients|client projects|service[- ]based|outsourc\w+|consulting firm|recruitment agency)\b", d[:3000]):
                c.update(tier=2, category="IT services")
            elif re.search(r"\b(fintech|payments|lending|banking|insurance|wealth|trading|stock broking)\b", d[:3000]):
                c.update(tier=1, category="Fintech")
            elif re.search(r"\b(saas|b2b|platform|our product|software product|startup|series [a-e]|app\b|mobile app|e-?commerce|edtech|healthtech)\b", d[:3000]):
                c.update(tier=1, category="Product / SaaS")
            else:
                c.update(tier=3, category="Other")
            c["classification"] = "Inferred from job description"
        jid = hid(ck, tkey(j["title"]))
        prev = ex_jobs.get(jid)
        # salary
        sal = dict(state="unavailable")
        sr = j.get("salary_raw")
        if isinstance(sr, str): sr = parse_salary_text(sr)
        if sr and sr.get("max"):
            lo_a, hi_a = annualize(sr["min"], sr["period"]), annualize(sr["max"], sr["period"])
            loc_low = j["location_raw"].lower()
            applies = True
            note = sr.get("note", "")
            if sr["currency"] != "INR" and j["india"]["status"] != "likely" and re.search(r"(?i)\b(us|usa|united states|ca|ny|sf|san francisco|new york|seattle|london|uk|canada|europe)\b", loc_low + " " + note.lower()):
                applies = False
            if j["india"]["status"] == "confirmed" and sr["currency"] != "INR":
                applies = False
            if applies is True and sr["currency"] != "INR" and j["india"]["status"] in ("likely", "unconfirmed"):
                applies = "unconfirmed"
            sal = dict(state="verified", currency=sr["currency"], min=round(lo_a), max=round(hi_a), period="annual", basis="base" if j["official"] else "listed",
                       inr_min=round(to_inr(lo_a, sr["currency"], fx)) if fx and to_inr(lo_a, sr["currency"], fx) else None,
                       inr_max=round(to_inr(hi_a, sr["currency"], fx)) if fx and to_inr(hi_a, sr["currency"], fx) else None,
                       source_url=j["sources"][0]["url"], source_name=j["sources"][0]["name"], verified_at=iso(NOW),
                       fx_rate=round(fx["rates"]["INR"] / fx["rates"].get(sr["currency"], 1), 4) if fx else None, fx_at=fx["as_of"] if fx else None,
                       applies_india=applies, note=(note or ("Range listed for a non-India location" if applies is False else ("Listed for a worldwide-remote role; confirm it applies to India-based hires" if applies == "unconfirmed" else "Listed in the posting"))),
                       original_period=sr["period"])
        j["salary"] = sal
        rec = dict(
            id=jid, title=j["title"], company=j["company"], company_slug=c.get("slug") or slugify(j["company"]), logo=j.get("logo"),
            tier=c.get("tier", 3), category=c.get("category") or "Other",
            url=j["sources"][0]["url"], apply_url=j["apply_url"] if j["official"] else None, board_apply=None if j["official"] else (j.get("board_apply") or j["sources"][0]["url"]), official=j["official"],
            source_primary=j["sources"][0]["name"], sources=j["sources"],
            status="verified" if j["official"] else ("lead" if j["source_kind"] in ("community", "search", "social") else "active"),
            posted_at=iso(j["posted_at"]), updated_at=iso(j.get("updated_latest")) if j.get("updated_latest") and j["posted_at"] and (j["updated_latest"] - j["posted_at"]).days >= 3 else None,
            date_quality=("approx" if (j.get("date_note") or j["source_id"] in ("dribbble",)) else "exact") if j["posted_at"] else "unknown",
            first_seen=(prev or {}).get("first_seen") or iso(NOW), last_verified=iso(NOW),
            location=j["location_raw"] or ("Remote" if j["work_mode"] == "remote" else ""), work_mode=j["work_mode"], india=j["india"],
            employment=j["employment"], seniority=j["seniority"], years_min=j.get("years_min"), years_max=j.get("years_max"), kind=j["kind"], include_reason=j["include_reason"],
            needs_review=j["needs_review"], tools=j["tools"], specializations=j["specializations"], track=j["track"],
            snippet=(j["desc"] or "")[:420], responsibilities=extract_bullets(j["desc"], ["what you", "responsibilit", "you will", "you'll", "the role", "role overview", "in this role"])[:7],
            requirements=extract_bullets(j["desc"], ["about you", "requirement", "qualification", "you have", "you bring", "what we're looking", "we're looking for", "must have", "skills"])[:7],
            salary=sal, deadline=j.get("deadline"),
        )
        rec["repost"] = bool(rec["updated_at"]) or bool(prev and prev.get("posted_at") and rec["posted_at"] and rec["posted_at"] > prev["posted_at"])
        sub = score(rec | {"desc": j["desc"]}, c)
        rec["comp_potential"] = comp_bucket(rec)[0]
        rec["scores"] = sub
        rec["fit"] = fit(sub)
        rec["why"], rec["concerns"] = explain(rec | {"desc": j["desc"]}, c)
        rec["content_hash"] = hashlib.sha1(json.dumps([rec["title"], rec["sources"][0]["url"], rec["salary"].get("max"), rec["location"], (j["desc"] or "")[:2000]]).encode()).hexdigest()[:12]
        docs.append(rec)
        texts.append(dict(id=jid, text=(j["desc"] or "")[:14000], fetched_at=iso(NOW), source_url=rec["url"]))
        cd = company_docs.setdefault(rec["company_slug"], dict(c))
        cd.setdefault("name", j["company"]); cd.setdefault("slug", rec["company_slug"])
        if j.get("logo") and not cd.get("logo"): cd["logo"] = j["logo"]
        if j.get("ats_board") and not cd.get("ats"): cd["ats"] = j["ats_board"]
        if j.get("company_site") and not cd.get("domain"):
            host = urllib.parse.urlparse(j["company_site"]).hostname or ""
            host = re.sub(r"^(www|careers|jobs|career|apply)\.", "", host)
            if host and "." in host and not re.search(r"workable|linkedin|greenhouse|lever|ashby", host): cd["domain"] = host
        cd.setdefault("tracks", [])
        if rec["track"] not in cd["tracks"]: cd["tracks"].append(rec["track"])
        cd.setdefault("job_ids", []).append(jid)
    descs = {t["id"]: t["text"] for t in texts}
    comp_lookup = {k: v for k, v in company_docs.items()}
    bms = apply_estimates(docs, descs, comp_lookup, benchmarks, fx, comparables)
    raw_contacts = []
    for j in jobs:
        for it in [j]:
            pass
    for d, j in zip(docs, jobs):
        if j.get("contact"):
            c = dict(j["contact"]); c.update(company=d["company"], company_slug=d["company_slug"], job_ids=[d["id"]])
            raw_contacts.append(c)
    contacts = build_contacts(docs, company_docs, raw_contacts)
    for d in docs:
        cs = [c for c in contacts if d["id"] in c["job_ids"] or (c["company_slug"] == d["company_slug"])]
        d["contacts_count"] = len(cs)
        d["has_named_contact"] = any(c["named"] for c in cs)
        d["has_public_email"] = any(c.get("email") for c in cs)
        d["has_hiring_manager"] = any(c["category"] == "hiring_team" for c in cs)
    return docs, texts, company_docs, excluded, contacts, bms

def apply_estimates(docs, descs, comp_lookup, benchmarks, fx, comparables=()):
    """QA pay varies too much by employer to estimate per role. Builds one context benchmark from pay disclosed
    on remote QA postings in this dataset; roles without pay stay 'not disclosed'."""
    bm = {b["id"]: b for b in benchmarks}
    seen_c, vals = set(), []
    for c in comparables:
        k = (company_key(c["company"]), norm(c["title"]))
        if k in seen_c or not c.get("mid"): continue
        seen_c.add(k); vals.append(c)
    if len(vals) >= 4:
        v = sorted(x["mid"] for x in vals)
        q = lambda p_: v[int(round(p_ * (len(v) - 1)))]
        ds = dict(id="qa-dataset-remote", label="Disclosed pay on remote QA postings (any region, this dataset)", market="Remote (global)",
                  currency="INR", avg=round(q(0.5)), low=round(q(0.25)), high=round(q(0.75)), n=len(v), basis="Interquartile range of posting midpoints, converted to INR at today's rate",
                  source_name="QA Intelligence dataset", source_url=None, retrieved_at=iso(NOW)[:10],
                  samples=[dict(title=x["title"], company=x["company"], url=x["url"], currency=x["currency"], min=round(x["min"]), max=round(x["max"]), location=x["location"]) for x in sorted(vals, key=lambda x: x["mid"])][:40])
        bm[ds["id"]] = ds
        for d in docs:
            if d["salary"]["state"] == "unavailable" and d["work_mode"] == "remote" and d["india"]["status"] in ("likely", "unconfirmed"):
                d["salary"]["context_benchmark"] = ds["id"]
    return [b for b in bm.values()]

def build_contacts(docs, comp_docs, raw_contacts):
    out = {}
    for c in raw_contacts:
        cid = hid(company_key(c["company"]), norm(c["name"]))
        cur = out.setdefault(cid, dict(id=cid, name=c["name"], title=c.get("title", ""), company=c["company"], company_slug=c["company_slug"],
            category="channel" if c.get("channel") else ("hiring_team" if re.search(r"(?i)hiring|head|lead|manager|director", c.get("relevance", "") + c.get("title", "")) else "talent"),
            linkedin=c.get("linkedin"), other_url=None, email=None, phone=None, relevance=c.get("relevance", ""), evidence_url=c.get("evidence"),
            evidence_label="Public LinkedIn post announcing the opening", verified_at=iso(NOW), confidence="medium", job_ids=[], named=not c.get("channel")))
        for j in c.get("job_ids", []):
            if j not in cur["job_ids"]: cur["job_ids"].append(j)
    for slug, cd in comp_docs.items():
        if cd.get("careers"):
            cid = hid(slug, "careers")
            out[cid] = dict(id=cid, name=f"{cd['name']} careers page", title="Official application channel", company=cd["name"], company_slug=slug, category="channel",
                linkedin=None, other_url=cd["careers"], email=None, phone=None, relevance="Official job board — authoritative for open roles", evidence_url=cd["careers"],
                evidence_label="Employer applicant-tracking board", verified_at=iso(NOW), confidence="high", job_ids=list(cd.get("job_ids", [])), named=False)
    return list(out.values())

def run_connectors(companies, watchlist):
    raw, status = [], {}
    simple = [("shine", c_shine), ("workable", c_workable), ("cutshort", c_cutshort), ("himalayas", c_himalayas), ("remotive", c_remotive), ("remoteok", c_remoteok),
              ("workingnomads", c_workingnomads), ("jobicy", c_jobicy), ("hn", c_hn)]
    def runone(item):
        sid, fn = item
        t0 = time.time()
        try:
            res = fn()
            return sid, res, dict(ok=True, fetched=len(res), ms=int((time.time() - t0) * 1000))
        except Exception as e:
            return sid, [], dict(ok=False, error=str(e)[:200], ms=int((time.time() - t0) * 1000))
    with cf.ThreadPoolExecutor(6) as ex:
        for sid, res, st in ex.map(runone, simple):
            raw += res; status[sid] = st; log(sid, st)
    # watchlist entries without a known board: look one up by company name
    for c in watchlist:
        if c.get("name") and not c.get("ats"):
            try: c["ats"] = ats_probe_company(c["name"])
            except Exception: c["ats"] = None
    # ATS boards
    boards = []
    for c in companies + watchlist:
        a = c.get("ats")
        if a and a.get("type") in ATS_FN:
            boards.append((a["type"], a["slug"], c["name"]))
    boards = list(dict.fromkeys(boards))
    ats_stat = {k: dict(ok=True, fetched=0, boards=0, errors=0) for k in ATS_FN}
    def runats(b):
        typ, slug, name = b
        try:
            return typ, ATS_FN[typ](slug, name), None
        except Exception as e:
            return typ, [], f"{slug}: {str(e)[:100]}"
    with cf.ThreadPoolExecutor(10) as ex:
        for typ, res, err in ex.map(runats, boards):
            ats_stat[typ]["boards"] += 1
            ats_stat[typ]["fetched"] += len(res)
            if err: ats_stat[typ]["errors"] += 1; ats_stat[typ].setdefault("error_samples", []).append(err)
            raw += res
    for k, v in ats_stat.items():
        if v["boards"]:
            v["ok"] = v["errors"] < v["boards"]
            status[k] = v
    log("ats", ats_stat)
    return raw, status


def loc_score(d):
    india = d["india"]["status"]
    loc = loc_points(india, d["work_mode"], d["india"].get("cities") or [], d.get("location") or "")
    if india == "likely" and d["india"].get("basis") == "company":
        loc = 82 if d["india"].get("company") == "yes" else 76
    return loc

def finish_india(docs, checks):
    """After the company check: refresh location score, fit, why/concerns and the change hash."""
    for d in docs:
        res = checks.get(d["company_slug"]) or {}
        v = d["india"].get("company")
        d["scores"]["location"] = round(loc_score(d))
        d["fit"] = fit(d["scores"])
        why = [w for w in d["why"] if w != d["india"].get("reason")]
        con = [c for c in d["concerns"] if c != "Employer has not confirmed hiring from India" and "limited to named countries" not in c]
        if d["india"]["status"] in ("confirmed", "likely"):
            why.insert(min(len(why), 4), d["india"]["reason"])
        if v == "yes" and not res.get("india_based") and d["india"].get("basis") != "company" and d["india"]["status"] != "confirmed":
            why.append(f"{d['company']} already hires people in India")
        if d["india"]["status"] == "unconfirmed":
            con.insert(0, "Employer has not confirmed hiring from India")
        if v == "no" and d["india"]["status"] in ("likely", "unconfirmed"):
            con.insert(0, f"{d['company']}'s other roles are limited to named countries; confirm India is eligible")
        d["why"], d["concerns"] = why[:6], con[:6]
        d["content_hash"] = hashlib.sha1(json.dumps([d["content_hash"][:12], d["india"]["status"], v, d["india"]["reason"], d.get("posted_at"), d.get("date_quality")]).encode()).hexdigest()[:12]


def india_queue(existing, limit=6):
    """Companies whose India hiring is still unclear and that have not had a web check in 14 days."""
    comps = existing.get("companies", {}); jobs = existing.get("jobs", {})
    verify = ((existing.get("meta", {}).get("verify") or {}).get("map")) or {}
    need, unc = {}, {}
    for jid, j in jobs.items():
        st = (verify.get(jid) or {}).get("s") or j.get("status")
        if st in ("closed", "stale", "excluded"): continue
        s_ = (j.get("india") or {}).get("status")
        if s_ in ("unconfirmed", "likely"):
            need[j.get("company_slug")] = need.get(j.get("company_slug"), 0) + 1
            if s_ == "unconfirmed": unc[j.get("company_slug")] = unc.get(j.get("company_slug"), 0) + 1
    out = []
    rank = lambda kv: (0 if unc.get(kv[0]) else 1, 0 if ((comps.get(kv[0]) or {}).get("india_hiring") or {}).get("verdict", "unclear") == "unclear" else 1, -kv[1])
    for slug, n in sorted(need.items(), key=rank):
        c = comps.get(slug) or {}
        ih = c.get("india_hiring") or {}
        if ih.get("verdict") in ("yes", "no") or ih.get("india_based"): continue
        w = ih.get("web") or {}
        ca = parse_date(w.get("checked_at"))
        if ca and (NOW - ca).days < 14: continue
        name = c.get("name") or slug
        dom = c.get("domain")
        out.append(dict(slug=slug, name=name, open_roles=n, verdict=ih.get("verdict", "unchecked"),
                        query=(f'site:{dom} ' if dom else f'"{name}" ') + 'careers (India OR "work from anywhere" OR countries OR "employer of record" OR "remote")'))
        if len(out) >= limit: break
    return out

# ---------------------------------------------------------------- LinkedIn dates
LI_ACTIVITY = re.compile(r"(?:activity|ugcPost|share)[-:](\d{19})")
LI_JOB = re.compile(r"linkedin\.com/jobs/view/(?:[^/?#]*-)?(\d{9,11})(?:[/?#]|$)")

def linkedin_post_time(url):
    """LinkedIn post and activity IDs carry their creation time: the top 41 bits are milliseconds since 1970."""
    m = LI_ACTIVITY.search(url or "")
    if not m: return None
    try:
        t = dt.datetime.fromtimestamp((int(m.group(1)) >> 22) / 1000, dt.timezone.utc)
        return t if dt.datetime(2015, 1, 1, tzinfo=dt.timezone.utc) < t <= NOW + dt.timedelta(hours=1) else None
    except Exception:
        return None

def estimate_linkedin_job_dates(docs):
    """LinkedIn job IDs increase over time. Fit a robust line through IDs whose posting date the listing showed,
    then estimate dates for listings that showed none. Only inside the range the anchors cover, and always labelled as an estimate."""
    def jid(d):
        for s in d.get("sources") or []:
            m = LI_JOB.search(s.get("url") or "")
            if m: return int(m.group(1))
        return None
    anchors = [(jid(d), parse_date(d["posted_at"]).timestamp()) for d in docs if d.get("posted_at") and d.get("date_quality") in ("approx", "exact") and jid(d)]
    anchors = [a for a in anchors if a[0]]
    if len(anchors) < 6: return 0
    xs = sorted(anchors)
    slopes = []
    for i in range(len(xs)):
        for k in range(i + 1, len(xs)):
            if xs[k][0] != xs[i][0]: slopes.append((xs[k][1] - xs[i][1]) / (xs[k][0] - xs[i][0]))
    if not slopes: return 0
    slopes.sort(); b = slopes[len(slopes) // 2]
    if b <= 0: return 0
    inter = sorted(t - b * x for x, t in xs); a0 = inter[len(inter) // 2]
    lo, hi = xs[0][0], xs[-1][0]; pad = (hi - lo) * 0.15
    n = 0
    for d in docs:
        if d.get("posted_at"): continue
        x = jid(d)
        if not x or x < lo - pad or x > hi + pad: continue
        t = dt.datetime.fromtimestamp(a0 + b * x, dt.timezone.utc)
        if t > NOW: t = NOW
        d["posted_at"] = iso(t.replace(hour=0, minute=0, second=0, microsecond=0))
        d["date_quality"] = "estimated"
        d["date_note"] = f"Estimated from the LinkedIn job ID against {len(xs)} dated LinkedIn listings (about ±3 days)"
        n += 1
    return n

# ---------------------------------------------------------------- published contact channels
EMAIL_RX = re.compile(r"(?<![\w.+-])([A-Za-z0-9][A-Za-z0-9._%+-]{0,40}@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,10})(?![\w-])")
EMAIL_SKIP = re.compile(r"(?i)^(no-?reply|donotreply|privacy|legal|security|abuse|support|help|accessibility|accommodations?|dpo|gdpr|compliance|press|media|info@sentry|example)|@(example|email|domain|sentry|himalayas|remoteok|remotive|weworkremotely|workingnomads|jobicy|linkedin|indeed|naukri|glassdoor)\.")
PHONE_RX = re.compile(r"(?i)(?:call|whatsapp|contact|phone|mobile|reach (?:us|me|out)|hr)\D{0,25}?(\+?\d[\d\s\-()]{8,16}\d)")

def _phone_ok(p):
    digits = re.sub(r"\D", "", p)
    if len(digits) < 10 or len(digits) > 13: return None
    if digits.startswith("91") and len(digits) == 12: digits = digits[2:]
    if len(digits) == 10 and digits[0] in "6789": return "+91 " + digits[:5] + " " + digits[5:]
    if p.strip().startswith("+"): return p.strip()
    return None

def posting_contacts(docs, descs):
    """Emails and phone numbers that an employer printed in its own job posting for applications."""
    out = {}
    for d in docs:
        t = descs.get(d["id"]) or ""
        if not t: continue
        for m in EMAIL_RX.finditer(t):
            e = m.group(1).strip(".").lower()
            if EMAIL_SKIP.search(e): continue
            ctx = t[max(0, m.start() - 120): m.end() + 40].replace("\n", " ")
            if not re.search(r"(?i)resume|cv|apply|application|portfolio|send|email|mail|reach|contact|hiring|career|job|hr\b|recruit|talent", ctx): continue
            cid = hid(company_key(d["company"]), e)
            c = out.setdefault(cid, dict(id=cid, name=e, title="Email published in the job posting", company=d["company"], company_slug=d["company_slug"], category="channel",
                linkedin=None, other_url=None, email=e, phone=None, relevance="The employer asks applicants to use this address", evidence_url=d["url"],
                evidence_label="Job posting", evidence_quote="…" + ctx.strip()[:200] + "…", verified_at=iso(NOW), confidence="high", job_ids=[], named=False, source="posting"))
            if d["id"] not in c["job_ids"]: c["job_ids"].append(d["id"])
        for m in PHONE_RX.finditer(t):
            p = _phone_ok(m.group(1))
            if not p: continue
            ctx = t[max(0, m.start() - 80): m.end() + 20].replace("\n", " ")
            cid = hid(company_key(d["company"]), re.sub(r"\D", "", p))
            c = out.setdefault(cid, dict(id=cid, name=p, title="Phone number published in the job posting", company=d["company"], company_slug=d["company_slug"], category="channel",
                linkedin=None, other_url=None, email=None, phone=p, relevance="The employer lists this number for applicants", evidence_url=d["url"],
                evidence_label="Job posting", evidence_quote="…" + ctx.strip()[:180] + "…", verified_at=iso(NOW), confidence="high", job_ids=[], named=False, source="posting"))
            if d["id"] not in c["job_ids"]: c["job_ids"].append(d["id"])
    return list(out.values())

# ---------------------------------------------------------------- web enrichment (search-index snippets only)
SAL_SITES = {"glassdoor": "Glassdoor", "ambitionbox": "AmbitionBox", "levels.fyi": "Levels.fyi", "6figr": "6figr", "payscale": "PayScale", "indeed": "Indeed", "naukri": "Naukri", "salaryexpert": "SalaryExpert", "builtin": "Built In"}
SAL_MARKET_QUERIES = [
    ('"QA tester" salary India (site:ambitionbox.com OR site:glassdoor.co.in OR site:payscale.com)', "India"),
    ('"game tester" salary India (site:ambitionbox.com OR site:glassdoor.co.in OR site:payscale.com)', "India"),
    ('"software tester" salary Mumbai (site:ambitionbox.com OR site:glassdoor.co.in OR site:payscale.com)', "Mumbai"),
    ('"QA engineer" salary Mumbai (site:ambitionbox.com OR site:glassdoor.co.in OR site:payscale.com)', "Mumbai"),
    ('"QA tester" remote salary (site:glassdoor.com OR site:payscale.com OR site:builtin.com)', "Global"),
]
ROLE_OK = re.compile(r"(?i)\bqa\b|quality|tester|testing|test engineer|test analyst|sdet")

def _money(amt, unit=""):
    s = amt.replace(" ", "")
    cur = "INR" if "₹" in s or re.search(r"(?i)rs|inr", s) else ("USD" if "$" in s else ("EUR" if "€" in s else ("GBP" if "£" in s else None)))
    num = re.sub(r"[^\d.]", "", s.replace(",", ""))
    if not num or not cur: return None, None
    v = float(num)
    u = (unit or "").lower()
    if u.startswith("l") or u.startswith("lakh"): v *= 1e5
    elif u in ("k",): v *= 1e3
    elif u in ("m",): v *= 1e6 if cur != "INR" else 1e6
    elif u in ("cr", "crore"): v *= 1e7
    return v, cur

def _sane(v, cur):
    if v is None: return False
    return (cur == "INR" and 1.5e5 <= v <= 3e7) or (cur in ("USD", "EUR", "GBP") and 3e3 <= v <= 6e5)

SAL_PATTERNS = [
    re.compile(r"(?i)(?:estimated )?average (?:base )?(?:salary|pay|total pay) for (?:an? )?(?P<role>[^.]{3,70}?)(?: at (?P<co>[^.]{2,50}?))?(?: in (?P<loc>[^.]{2,40}?))? is (?P<amt>[₹$€£]\s?[\d,]+(?:\.\d+)?)\s*(?P<unit>lakhs?|L|k|K|M|Cr)?\b(?:\s*(?:per year|/yr|a year|per annum|yearly))?(?P<tail>[^.]{0,60})"),
    re.compile(r"(?i)median (?P<role>[^.]{3,60}?) (?:salary|compensation package|total compensation) (?:at (?P<co>[^.]{2,40}?) )?in (?P<loc>[^.]{2,40}?) (?:is|totals) (?P<amt>[₹$€£]\s?[\d,]+(?:\.\d+)?)\s*(?P<unit>k|K|M|L)?(?P<tail>)"),
    re.compile(r"(?i)(?P<role>[A-Za-z /&+\-]{3,50}) salar(?:y|ies)[^.]{0,40}? average (?:salary )?is (?P<amt>₹\s?[\d,]+(?:\.\d+)?)\s*(?P<unit>lakhs?|L)(?P<tail>)"),
    re.compile(r"(?i)average salary is (?P<amt>₹\s?[\d,]+(?:\.\d+)?)\s*(?P<unit>lakhs?|L)(?P<role>)(?P<tail>)"),
]
SAL_RANGE2 = re.compile(r"(?i)average salary of an? (?P<role>[^.]{3,60}?) at (?P<co>[^.]{2,40}?)(?: in (?P<loc>[^.]{2,30}?))? typically ranges from (?P<lo>[₹$]\s?[\d,.]+)\s*(?P<lu>lakhs?|L|k|K)? to (?P<hi>[₹$]\s?[\d,.]+)\s*(?P<hu>lakhs?|L|k|K)?")
SAL_RANGE = re.compile(r"(?i)(?P<role>(?:senior |junior |sr\.? )?(?:software |game |manual |mobile )?(?:qa (?:tester|engineer|analyst)|tester|test engineer|test analyst|quality assurance (?:engineer|analyst)|qa)) salar(?:y|ies)[^₹]{0,80}?₹\s?(?P<lo>[\d.]+)\s*L(?:akhs|/yr)?\s*(?:-|to)\s*₹\s?(?P<hi>[\d.]+)\s*L(?:akhs|/yr)?")

def parse_salary_hits(results, company_name=None, market_hint=None):
    """Turn search-result snippets from salary sites into benchmark records, keeping the quoted sentence as evidence."""
    out = []
    for h in results or []:
        url = h.get("url") or ""
        if not safe_http(url): continue
        host = re.sub(r"^https?://", "", url).split("/")[0].lower()
        site = next((v for k, v in SAL_SITES.items() if k in host), None)
        if not site: continue
        title = h.get("title") or ""; text = f"{title}. {h.get('description') or ''}"
        if company_name and not re.search(r"(?i)\b" + re.escape(company_name) + r"\b", text): continue
        found = []
        for rx in SAL_PATTERNS:
            for m in rx.finditer(text):
                role = (m.group("role") or "").strip(" .:-") or re.sub(r"(?i)\s*salar.*$", "", title).strip()
                if not ROLE_OK.search(role): continue
                v, cur = _money(m.group("amt"), m.group("unit") or "")
                if cur == "USD" and site.startswith("Glassdoor") and ".co.in" in host and re.search(r"\$[\d]{1,3},\d{2},\d{3}", m.group("amt")): pass
                if not _sane(v, cur): continue
                loc = (m.groupdict().get("loc") or "").strip()
                tail = m.groupdict().get("tail") or ""
                lm = re.search(r"(?i)\bin ([A-Z][A-Za-z ,]{2,30}?)(?:,| which|\.|$)", tail)
                if not loc and lm: loc = lm.group(1).strip()
                co = (m.groupdict().get("co") or "").strip()
                found.append(dict(role=role[:60], amt=v, cur=cur, loc=loc[:40], co=co[:50], quote=m.group(0).strip()[:220]))
        for m in SAL_RANGE2.finditer(text):
            lo, c1 = _money(m.group("lo"), m.group("lu") or ""); hi, c2 = _money(m.group("hi"), m.group("hu") or "")
            if ROLE_OK.search(m.group("role")) and c1 == c2 and _sane(lo, c1) and _sane(hi, c2) and lo <= hi:
                found.append(dict(role=m.group("role").strip()[:60], lo=lo, hi=hi, cur=c1, loc=(m.group("loc") or "").strip()[:40], co=m.group("co").strip()[:50], quote=m.group(0).strip()[:220]))
        for m in SAL_RANGE.finditer(text):
            lo, hi = float(m.group("lo")) * 1e5, float(m.group("hi")) * 1e5
            if _sane(lo, "INR") and _sane(hi, "INR") and lo <= hi:
                found.append(dict(role=m.group("role").strip()[:60], lo=lo, hi=hi, cur="INR", loc="", co=company_name or "", quote=m.group(0).strip()[:220]))
        for f in found[:2]:
            market = "India" if f["cur"] == "INR" or re.search(r"(?i)india|bangalore|bengaluru|mumbai|delhi|pune|hyderabad|chennai", f.get("loc", "") + title) else ("United States" if re.search(r"(?i)united states|\bus\b|palo alto|new york|san francisco|seattle", f.get("loc", "") + title) else (market_hint or "Global"))
            key = hashlib.sha1((url + f["role"] + str(f.get("amt") or f.get("lo"))).encode()).hexdigest()[:10]
            rec = dict(id=f"web-{key}", kind="web", label=f"{f['role']}" + (f" at {company_name}" if company_name else "") + (f" — {f['loc']}" if f.get("loc") else (f" — {market}" if market and market != "Global" else "")),
                       role=f["role"], market=market, currency=f["cur"], source_name=site, source_url=url, retrieved_at=NOW.strftime("%Y-%m-%d"),
                       basis=f"Figure quoted in the {site} search result: “{f['quote']}”", page_title=title[:140])
            if "amt" in f: rec.update(avg=round(f["amt"]), low=None, high=None)
            else: rec.update(avg=None, low=round(f["lo"]), high=round(f["hi"]))
            if company_name: rec["company"] = company_name
            out.append(rec)
    # one record per (url, role)
    seen, uniq = set(), []
    for r in out:
        k = (r["source_url"], r["role"].lower())
        if k in seen: continue
        seen.add(k); uniq.append(r)
    return uniq

LI_COMPANY = re.compile(r"^https?://(?:[a-z]{2,3}\.)?(?:www\.)?linkedin\.com/company/([A-Za-z0-9\-_%.]+)")

LEGAL_SUFFIX = re.compile(r"(?i)[\s,]+(inc\.?|llc|ltd\.?|limited|corp\.?|corporation|pvt\.?|private|gmbh|plc|co\.?)$")
def pick_linkedin_company(results, name, domain=None, hq=None):
    """Choose the employer's own LinkedIn page among same-name companies. Scored on: the employer's web domain
    appearing in the result (strong), the page slug and title matching the name, a country subdomain that matches
    the employer's headquarters, search rank, and a penalty for pages describing a defunct or acquired venture."""
    best, best_score = None, -9
    key = re.sub(r"[^a-z0-9]", "", company_key(name))
    dom = re.sub(r"^www\.", "", (domain or "").lower()); dom_root = dom.split(".")[0]
    in_india = bool(hq and re.search(r"(?i)\b(IN|India|Bengaluru|Bangalore|Mumbai|Delhi|Gurugram|Gurgaon|Noida|Pune|Hyderabad|Chennai)\b", hq))
    for i, h in enumerate(results or []):
        m = LI_COMPANY.match(h.get("url") or "")
        if not m: continue
        slug = urllib.parse.unquote(m.group(1)).lower().strip("/")
        if "/" in slug: continue
        title = (h.get("title") or "").strip(); desc = h.get("description") or ""
        tname = re.split(r"\s*[|\-–:]\s*(?:LinkedIn|领英|Jobs|Culture)?\s*$", title)[0].strip("\u200f ").strip()
        tname = re.sub(r"\s*(?:\||-|–)\s*LinkedIn.*$", "", tname).strip()
        if not re.search(r"(?i)\b" + re.escape(name.split()[0]) + r"\b", tname): continue
        tcore = LEGAL_SUFFIX.sub("", tname).strip()
        sc = 0
        if dom and (dom in desc.lower() or dom in title.lower()): sc += 4
        elif dom_root and slug.replace("-", "") == dom_root: sc += 1
        if slug.replace("-", "") == key or slug.replace("-", "") == key + "ai" or slug.replace("-", "") in (key + "inc", key + "hq"): sc += 2
        if company_key(tcore) == company_key(name) or re.sub(r"[^a-z0-9]", "", company_key(tcore)) == key: sc += 2
        elif len(tname.split()) > len(name.split()) + 1: sc -= 2
        host = urllib.parse.urlparse(h.get("url") or "").hostname or ""
        if in_india and host.startswith("in."): sc += 1
        if re.search(r"(?i)\bwas (?:a|an)\b[^.]{0,80}\b(?:acquired|shut down|closed|defunct)\b", desc): sc -= 3
        sc += max(0, 2 - i)
        if sc > best_score:
            best_score, best = sc, dict(linkedin=f"https://www.linkedin.com/company/{slug}", linkedin_title=tname[:100], linkedin_about=desc[:240],
                                        confidence="high" if sc >= 6 else "medium")
    return best if best_score >= 2 else None

def _own_domain(email, name, dom):
    """True when the address is on the employer's domain: the known one, or (when unknown) a domain that spells the company name."""
    d = email.split("@")[-1]
    if dom: return d == dom or d.endswith("." + dom)
    key = re.sub(r"[^a-z0-9]", "", company_key(name)); labels = d.split(".")
    return len(key) >= 4 and ("".join(labels[:-1]) == key or "".join(labels) == key or (len(labels) >= 2 and labels[-2] == key))

def careers_emails(results, name, domain):
    out = []
    dom = re.sub(r"^www\.", "", (domain or "").lower())
    for h in results or []:
        url = h.get("url") or ""
        if not safe_http(url): continue
        text = f"{h.get('title', '')} {h.get('description', '')}"
        if not (re.search(r"(?i)\b" + re.escape(name) + r"\b", text) or (dom and dom.split(".")[0] in url.lower())): continue
        for m in EMAIL_RX.finditer(text):
            e = m.group(1).lower().strip(".")
            if not _own_domain(e, name, dom): continue
            if EMAIL_SKIP.search(e): continue
            local = e.split("@")[0]
            if not re.search(r"(?i)career|job|hr|hiring|recruit|talent|people|resume|cv|join|work", local): continue
            ctx = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text[max(0, m.start() - 100): m.end() + 40]).replace("\n", " ")
            out.append(dict(email=e, url=url, quote="…" + ctx.strip()[:180] + "…", page=(h.get("title") or "")[:100]))
    seen, uniq = set(), []
    for c in out:
        if c["email"] in seen: continue
        seen.add(c["email"]); uniq.append(c)
    return uniq[:3]

def _period(kind):
    return NOW.strftime("%Y%m") if kind in ("salary_company", "salary_market", "careers_email", "linkedin_company") else NOW.strftime("%G%V")

def web_queue(existing, companies_reg, limit=4):
    """The searches worth running this hour, most useful first. Each is recorded in web_checks so it is not repeated within its refresh period."""
    done = set((existing.get("web_checks") or {}).keys())
    comps = existing.get("companies", {}); jobs = existing.get("jobs", {}); links = existing.get("company_links", {})
    verify = ((existing.get("meta", {}).get("verify") or {}).get("map")) or {}
    contacts = existing.get("contacts", {})
    reg = {c["slug"]: c for c in companies_reg}
    open_by = {}
    for jid, j in jobs.items():
        if not isinstance(j, dict): continue
        st = (verify.get(jid) or {}).get("s") or j.get("status")
        if st in ("closed", "stale", "excluded"): continue
        open_by.setdefault(j.get("company_slug"), []).append(j)
    def best_fit(slug): return max([j.get("fit") or 0 for j in open_by.get(slug, [])] or [0])
    ranked = sorted(open_by, key=lambda s: -best_fit(s))
    out = []
    def add(kind, slug, name, query, extra=None):
        key = f"{kind}-{slug or 'all'}-{_period(kind)}"
        if key in done or any(o["key"] == key for o in out): return
        out.append(dict(key=key, kind=kind, slug=slug, name=name, query=query, **(extra or {})))
    # 1) employers whose India hiring is unclear (role eligibility depends on it)
    for q in india_queue(existing, limit=1):
        add("india", q["slug"], q["name"], q["query"])
    # 2) LinkedIn company pages for the best-fitting employers (makes people links company-specific)
    for slug in ranked[:25]:
        if slug in links or not slug: continue
        c = comps.get(slug) or reg.get(slug) or {}
        name = c.get("name") or (open_by[slug][0].get("company") if open_by.get(slug) else slug)
        add("linkedin_company", slug, name, f'site:linkedin.com/company "{name}"')
        if sum(1 for o in out if o["kind"] == "linkedin_company") >= 1: break
    # 3) what each top employer pays designers
    for slug in ranked[:20]:
        c = comps.get(slug) or reg.get(slug) or {}
        if (c.get("tier") or 3) > 2: continue
        name = c.get("name") or slug
        add("salary_company", slug, name, f'"{name}" (QA OR tester OR "test engineer") salary (site:ambitionbox.com OR site:glassdoor.co.in OR site:glassdoor.com OR site:payscale.com)')
        if sum(1 for o in out if o["kind"] == "salary_company") >= 1: break
    # 4) careers or HR email published on the employer's own pages
    for slug in ranked[:20]:
        c = comps.get(slug) or reg.get(slug) or {}
        dom = c.get("domain"); nm = c.get("name") or slug
        if any((x.get("company_slug") == slug and x.get("email")) for x in contacts.values() if isinstance(x, dict)): continue
        if dom: add("careers_email", slug, nm, f'"@{dom}" (careers OR jobs OR hiring OR resume OR talent OR hr)', dict(domain=dom))
        else: add("careers_email", slug, nm, f'{nm} careers contact email jobs')
        if sum(1 for o in out if o["kind"] == "careers_email") >= 1: break
    # 5) market pay (monthly)
    for i, (q, mk) in enumerate(SAL_MARKET_QUERIES):
        add("salary_market", f"m{i}", mk, q)
    return out[:limit]

def apply_web(web, existing, comps, docs):
    """Turn this run's search results into documents: company LinkedIn pages, salary benchmarks, published careers emails and a check log."""
    links, checks, salaries, emails = {}, {}, [], []
    for key, w in (web or {}).items():
        if not isinstance(w, dict): continue
        kind = w.get("kind") or ("india" if key.startswith("india") else "")
        slug = w.get("slug"); res = w.get("results") or []
        name = w.get("name") or (comps.get(slug) or {}).get("name") or slug
        n_found = 0
        if kind == "linkedin_company" and slug:
            got = pick_linkedin_company(res, name, (comps.get(slug) or {}).get("domain") or w.get("domain"), (comps.get(slug) or {}).get("hq"))
            if got:
                links[slug] = dict(slug=slug, name=name, **got, found_via=w.get("query"), checked_at=iso(NOW)); n_found = 1
        elif kind in ("salary_company", "salary_market"):
            recs = parse_salary_hits(res, company_name=name if kind == "salary_company" else None, market_hint=None if kind == "salary_company" else name)
            for r in recs:
                if slug and kind == "salary_company": r["company_slug"] = slug
                r["query"] = w.get("query")
            salaries += recs; n_found = len(recs)
        elif kind == "careers_email" and slug:
            dom = w.get("domain") or (comps.get(slug) or {}).get("domain")
            for c in careers_emails(res, name, dom):
                cid = hid(slug, c["email"])
                emails.append(dict(id=cid, name=c["email"], title="Careers email published by the company", company=name, company_slug=slug, category="channel",
                                   linkedin=None, other_url=None, email=c["email"], phone=None, relevance="Published on a page about working at " + name,
                                   evidence_url=c["url"], evidence_label=c["page"] or "Company page", evidence_quote=c["quote"], verified_at=iso(NOW),
                                   confidence="medium", job_ids=[], named=False, source="web"))
                n_found += 1
        elif kind == "india":
            n_found = len(res)
        checks[key] = dict(kind=kind, slug=slug, name=name, query=w.get("query"), results=len(res), found=n_found, checked_at=iso(NOW))
    return links, checks, salaries, emails


def load_dir(d):
    out = {}
    if not d or not os.path.isdir(d): return out
    for root, _, files in os.walk(d):
        for f in files:
            if f.endswith(".json"):
                coll = os.path.relpath(root, d).split(os.sep)[0]
                try:
                    out.setdefault(coll, {})[f[:-5]] = json.load(open(os.path.join(root, f)))
                except Exception:
                    pass
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    ap.add_argument("--existing")
    ap.add_argument("--companies", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "companies.json"))
    ap.add_argument("--watchlist")
    ap.add_argument("--leads")
    ap.add_argument("--benchmarks")
    ap.add_argument("--india-web", help="JSON {slug: {query, results:[{url,title,description}]}} from web searches")
    ap.add_argument("--india-queue", action="store_true", help="print companies that need a web check, then exit")
    ap.add_argument("--web-queue", action="store_true", help="print this run's enrichment searches as JSON, then exit")
    ap.add_argument("--web", help="JSON {key: {kind, slug, name, query, results:[{url,title,description}]}} from the enrichment searches")
    a = ap.parse_args()
    if a.india_queue:
        print(json.dumps(india_queue(load_dir(a.existing)), ensure_ascii=False, indent=1)); return
    if a.web_queue:
        reg = json.load(open(a.companies)) if a.companies and os.path.exists(a.companies) else []
        print(json.dumps(web_queue(load_dir(a.existing), reg), ensure_ascii=False, indent=1)); return
    if not a.out: ap.error("--out is required")
    t0 = time.time()
    companies = json.load(open(a.companies)) if a.companies and os.path.exists(a.companies) else []
    watch = []
    if a.watchlist and os.path.isdir(a.watchlist):
        for f in glob.glob(os.path.join(a.watchlist, "**", "*.json"), recursive=True):
            try:
                d = json.load(open(f)); watch.append(d.get("data", d) if isinstance(d, dict) and "name" not in d else d)
            except Exception: pass
    elif a.watchlist and os.path.exists(a.watchlist):
        watch = json.load(open(a.watchlist))
    existing = load_dir(a.existing)
    fx = load_fx()
    raw, status = run_connectors(companies, watch)
    leads = json.load(open(a.leads)) if a.leads and os.path.exists(a.leads) else []
    official_recs, leads, dropped_leads = resolve_official_leads(leads)
    raw += official_recs
    for L in leads:
        exact = linkedin_post_time(L.get("url"))
        if exact: L["date_note"] = None
        raw.append(make_record(title=L["title"], company=L["company"], url=L["url"], apply_url=L.get("apply_url"), official=bool(L.get("official")),
            source_id=L.get("source_id", "search"), source_name=L.get("source_name", "Search index"), source_kind=L.get("source_kind", "community"),
            source_job_id=L["url"], posted_at=exact or parse_date(L.get("posted_at")), location=L.get("location", ""), restrictions=L.get("restrictions", []),
            remote=L.get("remote", False), workplace=L.get("workplace"), employment=L.get("employment", ""), desc=L.get("desc", ""), salary=L.get("salary"),
            deadline=L.get("deadline")))
        raw[-1].update(level=L.get("level"), staffing=L.get("staffing"), contact=L.get("contact"), date_note=L.get("date_note"))
    benchmarks = json.load(open(a.benchmarks)) if a.benchmarks and os.path.exists(a.benchmarks) else []
    docs, texts, comps, excluded, contacts, bms = build(raw, companies, fx, benchmarks, existing)
    web = {}
    if a.india_web and os.path.exists(a.india_web):
        try:
            web = json.load(open(a.india_web))
            if isinstance(web, list): web = {x.get("slug"): x for x in web if isinstance(x, dict)}
        except Exception as e: log("india-web", e)
    webx = {}
    if a.web and os.path.exists(a.web):
        try: webx = json.load(open(a.web))
        except Exception as e: log("web", e)
    for key, w in webx.items():
        if isinstance(w, dict) and (w.get("kind") == "india" or key.startswith("india-")) and w.get("slug"):
            web[w["slug"]] = w
    descs_all = {t["id"]: t["text"] for t in texts}
    checks, upgraded = apply_india_checks(docs, comps, descs_all, existing, web, companies)
    n_est = estimate_linkedin_job_dates(docs)
    finish_india(docs, checks)
    links, wchecks, web_salaries, web_emails = apply_web(webx, existing, comps, docs)
    contacts += [c for c in posting_contacts(docs, descs_all) + web_emails if c["id"] not in {x["id"] for x in contacts}]
    bms = list(bms) + web_salaries
    for d in docs:
        cs = [c for c in contacts if d["id"] in c["job_ids"] or c["company_slug"] == d["company_slug"]]
        d["contacts_count"] = len(cs); d["has_public_email"] = any(c.get("email") for c in cs)
        d["has_named_contact"] = any(c["named"] for c in cs); d["has_hiring_manager"] = any(c["category"] == "hiring_team" for c in cs)
    for sub, items in (("company_links", links), ("web_checks", wchecks)):
        os.makedirs(os.path.join(a.out, sub), exist_ok=True)
        for k, v in items.items(): json.dump(v, open(os.path.join(a.out, sub, k + ".json"), "w"), ensure_ascii=False)
    log("enrichment:", len(links), "LinkedIn pages,", len(web_salaries), "salary figures,", len(web_emails), "careers emails;", n_est, "LinkedIn dates estimated")
    iv = {}
    for r in checks.values(): iv[r["verdict"]] = iv.get(r["verdict"], 0) + 1
    log("india checks", iv, "roles clarified:", upgraded)
    os.makedirs(a.out, exist_ok=True)
    for sub in ["jobs", "job_text", "companies", "contacts", "salary_benchmarks"]:
        os.makedirs(os.path.join(a.out, sub), exist_ok=True)
    for d in docs: json.dump(d, open(os.path.join(a.out, "jobs", d["id"] + ".json"), "w"), ensure_ascii=False)
    for t in texts: json.dump(t, open(os.path.join(a.out, "job_text", t["id"] + ".json"), "w"), ensure_ascii=False)
    for s, c in comps.items(): json.dump(c, open(os.path.join(a.out, "companies", s + ".json"), "w"), ensure_ascii=False)
    for c in contacts: json.dump(c, open(os.path.join(a.out, "contacts", c["id"] + ".json"), "w"), ensure_ascii=False)
    for b in bms: json.dump(b, open(os.path.join(a.out, "salary_benchmarks", b["id"] + ".json"), "w"), ensure_ascii=False)
    ex_counts = {}
    for r, why in excluded:
        k = why.split(":")[0] if why.startswith("Not open to India") else why
        ex_counts[k] = ex_counts.get(k, 0) + 1
    ex_samples = [dict(title=r["title"], company=r.get("company"), reason=why, url=r.get("url"), source=r["source_name"]) for r, why in excluded
                  if why not in ("Not a QA / testing role", "Older than window") and TITLE_QA.search(r["title"])][:200]
    run = dict(id=NOW.strftime("%Y%m%dT%H%M%SZ"), started_at=iso(NOW), finished_at=iso(dt.datetime.now(dt.timezone.utc)), duration_s=round(time.time() - t0, 1),
               pipeline_version=PIPELINE_VERSION, raw_count=len(raw), kept=len(docs), excluded_counts=ex_counts, excluded_samples=ex_samples,
               source_status=status, enrichment=dict(searches=len(webx), linkedin_pages=len(links), salary_figures=len(web_salaries), careers_emails=len(web_emails), dates_estimated=n_est), india_checks=dict(companies=len(checks), verdicts=iv, roles_clarified=upgraded, web_checked=len(web)), fx=dict(as_of=fx["as_of"], usd_inr=fx["rates"]["INR"], source=fx["source"]) if fx else None)
    json.dump(run, open(os.path.join(a.out, "sync_log.json"), "w"), ensure_ascii=False, indent=1)
    json.dump([dict(title=r["title"], company=r.get("company"), reason=w, url=r.get("url"), loc=r.get("location_raw"), source=r["source_name"]) for r, w in excluded],
              open(os.path.join(a.out, "excluded_all.debug.json"), "w"), ensure_ascii=False, indent=0)
    json.dump(dict(fx=fx), open(os.path.join(a.out, "fx.json"), "w"))
    json.dump(sorted({str(r.get("source_job_id")) for r in raw if r.get("source_job_id")}), open(os.path.join(a.out, "seen_source_ids.json"), "w"))
    ex_ids = {}
    for r, why in excluded:
        if r.get("company") and r.get("title") and why not in ("Not a QA / testing role",):
            ex_ids[job_id_for(r["company"], r["title"])] = why
    for d in dropped_leads:
        if d.get("company") and d.get("title"):
            ex_ids[job_id_for(d["company"], d["title"])] = d["reason"]
    json.dump(ex_ids, open(os.path.join(a.out, "excluded_ids.json"), "w"))
    log(f"done: raw={len(raw)} kept={len(docs)} excluded={len(excluded)} in {run['duration_s']}s")

if __name__ == "__main__":
    main()
