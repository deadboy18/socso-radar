"""
PERKESO IC Checker  v6  — Persistent State + TTL Expiry for Invalids
─────────────────────────────────────────────────────────────────────
Two master files in ic_results/:

  master_valid.json
      → permanent list of full records for every IC confirmed valid
        (valid = real person in PERKESO, their DOB/name won't change)

  master_invalid.json
      → { "ic_number": "850427105006", "checked_at": "2026-07-15" }
        invalids expire after INVALID_TTL_DAYS and get re-checked
        (person could register with PERKESO at any time)

IC structure recap (12 digits, each is unique):
    YYMMDD  PB  NNN  C
    ──────  ──  ───  ─
    DOB     State  Seq  Gender+Checksum

    610212-07-5425  →  born 1961-02-12, Penang, seq 542  (unique person)
    850427-07-5425  →  born 1985-04-27, Penang, seq 542  (different person)
    These are DIFFERENT 12-digit numbers. No cross-DOB confusion possible.

Local:        python ic_checker.py
Non-interactive:
    python ic_checker.py --mode 1 --max-checks 200 --workers 2
    python ic_checker.py --mode 2 --date 1985-04-27 --pb 10
    python ic_checker.py --mode 3 --ics 610212075425,920913235001
"""

import argparse, random, json, os, time, signal, threading, queue, sys
from datetime import date, timedelta, datetime
from collections import deque

try:
    import requests
except ImportError:
    sys.exit("requests not installed.  Run: pip install requests")

# ── Config ────────────────────────────────────────────────────────────────────
API_URL          = "https://lindungfaedah.perkeso.gov.my/auth/script/check_ic.php"
OUTPUT_DIR       = "ic_results"
VALID_FILE       = os.path.join(OUTPUT_DIR, "master_valid.json")
INVALID_FILE     = os.path.join(OUTPUT_DIR, "master_invalid.json")

INVALID_TTL_DAYS = 30     # re-check invalids after this many days
DELAY_MIN        = 1.5   # ~2 req/s at 3 workers — safe for gov API, 2.5× faster than before
DELAY_MAX        = 60.0  # maximum backoff — gives PERKESO time to cool down
WORKERS_MAX      = 3     # 3 workers × 1.5s = ~2 req/s total — sweet spot of speed vs safety
STATS_EVERY      = 10
FLUSH_EVERY      = 25     # write invalid buffer to disk every N additions

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
    for i in range(_s, _e+1): REGION_LEGACY[str(i).zfill(2)] = _r

MALAYSIAN_REGIONS = {**REGION_PRIMARY, **REGION_LEGACY}
WEIGHTS = [2,4,8,5,10,9,7,3,6,1,2]

STATE_POOL = (
    ["10"]*20 + ["14"]*18 + ["01"]*12 + ["07"]*10 + ["08"]*8 +
    ["12"]*7  + ["13"]*7  + ["03"]*4  + ["05"]*4  + ["06"]*4  +
    ["02"]*3  + ["04"]*2  + ["11"]*2  + ["09"]*1  + ["15"]*1  + ["16"]*1
)

def state_name(pb): return MALAYSIAN_REGIONS.get(pb, f"Unknown ({pb})")

# ── IC helpers ────────────────────────────────────────────────────────────────
def compute_check_digit(digits11):
    rev = digits11[::-1]
    return (12 - (sum(rev[j]*WEIGHTS[j] for j in range(11)) % 11)) % 11

def all_valid_sequences(yy, mm, dd, pb, year):
    seq_range = range(0,500) if year >= 2000 else range(500,1000)
    out = []
    for s in seq_range:
        s_str    = str(s).zfill(3)
        digits11 = [int(ch) for ch in (yy+mm+dd+pb+s_str)]
        c = compute_check_digit(digits11)
        if c != 10: out.append((s, c))
    return out

def random_date(from_year=1965, to_year=2000):
    start = date(from_year, 1, 1)
    end   = date(to_year,  12, 31)
    d     = start + timedelta(days=random.randint(0, (end-start).days))
    return d.year, d.month, d.day

def calc_age(year, month, day):
    today = date.today()
    try:
        dob = date(year, month, day)
        return f"{today.year-dob.year-((today.month,today.day)<(dob.month,dob.day))}y"
    except ValueError: return "?"

def make_ic_record(yy, mm, dd, pb, year, seq, chk):
    s_str = str(seq).zfill(3)
    ic    = yy+mm+dd+pb+s_str+str(chk)
    return {
        "ic_number":     ic,
        "ic_formatted":  f"{yy}{mm}{dd}-{pb}-{s_str}{chk}",
        "date_of_birth": f"{year}-{mm}-{dd}",
        "age":           calc_age(year, int(mm), int(dd)),
        "pb_code":       pb,
        "pb_name":       state_name(pb),
        "sequence":      seq,
        "check_digit":   chk,
        "gender":        "Male" if chk%2==1 else "Female",
    }

# ── API ───────────────────────────────────────────────────────────────────────
def check_perkeso(icno):
    resp = requests.get(API_URL, params={"icno": icno}, timeout=10)
    resp.raise_for_status()
    return resp.json()

# ══════════════════════════════════════════════════════════════════════════════
#  Persistent State
# ══════════════════════════════════════════════════════════════════════════════
class PersistentState:
    """
    master_valid.json
        List of full record dicts for every valid IC.
        PERMANENT — valid records never expire or get deleted.

    master_invalid.json
        List of {"ic": "...", "t": "YYYY-MM-DD"} objects.
        EXPIRES after INVALID_TTL_DAYS days.
        Re-checking expired invalids catches people who registered
        with PERKESO after we first checked them.

    is_checked(ic) returns True only for:
        - valid ICs (always)
        - invalid ICs checked within the last INVALID_TTL_DAYS
    Expired invalids are transparently removed and will be re-queued.
    """

    def __init__(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self._lock = threading.Lock()
        today      = date.today()
        cutoff     = today - timedelta(days=INVALID_TTL_DAYS)

        # Load valid (permanent)
        raw_valid          = self._load_json(VALID_FILE, [])
        self._valid_records: list = raw_valid
        self._valid_ics: set      = {r["ic_number"] for r in raw_valid}

        # Load invalid — split into live vs expired
        raw_invalid = self._load_json(INVALID_FILE, [])
        live, expired = [], []
        for entry in raw_invalid:
            try:
                checked = date.fromisoformat(entry["t"])
                if checked >= cutoff:
                    live.append(entry)
                else:
                    expired.append(entry["ic"])
            except (KeyError, ValueError):
                pass   # malformed entry — drop it

        self._invalid_live: list  = live          # still-valid cache entries
        self._invalid_ics:  set   = {e["ic"] for e in live}
        self._invalid_buf:  list  = []            # pending flush buffer

        self._all_checked: set    = self._valid_ics | self._invalid_ics

        # Stats for startup display
        self.expired_count = len(expired)
        self.loaded_valid   = len(self._valid_ics)
        self.loaded_invalid = len(self._invalid_ics)

        # If any expired records existed, rewrite the file without them
        if expired:
            self._write_json(INVALID_FILE, self._invalid_live)

    # ── Queries ───────────────────────────────────────────────────────────────
    @property
    def valid_count(self):   return len(self._valid_ics)
    @property
    def invalid_count(self): return len(self._invalid_ics)
    @property
    def total_checked(self): return len(self._all_checked)

    def is_checked(self, ic: str) -> bool:
        with self._lock:
            return ic in self._all_checked

    # ── Record valid ──────────────────────────────────────────────────────────
    def add_valid(self, ic: str, generated: dict, api_response: dict,
                  checked_at: str):
        with self._lock:
            if ic in self._all_checked:
                # Could be previously invalid — remove from invalid list
                if ic in self._invalid_ics:
                    self._invalid_ics.discard(ic)
                    self._invalid_live = [e for e in self._invalid_live if e["ic"] != ic]
                else:
                    return   # already valid, nothing to do
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
            self._write_json(VALID_FILE, self._valid_records)

    # ── Record invalid (with TTL) ─────────────────────────────────────────────
    def add_invalid(self, ic: str):
        with self._lock:
            if ic in self._valid_ics:
                return   # never downgrade a valid record
            # Update or add the entry with today's date
            today_str = date.today().isoformat()
            if ic in self._invalid_ics:
                # Refresh the timestamp (re-checked, still invalid)
                for e in self._invalid_live:
                    if e["ic"] == ic:
                        e["t"] = today_str
                        break
            else:
                self._invalid_live.append({"ic": ic, "t": today_str})
                self._invalid_ics.add(ic)
                self._all_checked.add(ic)
            self._invalid_buf.append(ic)
            if len(self._invalid_buf) >= FLUSH_EVERY:
                self._flush_invalid()

    def flush(self):
        with self._lock:
            self._flush_invalid()

    def _flush_invalid(self):
        if self._invalid_buf:
            self._write_json(INVALID_FILE, self._invalid_live)
            self._invalid_buf.clear()

    # ── Atomic write ──────────────────────────────────────────────────────────
    @staticmethod
    def _load_json(path, default):
        if os.path.exists(path) and os.path.getsize(path) > 2:
            try: return json.load(open(path, "r", encoding="utf-8"))
            except Exception: pass
        return default

    @staticmethod
    def _write_json(path, data):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        os.replace(tmp, path)

# ══════════════════════════════════════════════════════════════════════════════
#  Rate Controller
# ══════════════════════════════════════════════════════════════════════════════
class RateController:
    def __init__(self, workers):
        self.delay   = DELAY_MIN          # start at minimum — ramp up only if needed
        self.workers = workers
        self._w      = deque(maxlen=50)   # larger window = more stable decisions
        self._lock   = threading.Lock()
        self._rate_limited_until = 0      # epoch time — pause all workers if 429

    def rate_limited(self, retry_after: int = 60):
        """Call when HTTP 429 received. Pauses all workers for retry_after seconds."""
        with self._lock:
            self._rate_limited_until = time.time() + retry_after
            self.delay = min(self.delay * 2, DELAY_MAX)
        safe_print(f"\n  🚫 429 RATE LIMITED — pausing all workers for {retry_after}s\n")
        time.sleep(retry_after)

    def wait_if_limited(self):
        """Workers call this before each request to honour any active 429 pause."""
        pause = self._rate_limited_until - time.time()
        if pause > 0:
            time.sleep(pause)

    def record(self, ok: bool):
        with self._lock:
            self._w.append(ok)
            if len(self._w) < 10: return          # need at least 10 samples
            err = self._w.count(False) / len(self._w)

            if err > 0.40:
                # Heavy errors — double the delay (exponential back-off), cap at MAX
                self.delay = min(self.delay * 2, DELAY_MAX)
            elif err > 0.20:
                # Moderate errors — add 3 seconds
                self.delay = min(self.delay + 3.0, DELAY_MAX)
            elif err > 0.05:
                # Light errors — add 1 second
                self.delay = min(self.delay + 1.0, DELAY_MAX)
            elif err == 0 and len(self._w) == 50:
                # Perfect run for 50 checks — nudge delay down slightly
                self.delay = max(self.delay - 0.2, DELAY_MIN)

    def get_delay(self):
        with self._lock: return self.delay

    def summary(self):
        with self._lock:
            e = self._w.count(False) if self._w else 0
            lim = max(0, self._rate_limited_until - time.time())
            s = f"delay={self.delay:.1f}s  err={e}/{len(self._w)}"
            if lim > 0: s += f"  RATE-LIMITED ({lim:.0f}s remaining)"
            return s

# ── Print lock ────────────────────────────────────────────────────────────────
_plock = threading.Lock()
def safe_print(*a, **kw):
    with _plock: print(*a, **kw)

# ── Core check ────────────────────────────────────────────────────────────────
def do_check(gen, index, rate_ctrl, state, run_stats):
    # Honour any active rate-limit pause before sleeping the normal delay
    rate_ctrl.wait_if_limited()
    delay = rate_ctrl.get_delay()
    time.sleep(random.uniform(delay, delay * 1.3))   # slight positive jitter only

    icno = gen["ic_number"]
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        api = check_perkeso(icno)
        rate_ctrl.record(True)
        is_valid = api.get("akses") == 1
    except requests.exceptions.HTTPError as e:
        # 429 = explicit rate limit — back off hard
        if e.response is not None and e.response.status_code == 429:
            retry_after = int(e.response.headers.get("Retry-After", 60))
            rate_ctrl.rate_limited(retry_after)
        api      = {"error": str(e)}
        is_valid = None
        rate_ctrl.record(False)
    except requests.exceptions.RequestException as e:
        api      = {"error": str(e)}
        is_valid = None
        rate_ctrl.record(False)

    run_stats["checked"] += 1

    if is_valid is True:
        state.add_valid(icno, gen, api, now)
        run_stats["valid"] += 1
        rec = api["debug"]["data"][0]
        tag = f"✅  VALID   — {rec.get('name','N/A')}  DOB: {rec.get('dob','N/A')}"
    elif is_valid is False:
        state.add_invalid(icno)
        run_stats["invalid"] += 1
        tag = "❌  INVALID"
    else:
        run_stats["errors"] += 1
        tag = f"⚠️   ERROR  — {api.get('error','?')}"

    c   = run_stats
    pct = c["valid"] / max(c["checked"], 1) * 100
    safe_print(f"  [{index}] {gen['ic_formatted']}  "
               f"({gen['date_of_birth']}, {gen['gender']}, {gen['pb_name']})")
    safe_print(f"        {tag}   "
               f"({c['valid']}/{c['checked']} this run, {pct:.0f}%  "
               f"| all-time: {state.valid_count}✅ {state.invalid_count}❌)")

    if c["checked"] % STATS_EVERY == 0:
        safe_print(f"\n{'─'*62}")
        safe_print(f"  This run : {c['checked']} checked | "
                   f"✅ {c['valid']} | ❌ {c['invalid']} | ⚠️  {c['errors']}")
        safe_print(f"  All-time : {state.valid_count} valid | "
                   f"{state.invalid_count} invalid (expire in {INVALID_TTL_DAYS}d)")
        safe_print(f"  {rate_ctrl.summary()}")
        safe_print(f"{'─'*62}\n")

    return is_valid

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 1 — Infinite / batch adaptive
# ══════════════════════════════════════════════════════════════════════════════
def run_infinite(from_year, to_year, initial_workers, state,
                 max_checks=None, max_runtime=None):
    rate_ctrl  = RateController(initial_workers)
    run_stats  = {"checked":0, "valid":0, "invalid":0, "errors":0}
    shutdown   = threading.Event()
    task_q     = queue.Queue(maxsize=300)
    hot_dobs   = deque()
    tried      = {}   # {DOB+PB key → set of seqs queued this run}
    idx_lock   = threading.Lock()
    idx_cnt    = [0]
    start_time = time.time()
    deadline   = (start_time + max_runtime * 60) if max_runtime else None

    def next_idx():
        with idx_lock: idx_cnt[0] += 1; return idx_cnt[0]

    def should_stop():
        if shutdown.is_set(): return True
        if max_checks and run_stats["checked"] >= max_checks: return True
        if deadline   and time.time() >= deadline: return True
        return False

    def enqueue_dob(year, month, day, pb, limit=None, shuffle=True):
        yy  = str(year % 100).zfill(2)
        mm  = str(month).zfill(2)
        dd  = str(day).zfill(2)
        key = f"{yy}{mm}{dd}{pb}"
        done = tried.setdefault(key, set())
        cands = [c for c in all_valid_sequences(yy, mm, dd, pb, year)
                 if c[0] not in done]
        if shuffle: random.shuffle(cands)
        added = 0
        for seq, chk in cands:
            if should_stop(): return
            if limit and added >= limit: break
            rec = make_ic_record(yy, mm, dd, pb, year, seq, chk)
            # Skip if already known AND not expired
            if state.is_checked(rec["ic_number"]): continue
            done.add(seq)
            try:
                task_q.put(rec, timeout=3)
                added += 1
            except queue.Full: break

    def producer():
        while not should_stop():
            if hot_dobs:
                yr, mo, dy, pb = hot_dobs.popleft()
                enqueue_dob(yr, mo, dy, pb, limit=20, shuffle=False)
            else:
                yr, mo, dy = random_date(from_year, to_year)
                pb = random.choice(STATE_POOL)
                enqueue_dob(yr, mo, dy, pb, limit=5)
        task_q.put(None)

    def worker():
        while not should_stop():
            try: gen = task_q.get(timeout=2)
            except queue.Empty: continue
            if gen is None: task_q.put(None); break
            result = do_check(gen, next_idx(), rate_ctrl, state, run_stats)
            if result is True:
                p = gen["date_of_birth"].split("-")
                hot_dobs.append((int(p[0]), int(p[1]), int(p[2]), gen["pb_code"]))
            if should_stop(): shutdown.set()
            task_q.task_done()

    def handle_stop(sig, frame):
        safe_print("\n\n  🛑  Stopping..."); shutdown.set()
    signal.signal(signal.SIGINT, handle_stop)

    limit_label = (f"max {max_checks} checks" if max_checks
                   else f"max {max_runtime} min" if max_runtime
                   else "infinite — Ctrl+C to stop")
    print("=" * 62)
    print(f"  Smart Random + Adaptive  [{limit_label}]")
    print(f"  DOB {from_year}–{to_year}  |  workers {initial_workers}  |  "
          f"delay {DELAY_MIN}–{DELAY_MAX}s (auto)")
    print(f"  ⏭  Skipping {state.total_checked:,} ICs already checked")
    if state.expired_count:
        print(f"  🔄 {state.expired_count} expired invalids removed — will re-check")
    print(f"  💾 {VALID_FILE}  +  {INVALID_FILE}  (TTL {INVALID_TTL_DAYS}d)")
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
    state.flush()

    elapsed = int(time.time() - start_time)
    e_str   = f"{elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d}"
    c       = run_stats
    print(f"\n{'='*62}")
    print(f"  Run complete  |  {e_str}")
    print(f"  This run : {c['checked']} | ✅ {c['valid']} | ❌ {c['invalid']} | ⚠️  {c['errors']}")
    print(f"  All-time : {state.valid_count} valid | {state.invalid_count} invalid")
    print(f"{'='*62}")
    print(f"\n  💾 Valid   → {VALID_FILE}   ({state.valid_count} records, permanent)")
    print(f"  💾 Invalid → {INVALID_FILE}  "
          f"({state.invalid_count} records, expire after {INVALID_TTL_DAYS}d)\n")

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 2 — Full DOB Sweep
# ══════════════════════════════════════════════════════════════════════════════
def run_full_sweep(year, month, day, pb, workers, state):
    yy    = str(year % 100).zfill(2)
    mm    = str(month).zfill(2)
    dd    = str(day).zfill(2)
    cands = all_valid_sequences(yy, mm, dd, pb, year)
    todo  = [make_ic_record(yy, mm, dd, pb, year, s, c)
             for s, c in cands if not state.is_checked(yy+mm+dd+pb+str(s).zfill(3)+str(c))]
    skip  = len(cands) - len(todo)
    total = len(todo)

    rate_ctrl = RateController(workers)
    run_stats = {"checked":0,"valid":0,"invalid":0,"errors":0}
    shutdown  = threading.Event()

    def handle_stop(sig, frame):
        safe_print("\n\n  🛑  Stopping..."); shutdown.set()
    signal.signal(signal.SIGINT, handle_stop)

    eta = total * ((DELAY_MIN+DELAY_MAX)/2) / workers / 60
    print("=" * 62)
    print(f"  Full DOB Sweep — {year}-{mm}-{dd}  [{pb}] {state_name(pb)}")
    print(f"  {total} to check  |  {skip} skipped  |  ETA ~{eta:.0f} min")
    print("=" * 62 + "\n")

    task_q = queue.Queue()
    for i, gen in enumerate(todo): task_q.put((i+1, gen))

    def worker():
        while not shutdown.is_set():
            try: idx, gen = task_q.get_nowait()
            except queue.Empty: break
            do_check(gen, idx, rate_ctrl, state, run_stats)
            task_q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads: t.start()
    for t in threads: t.join()
    state.flush()

    c = run_stats
    print(f"\n{'='*62}")
    print(f"  Done  |  {c['checked']} checked  |  ✅ {c['valid']}  |  ❌ {c['invalid']}")
    print(f"  All-time: {state.valid_count} valid | {state.invalid_count} invalid\n")

# ══════════════════════════════════════════════════════════════════════════════
#  MODE 3 — Manual
# ══════════════════════════════════════════════════════════════════════════════
def run_manual(ic_list, workers, state):
    rate_ctrl = RateController(workers)
    run_stats = {"checked":0,"valid":0,"invalid":0,"errors":0}
    todo = []

    for icno in ic_list:
        icno = icno.strip()
        if not icno.isdigit() or len(icno) != 12:
            print(f"  ⚠️  SKIPPED {icno!r} — must be 12 digits"); continue
        if state.is_checked(icno):
            tag = "valid" if icno in state._valid_ics else f"invalid (re-checks in {INVALID_TTL_DAYS}d)"
            print(f"  ⏭   SKIP {icno} — already {tag}"); continue
        pb = icno[6:8]
        todo.append({
            "ic_number":    icno,
            "ic_formatted": f"{icno[:6]}-{pb}-{icno[8:]}",
            "date_of_birth": f"{icno[:2]}/{icno[2:4]}/{icno[4:6]}",
            "age":"N/A", "pb_code":pb, "pb_name":state_name(pb),
            "sequence":int(icno[8:11]),"check_digit":int(icno[11]),
            "gender":"Male" if int(icno[11])%2==1 else "Female",
        })

    print("=" * 62)
    print(f"  Manual — {len(todo)} to check  |  {workers} workers")
    print("=" * 62 + "\n")

    task_q = queue.Queue()
    for i, gen in enumerate(todo): task_q.put((i+1, gen))

    def worker():
        while True:
            try: idx, gen = task_q.get_nowait()
            except queue.Empty: break
            do_check(gen, idx, rate_ctrl, state, run_stats)
            task_q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads: t.start()
    for t in threads: t.join()
    state.flush()

    c = run_stats
    print(f"\n{'='*62}")
    print(f"  Done  |  {c['checked']} checked  |  ✅ {c['valid']}  |  ❌ {c['invalid']}")
    print(f"  All-time: {state.valid_count} valid | {state.invalid_count} invalid\n")

# ══════════════════════════════════════════════════════════════════════════════
#  CLI + entry point
# ══════════════════════════════════════════════════════════════════════════════
def parse_args():
    p = argparse.ArgumentParser(description="PERKESO IC Checker v6")
    p.add_argument("--mode",        type=int, choices=[1,2,3], default=None)
    p.add_argument("--workers",     type=int, default=2 if IS_CI else 3)
    p.add_argument("--from-year",   type=int, default=1965, dest="from_year")
    p.add_argument("--to-year",     type=int, default=2000, dest="to_year")
    p.add_argument("--max-checks",  type=int, default=None, dest="max_checks")
    p.add_argument("--max-runtime", type=int, default=None, dest="max_runtime")
    p.add_argument("--date",        type=str, default=None)
    p.add_argument("--pb",          type=str, default="10")
    p.add_argument("--ics",         type=str, default=None)
    p.add_argument("--ttl",         type=int, default=INVALID_TTL_DAYS,
                   help="Days before invalid records expire and are re-checked")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    INVALID_TTL_DAYS = args.ttl   # allow CLI override

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n  Loading persistent state...")
    state = PersistentState()
    print(f"  ✅ {state.loaded_valid:,} valid (permanent)")
    print(f"  ❌ {state.loaded_invalid:,} invalid (cached, expire in {INVALID_TTL_DAYS}d)")
    if state.expired_count:
        print(f"  🔄 {state.expired_count} expired records cleared — will re-check\n")
    else:
        print()

    workers = max(1, min(args.workers, WORKERS_MAX))

    if args.mode is not None:
        # ── Non-interactive (CI) ──────────────────────────────────────────────
        if args.mode == 1:
            run_infinite(args.from_year, args.to_year, workers, state,
                         max_checks=args.max_checks, max_runtime=args.max_runtime)
        elif args.mode == 2:
            if args.date:
                try: yr, mo, dy = [int(x) for x in args.date.split("-")]
                except ValueError: sys.exit(f"Bad --date: {args.date}")
            else: yr, mo, dy = random_date(args.from_year, args.to_year)
            pb = args.pb.zfill(2) if args.pb.zfill(2) in MALAYSIAN_REGIONS else "10"
            run_full_sweep(yr, mo, dy, pb, workers, state)
        elif args.mode == 3:
            if not args.ics: sys.exit("--ics required for mode 3")
            run_manual([x.strip() for x in args.ics.split(",") if x.strip()],
                       workers, state)
    else:
        # ── Interactive ───────────────────────────────────────────────────────
        print("=" * 62)
        print("  PERKESO IC Checker  v6")
        print("=" * 62)
        print("\n  [1]  Infinite Run  (adaptive, Ctrl+C to stop)")
        print("  [2]  Full DOB Sweep  (all seqs for one date+state)")
        print("  [3]  Manual  (paste IC numbers)")

        mode = input("\n  Choice [1/2/3, default=1]: ").strip() or "1"
        try:
            workers = int(input("  Workers [default=3, max=5]: ").strip() or "3")
            workers = max(1, min(workers, WORKERS_MAX))
        except ValueError: workers = 3

        if mode == "1":
            try:
                from_y = int(input("  Birth year from [default=1965]: ").strip() or "1965")
                to_y   = int(input("  Birth year to   [default=2000]: ").strip() or "2000")
            except ValueError: from_y, to_y = 1965, 2000
            run_infinite(from_y, to_y, workers, state)

        elif mode == "2":
            dob_in = input("\n  Date (YYYY-MM-DD) or Enter for random: ").strip()
            if dob_in:
                try: yr, mo, dy = [int(x) for x in dob_in.split("-")]
                except ValueError: yr, mo, dy = random_date()
            else: yr, mo, dy = random_date()
            print("  10=Selangor  14=WP KL  01=Johor  07=Penang  08=Perak")
            pb = input("  State code [default=10]: ").strip().zfill(2) or "10"
            if pb not in MALAYSIAN_REGIONS: pb = "10"
            run_full_sweep(yr, mo, dy, pb, workers, state)

        elif mode == "3":
            raw = input("\n  IC numbers (comma-separated):\n  > ").strip()
            ic_list = [x.strip() for x in raw.split(",") if x.strip()]
            if ic_list: run_manual(ic_list, workers, state)
            else: print("  No ICs entered.")
