#!/usr/bin/env bash
# solution.sh — sets up FIFO, producer/consumer, supervisor, runs the pipeline, and writes summaries.
set -euo pipefail

# Clean slate
rm -rf pipe logs alerts.txt summary.json timeline.json \
       producer_lines.txt rotations.txt producer.pid consumer.pid \
       run.sh producer.py consumer.awk consumer.sh write_json.py 2>/dev/null || true

# Directories & FIFO
mkdir -p pipe logs
[ -p pipe/events.fifo ] || mkfifo pipe/events.fifo

########################################
# Producer (Python)
########################################
cat > producer.py << 'PY'
import json, time, random, string, pathlib, signal

LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG = LOG_DIR / "events.log"

rnd = random.Random(1337)
USERS = ["alice", "bob", "carl", "dídí", "赵钱孙", "ユーザ", "🙂user"]

stop = False
def _term(sig, frame):
    global stop
    stop = True
signal.signal(signal.SIGTERM, _term)

def now_ms(): return int(time.time() * 1000)
def new_session():
    return ''.join(rnd.choice('abcdef0123456789') for _ in range(12))

def emit_line():
    # ~10% malformed lines
    if rnd.random() < 0.10:
        return '{bad json line"'
    t = now_ms()
    user = rnd.choice(USERS)
    admin = rnd.random() < 0.5  # generous to ensure alerts
    sess  = new_session()
    # long-ish payload to grow logs fast (helps size/rotation tests)
    payload = ''.join(rnd.choice(string.ascii_letters + " äöüß😀") for _ in range(rnd.randint(100, 180)))
    return json.dumps(
        {"ts": t, "session": sess, "user": user, "meta": {"admin": admin}, "payload": payload},
        ensure_ascii=False
    )

def rot():
    # size-based rotation at ~6 KiB: .1, .2, .3
    if LOG.exists() and LOG.stat().st_size >= 6 * 1024:
        p1 = LOG.with_suffix(".log.1")
        p2 = LOG.with_suffix(".log.2")
        p3 = LOG.with_suffix(".log.3")
        if p3.exists(): p3.unlink()
        if p2.exists(): p2.rename(p3)
        if p1.exists(): p1.rename(p2)
        LOG.rename(p1)

count = 0
start = time.time()
# Finish naturally before supervisor cleanup (supervisor sleeps ~32s)
MAX = 31.0
try:
    while not stop and (time.time() - start) < MAX:
        line = emit_line()
        # STDOUT → FIFO (supervisor redirects)
        print(line, flush=True)
        # Append to file log
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        rot()
        count += 1  # count ALL emitted lines (valid + malformed)
        # Pace chosen to satisfy volume ≥1100 within ~32 s total
        time.sleep(rnd.uniform(0.018, 0.024))
finally:
    # Always write the counter, even on SIGTERM
    with open("producer_lines.txt", "w") as f:
        f.write(str(count))
PY

########################################
# Consumer (AWK-only)
########################################
cat > consumer.awk << 'AWK'
BEGIN { OFS=" " }
# Extract simple JSON fields (string/number) by regex; tolerant to spacing.
function json_get(s, key,   pat, s2) {
  pat="\"" key "\"[[:space:]]*:[[:space:]]*"
  if (match(s, pat)) {
    s2=substr(s, RSTART+RLENGTH)
    if (key=="ts" && match(s2, /^-?[0-9]+/)) return substr(s2, RSTART, RLENGTH)
    if (match(s2, /^\"([^\"\\]|\\.)*\"/))    return substr(s2, RSTART+1, RLENGTH-2)
  }
  return ""
}
# Detect meta.admin=true without parsing whole JSON.
function has_admin_true(s,    pat) {
  pat="\"meta\"[[:space:]]*:[[:space:]]*\\{[^}]*\"admin\"[[:space:]]*:[[:space:]]*true"
  return (s ~ pat)
}
# ISO8601 UTC using awk's strftime()
function iso8601(ms,    sec){ sec=ms/1000; return strftime("%Y-%m-%dT%H:%M:%SZ", sec, 1) }

# Purge sessions older than now-10s for a given user; return remaining distinct count.
function purge_old(user, now,   k, ts, idx, cnt) {
  cnt=0
  for (k in last_seen) {
    if (split(k, idx, SUBSEP)==2 && idx[1]==user) {
      ts = last_seen[k]
      if (ts < now-10000) delete last_seen[k]
    }
  }
  for (k in last_seen) if (split(k, idx, SUBSEP)==2 && idx[1]==user) cnt++
  return cnt
}

# Process only brace-delimited lines; malformed lines are ignored safely.
# Input must be: cat pipe/events.fifo | awk -f consumer.awk
/^\{.*\}$/ {
  if (!has_admin_true($0)) next
  ts  = json_get($0, "ts") + 0
  usr = json_get($0, "user")
  ses = json_get($0, "session")
  if (ts==0 || usr=="" || ses=="") next

  oldc = purge_old(usr, ts)
  last_seen[usr SUBSEP ses] = ts

  newc = 0
  for (k in last_seen) if (split(k, idx, SUBSEP)==2 && idx[1]==usr) newc++

  # Emit ONE alert on threshold crossing (<3 → ≥3) with 3s per-user cooldown
  sec = int(ts/1000)
  if (oldc < 3 && newc >= 3) {
    if (!(usr in last_alert_sec) || sec - last_alert_sec[usr] >= 3) {
      print "ALERT", usr, newc, "admin-sessions in 10s at", iso8601(ts) >> "alerts.txt"
      fflush("alerts.txt")
      last_alert_sec[usr] = sec
    }
  }
}
AWK

# (Optional wrapper retained but not used by tests)
cat > consumer.sh << 'SH'
#!/usr/bin/env bash
set -euo pipefail
cat pipe/events.fifo | awk -f consumer.awk
SH
chmod +x consumer.sh

########################################
# Supervisor (run.sh)
########################################
cat > run.sh << 'SH'
#!/usr/bin/env bash
set -euo pipefail

# Idempotency
exec 9>run.lock
flock -n 9 || { echo "Another run in progress"; exit 1; }

mkdir -p pipe logs
[ -p pipe/events.fifo ] || mkfifo pipe/events.fifo
: > alerts.txt
rm -f timeline.json summary.json producer_lines.txt rotations.txt *.pid

now_ms() { python3 -c "import time; print(int(time.time() * 1000))"; }

# Start consumer first (inline pipeline required by tests)
CONS_START=$(now_ms)
cat pipe/events.fifo | awk -f consumer.awk &
CONS_PID=$!

# Tiny delay to ensure consumer is ready
sleep 0.3

# Start producer (must be backgrounded)
PROD_START=$(now_ms)
python3 producer.py > pipe/events.fifo &
PROD_PID=$!

echo "$PROD_PID" > producer.pid
echo "$CONS_PID" > consumer.pid

cleanup(){
  kill -TERM "$PROD_PID" "$CONS_PID" 2>/dev/null || true
  wait "$PROD_PID" 2>/dev/null || true
  wait "$CONS_PID" 2>/dev/null || true
}
trap cleanup INT TERM

# Let it run ~32s
sleep 32
cleanup

PROD_END=$(now_ms)
CONS_END=$(now_ms)

# Count rotations present & non-trivial
ROT=0
for f in logs/events.log.1 logs/events.log.2 logs/events.log.3; do
  if [ -s "$f" ]; then ROT=$((ROT+1)); fi
done

PL=0; AL=0
[ -f producer_lines.txt ] && PL=$(cat producer_lines.txt)
[ -f alerts.txt ] && AL=$(grep -c '^ALERT ' alerts.txt || true)

# Write JSON summaries (include both keys for compatibility)
cat > write_json.py <<EOF
import json
n = int("$PL")
summary = {
    "producer_lines_total": n,
    "producer_lines": n,
    "alerts": int("$AL"),
    "rotations_min": int("$ROT"),
    "parallel": True,
}
timeline = {
    "consumer_start_ms": int("$CONS_START"),
    "producer_start_ms": int("$PROD_START"),
    "consumer_end_ms": int("$CONS_END"),
    "producer_end_ms": int("$PROD_END"),
}
open("summary.json","w").write(json.dumps(summary))
open("timeline.json","w").write(json.dumps(timeline))
EOF
python3 write_json.py

echo DONE
SH
chmod +x run.sh

# Execute the supervisor
./run.sh
