#!/bin/sh
# Integration test: real gRPC server + client over localhost.
set -eu
bin="$1"
port=50091
pids=""
cleanup() { for p in $pids; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

start_server() {
  "$bin" server --port "$port" "$@" > /dev/null 2>&1 &
  pids="$pids $!"
  sleep 0.5
}

# 1) Light load, continuous batching: every request completes and every token verifies.
start_server --workers 2 --mode continuous --max-batch 8 --policy taildrop --capacity 64 \
  --step-fixed-us 300 --step-per-seq-us 20 --prefill-us 50
"$bin" client --port "$port" --count 200 --rate 200 --interactive-frac 0.3 --b-deadline-ms 3000 --i-deadline-ms 3000 \
  | python3 -c '
import json, sys
x = json.load(sys.stdin); a = x["all"]; s = x["server"]
assert a["ok"] == 200 and a["bad_stream"] == 0 and a["errors"] == 0, x
assert x["interactive"]["ok"] + x["batch"]["ok"] == 200, x
assert s["completed"] == 200 and s["tokens_wasted"] == 0 and s["mean_batch"] >= 1, s
print("light load ok: 200/200, mean batch %.2f" % s["mean_batch"])'

# 2) Corrupted prompt checksum -> DATA_LOSS, nothing admitted.
"$bin" client --port "$port" --count 5 --rate 50 --corrupt-checksum 1 \
  | python3 -c '
import json, sys
x = json.load(sys.stdin)["all"]
assert x["data_loss"] == 5 and x["ok"] == 0, x
print("checksum ok: 5/5 DATA_LOSS")'
cleanup; pids=""

# 3) Overload with a tiny tail-drop queue: rejections happen, every request is accounted for.
port=50092
start_server --workers 1 --mode none --policy taildrop --capacity 4 --step-fixed-us 2000
"$bin" client --port "$port" --count 300 --rate 1000 --b-deadline-ms 2000 \
  | python3 -c '
import json, sys
x = json.load(sys.stdin); a = x["all"]
assert a["rejected"] > 0, a
assert a["ok"] + a["rejected"] + a["deadline"] + a["data_loss"] + a["bad_stream"] + a["errors"] == 300, a
assert a["bad_stream"] == 0 and a["errors"] == 0, a
print("overload ok: %d ok, %d rejected, %d deadline" % (a["ok"], a["rejected"], a["deadline"]))'
cleanup; pids=""

# 4) Deadline-aware admission rejects requests that cannot make it instead of timing them out.
port=50093
start_server --workers 1 --mode continuous --max-batch 4 --policy deadline --capacity 1000 --step-fixed-us 2000
"$bin" client --port "$port" --count 300 --rate 1000 --b-deadline-ms 300 \
  | python3 -c '
import json, sys
x = json.load(sys.stdin); a = x["all"]; s = x["server"]
assert s["rejected_deadline"] > 0 and a["ok"] > 0, x
assert a["bad_stream"] == 0 and a["errors"] == 0, a
print("deadline-aware ok: %d ok, %d rejected early, %d timed out" % (a["ok"], a["rejected"], a["deadline"]))'
echo "integration: all checks passed"
