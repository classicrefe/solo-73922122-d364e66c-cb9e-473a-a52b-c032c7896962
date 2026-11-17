#!/usr/bin/env bash
# End-to-end demo of the PITR recovery flow, including the strict-retirement
# guarantees added for the "old replica stop failure" incident:
#
#   collect -> restore point -> accidental delete -> keep collecting -> recover
#   -> fetch deleted rows on the replica -> switch to a second restore point
#   -> simulate an old replica that refuses to stop (new recovery rejected,
#      old replica keeps serving, its directory preserved)
#   -> clear the fault -> retry succeeds without any service restart
#
# Run against a live stack (default http://localhost:8000; override with BASE).
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
POINT="${POINT:-before_delete}"
POINT2="${POINT2:-before_delete_2}"

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
need curl; need jq

say "0. wait for the API and the startup physical base backup"
status=""
for _ in $(seq 1 90); do
  status=$(curl -fsS "$BASE/backup" 2>/dev/null | jq -r '.status // empty' || true)
  [ "$status" = "ready" ] && break
  sleep 2
done
[ "$status" = "ready" ] || fail "base backup never became ready (last: ${status:-unknown})"
curl -fsS "$BASE/backup" | jq .

say "1. simulate collection: 3 intersections x 20 minutes on the primary"
curl -fsS -X POST "$BASE/demo/generate?intersections=3&minutes=20&prefix=IX" | jq .

say "2. create named restore point '$POINT'"
curl -fsS -X POST "$BASE/restore-points?name=$POINT" | jq .

say "3. wait until its WAL is fully archived (point becomes selectable)"
rp=""
for _ in $(seq 1 60); do
  rp=$(curl -fsS "$BASE/restore-points" | jq -r --arg p "$POINT" \
       '.items[] | select(.name==$p) | .status')
  [ "$rp" = "ready" ] && break
  sleep 2
done
[ "$rp" = "ready" ] || fail "restore point never became ready"

say "4. operator accident: delete the latest 10 minutes on the primary"
curl -fsS -X POST "$BASE/demo/delete?minutes=10" | jq .

say "5. primary keeps collecting (new IXB intersections) - writes are not blocked"
curl -fsS -X POST "$BASE/demo/generate?intersections=2&minutes=3&prefix=IXB" | jq .

say "6. start PITR recovery to '$POINT'"
rec=$(curl -fsS -X POST "$BASE/recoveries" -H 'Content-Type: application/json' \
      -d "{\"restore_point\":\"$POINT\"}")
echo "$rec" | jq .
rid=$(echo "$rec" | jq -r .id)

st=""
for _ in $(seq 1 90); do
  st=$(curl -fsS "$BASE/recoveries/$rid" | jq -r .status)
  [ "$st" = "success" ] && break
  if [ "$st" = "failed" ]; then
    curl -fsS "$BASE/recoveries/$rid" | jq .
    fail "recovery failed"
  fi
  sleep 2
done
[ "$st" = "success" ] || fail "recovery did not succeed (last status: $st)"
curl -fsS "$BASE/recoveries/$rid" | jq .

say "7. query the recovered read-only replica"
replica=$(curl -fsS "$BASE/replica/traffic?limit=1000")
echo "$replica" | jq '{recovery: .recovery, total_rows: .total_rows, sample: .rows[0:3]}'

say "8. compare with the primary (which kept collecting)"
primary=$(curl -fsS "$BASE/primary/traffic?limit=1000")
echo "$primary" | jq '{total_rows: .total_rows, sample: .rows[0:3]}'

replica_total=$(echo "$replica" | jq .total_rows)
replica_ixb=$(echo "$replica" | jq '[.rows[] | select(.intersection_id | startswith("IXB"))] | length')
primary_total=$(echo "$primary" | jq .total_rows)
primary_ixb=$(echo "$primary" | jq '[.rows[] | select(.intersection_id | startswith("IXB"))] | length')

say "9. assertions"
[ "$replica_total" = "60" ] || fail "replica should hold 60 rows (pre-delete state), got $replica_total"
[ "$replica_ixb"  = "0"  ] || fail "replica must not contain post-restore-point commits (IXB), got $replica_ixb"
[ "$primary_total" = "36" ] || fail "primary should hold 36 rows (30 left + 6 new), got $primary_total"
[ "$primary_ixb"  = "6"  ] || fail "primary should hold 6 IXB rows, got $primary_ixb"

say "10. switch restore points: create a second point and recover to it"
# More collection, then a second restore point (strictly later than the first).
curl -fsS -X POST "$BASE/demo/generate?intersections=2&minutes=2&prefix=IXC" >/dev/null
curl -fsS -X POST "$BASE/restore-points?name=$POINT2" | jq .
for _ in $(seq 1 60); do
  rp2=$(curl -fsS "$BASE/restore-points" | jq -r --arg p "$POINT2" \
        '.items[] | select(.name==$p) | .status')
  [ "$rp2" = "ready" ] && break
  sleep 2
done
[ "${rp2:-}" = "ready" ] || fail "second restore point never became ready"

rec2=$(curl -fsS -X POST "$BASE/recoveries" -H 'Content-Type: application/json' \
       -d "{\"restore_point\":\"$POINT2\"}")
rid2=$(echo "$rec2" | jq -r .id)
for _ in $(seq 1 90); do
  st2=$(curl -fsS "$BASE/recoveries/$rid2" | jq -r .status)
  [ "$st2" = "success" ] && break
  if [ "$st2" = "failed" ]; then
    curl -fsS "$BASE/recoveries/$rid2" | jq .
    fail "second recovery failed"
  fi
  sleep 2
done
[ "$st2" = "success" ] || fail "second recovery did not succeed (last: $st2)"
# the old job is now superseded; only the new one is served
[ "$(curl -fsS "$BASE/recoveries/$rid"  | jq -r .status)" = "superseded" ] \
  || fail "first recovery should be superseded after the switch"
[ "$(curl -fsS "$BASE/recoveries/current" | jq -r .id)" = "$rid2" ] \
  || fail "current recovery should be the new one"
# the second replica serves its own point: it must contain the IXC rows
ixc=$(curl -fsS "$BASE/replica/traffic?limit=1000" \
      | jq '[.rows[] | select(.intersection_id | startswith("IXC"))] | length')
[ "$ixc" = "4" ] || fail "second replica should contain 4 IXC rows, got $ixc"
echo "switch OK: replica now serves '$POINT2' (IXC rows present), task $rid superseded"

say "11. fault injection: old replica refuses to stop -> new recovery rejected"
curl -fsS -X PUT "$BASE/demo/faults" -H 'Content-Type: application/json' \
     -d '{"simulate_stop_failure": true}' | jq .
blocked=$(curl -sS -o /tmp/blocked.json -w '%{http_code}' -X POST \
          "$BASE/recoveries" -H 'Content-Type: application/json' \
          -d "{\"restore_point\":\"$POINT\"}")
[ "$blocked" = "409" ] || fail "expected 409 while old postmaster is alive, got $blocked"
jq . /tmp/blocked.json
# no new job was created; the healthy old replica keeps serving its own point
[ "$(curl -fsS "$BASE/recoveries/current" | jq -r .id)" = "$rid2" ] \
  || fail "old replica must stay the served one after a rejected switch"
curl -fsS "$BASE/replica/traffic?limit=1" | jq '{served_recovery: .recovery.id, total_rows}'
# task history is still queryable, and the primary kept collecting throughout
curl -fsS "$BASE/primary/traffic?limit=1" | jq '{primary_total_rows: .total_rows}'

say "12. clear the fault and retry - no service restart"
curl -fsS -X PUT "$BASE/demo/faults" -H 'Content-Type: application/json' -d '{}' | jq .
rec3=$(curl -fsS -X POST "$BASE/recoveries" -H 'Content-Type: application/json' \
       -d "{\"restore_point\":\"$POINT\"}")
rid3=$(echo "$rec3" | jq -r .id)
for _ in $(seq 1 90); do
  st3=$(curl -fsS "$BASE/recoveries/$rid3" | jq -r .status)
  [ "$st3" = "success" ] && break
  if [ "$st3" = "failed" ]; then
    curl -fsS "$BASE/recoveries/$rid3" | jq .
    fail "retried recovery failed"
  fi
  sleep 2
done
[ "$st3" = "success" ] || fail "retried recovery did not succeed (last: $st3)"
[ "$(curl -fsS "$BASE/recoveries/current" | jq -r .id)" = "$rid3" ] \
  || fail "current recovery should be the retried one"
echo "retry OK: recovery $rid3 succeeded after the fault was cleared (api never restarted)"

printf '\n\033[32mDEMO OK: strict retirement holds, switch/query/retry all work, primary kept collecting.\033[0m\n'
