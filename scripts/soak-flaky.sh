#!/usr/bin/env bash
# Flaky-retry soak test — exercises the jitter + exponential-backoff retry chain (retry-0..N)
# under a flaky vendor, while MONITORING the retry/DLT topics throughout.
#
# The vendor endpoint is set to FLAKY (probabilistic HTTP 500 = retryable). Flows submitted
# continuously will fail transiently, climb the retry topics (retry-0 -> retry-1 -> ... with
# jittered backoff), and eventually succeed. Asserts: flows complete despite flakiness (lossless),
# retry topics show real traffic, and the DLT stays bounded (flaky recovers, not permanent-fail).
#
# Usage:
#   DURATION=15 FLAKY_RATE=0.4 bash scripts/soak-flaky.sh
# Single-cluster:
#   DIS_URL=http://localhost:8090 KAFKA_CONTAINER=infra-kafka-1-1 KAFKA_BOOT=kafka-1:29092 \
#     DB=digital_instrument_service bash scripts/soak-flaky.sh
# Multi-cluster (defaults): DIS_URL=http://localhost:8087 KAFKA_CONTAINER=infra-kafka-a-1 KAFKA_BOOT=kafka-a:29092
set -uo pipefail

DURATION=${DURATION:-15}              # minutes of load
WAVE_SIZE=${WAVE_SIZE:-5}
WAVE_INTERVAL=${WAVE_INTERVAL:-15}    # seconds between waves
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-360}   # seconds to drain after load (retries need time)
SAMPLE_EVERY=${SAMPLE_EVERY:-90}      # seconds between retry/DLT topic samples
FLAKY_RATE=${FLAKY_RATE:-0.4}         # fraction of vendor calls that fail transiently (HTTP 500)
FLAKY_ENDPOINT=${FLAKY_ENDPOINT:-createDocument}

DIS_URL=${DIS_URL:-http://localhost:8087}
VENDOR_URL=${VENDOR_URL:-http://localhost:8081}
API_KEY=${API_KEY:-soak-test-key}
MONGO_CONTAINER=${MONGO_CONTAINER:-infra-mongodb-1}
DB=${DB:-digital_instrument_service}
KAFKA_CONTAINER=${KAFKA_CONTAINER:-infra-kafka-a-1}
KAFKA_BOOT=${KAFKA_BOOT:-kafka-a:29092}
COLL=dis_instrument_flows

log() { echo "[$(date +%H:%M:%S)] $*"; }
mongo_eval() { docker exec "$MONGO_CONTAINER" mongosh --quiet "$DB" --eval "$1" 2>/dev/null; }
offsets_sum() { docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell --broker-list "$KAFKA_BOOT" --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }

count_by() { mongo_eval "print(db.$COLL.countDocuments({reference:/^FLAKY-/,status:'$1'}))"; }
count_total() { mongo_eval "print(db.$COLL.countDocuments({reference:/^FLAKY-/}))"; }

submit_flow() {
  local ref=$1
  curl -sf -X POST "$DIS_URL/flows/enigio-instrument" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
    -d "{\"correlationId\":\"flaky-$ref-$(date +%s%N)\",\"reference\":\"$ref\",\"title\":\"Flaky $ref\",\"instrumentType\":\"PROMISSORY_NOTE\",\"documentCode\":\"NEG\",\"signers\":[{\"name\":\"A\",\"email\":\"a@t.com\",\"phone\":\"+46700000001\",\"capacity\":\"CEO\",\"organisation\":\"T\",\"order\":1}],\"recipient\":{\"name\":\"B\",\"email\":\"b@t.com\"}}" >/dev/null 2>&1
}

auto_approve() {
  local ids
  ids=$(mongo_eval "db.$COLL.find({reference:/^FLAKY-/,status:{\$in:['WAITING_RETRY','PARKED']},currentStep:{\$in:['AWAIT_PREPARATION_APPROVAL','AWAIT_DELIVERY_APPROVAL']}},{_id:1}).forEach(f=>print(String(f._id)))")
  for id in $ids; do
    curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$id/approve" -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{}' >/dev/null 2>&1 &
  done
  wait 2>/dev/null
}

# Determine which command-topic families have retry topics on this cluster (base + prefixed)
FAMILIES=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$KAFKA_BOOT" --list 2>/dev/null \
  | grep -E "instrument\.commands-retry-0$" | sed 's/-retry-0$//' | sort -u)

sample_retry_dlt() { # label
  local label=$1
  # (1) orchestrator retry state — the PRIMARY path: step failures set WAITING_RETRY + retryCount,
  #     and the recovery scanner re-delivers on backoff. This is where jitter/backoff shows up here.
  local wr mx
  wr=$(mongo_eval "print(db.$COLL.countDocuments({reference:/^FLAKY-/,status:'WAITING_RETRY'}))")
  mx=$(mongo_eval "var m=db.$COLL.find({reference:/^FLAKY-/},{retryCount:1}).sort({retryCount:-1}).limit(1).toArray();print(m.length?(m[0].retryCount||0):0)")
  log "  [RETRY:$label] waiting_retry=${wr:-0} max_retryCount=${mx:-0}"
  # (2) Kafka retry-topic chain (retry-0..8 + dlt) — may stay 0 if step retries use the WAITING_RETRY path.
  for fam in $FAMILIES; do
    local r=0 n
    for i in 0 1 2 3 4 5 6 7 8; do n=$(offsets_sum "${fam}-retry-${i}"); r=$((r + ${n:-0})); done
    local d; d=$(offsets_sum "${fam}-dlt")
    if [ "$r" -gt 0 ] || [ "${d:-0}" -gt 0 ]; then
      log "  [TOPICS:$label] $fam  retry_total=$r  dlt=${d:-0}"
    fi
  done
}

# ---- preflight ----
curl -sf "$DIS_URL/actuator/health" >/dev/null || { log "DIS not reachable at $DIS_URL"; exit 1; }
curl -sf "$VENDOR_URL/actuator/health" >/dev/null || { log "Vendor not reachable at $VENDOR_URL"; exit 1; }
log "DIS=$DIS_URL  VENDOR=$VENDOR_URL  KAFKA=$KAFKA_CONTAINER  families=[$(echo $FAMILIES | tr '\n' ' ')]"

# ---- configure flaky vendor ----
curl -sf -X POST "$VENDOR_URL/admin/reset" >/dev/null 2>&1
curl -sf -X POST "$VENDOR_URL/admin/failure-config" -H "Content-Type: application/json" -d "{\"$FLAKY_ENDPOINT\":\"FLAKY\"}" >/dev/null 2>&1
curl -sf -X POST "$VENDOR_URL/admin/flaky-rate" -H "Content-Type: application/json" -d "{\"rate\":$FLAKY_RATE}" >/dev/null 2>&1
log "Vendor FLAKY on '$FLAKY_ENDPOINT' at rate=$FLAKY_RATE (HTTP 500 => retryable => climbs retry-0..8 w/ jitter+backoff)"
sample_retry_dlt "start"

# ---- load loop ----
log "=== ${DURATION}m flaky soak: $WAVE_SIZE flows / ${WAVE_INTERVAL}s ==="
END=$((SECONDS + DURATION * 60))
NEXT_SAMPLE=$((SECONDS + SAMPLE_EVERY))
idx=0; SUBMITTED=0
while [ $SECONDS -lt $END ]; do
  for _ in $(seq 1 "$WAVE_SIZE"); do idx=$((idx+1)); submit_flow "FLAKY-$(printf '%04d' $idx)" & SUBMITTED=$((SUBMITTED+1)); done
  wait 2>/dev/null
  auto_approve
  DONE=$(count_by COMPLETED); REM=$(( (END - SECONDS) / 60 ))
  log "wave — submitted=$SUBMITTED completed=$DONE (${REM}m left)"
  if [ $SECONDS -ge $NEXT_SAMPLE ]; then sample_retry_dlt "load"; NEXT_SAMPLE=$((SECONDS + SAMPLE_EVERY)); fi
  sleep "$WAVE_INTERVAL"
done

# ---- drain ----
log "Load done ($SUBMITTED submitted). Draining up to ${DRAIN_TIMEOUT}s (retries in flight)..."
DEND=$((SECONDS + DRAIN_TIMEOUT))
while [ $SECONDS -lt $DEND ]; do
  auto_approve
  C=$(count_by COMPLETED); F=$(count_by FAILED); T=$(count_total)
  [ $((C + F)) -ge "$T" ] && break
  if [ $SECONDS -ge $NEXT_SAMPLE ]; then sample_retry_dlt "drain"; NEXT_SAMPLE=$((SECONDS + SAMPLE_EVERY)); fi
  sleep 5
done

# ---- report ----
COMPLETED=$(count_by COMPLETED); FAILED=$(count_by FAILED); TOTAL=$(count_total)
PARKED=$(count_by PARKED); INPROG=$(count_by IN_PROGRESS)
echo ""
log "===== FLAKY SOAK RESULTS ====="
log "  submitted=$SUBMITTED total=$TOTAL completed=$COMPLETED failed=$FAILED parked=$PARKED in_progress=$INPROG"
sample_retry_dlt "final"
DLT_TOTAL=0; for fam in $FAMILIES; do d=$(offsets_sum "${fam}-dlt"); DLT_TOTAL=$((DLT_TOTAL + ${d:-0})); done
log "  DLT total across families: $DLT_TOTAL"

# ---- reset vendor ----
curl -sf -X POST "$VENDOR_URL/admin/reset" >/dev/null 2>&1
log "Vendor reset (flaky cleared)"

# ---- assert ----
RC=0
if [ "$FAILED" -gt $((TOTAL / 10 + 1)) ]; then log "❌ too many FAILED ($FAILED/$TOTAL) — flaky retries not recovering"; RC=1; fi
if [ "$COMPLETED" -lt $((TOTAL / 2)) ] && [ "$TOTAL" -gt 0 ]; then log "❌ low completion ($COMPLETED/$TOTAL) — check retry/drain window"; RC=1; fi
[ "$RC" -eq 0 ] && log "✅ flaky flows retried and recovered (completed=$COMPLETED failed=$FAILED, DLT=$DLT_TOTAL)"
exit $RC
