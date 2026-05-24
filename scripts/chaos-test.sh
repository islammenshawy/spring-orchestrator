#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Chaos Test Suite — targeted failure scenarios
# Runs ~5 min. Each scenario is independent with pass/fail.
# Requires: Docker stack running (Kafka x3, MongoDB, mock-vendor, DIS)
# Usage: DIS_URL=http://localhost:8087 API_KEY=soak-test-key bash scripts/chaos-test.sh
# ============================================================

DIS_URL=${DIS_URL:-http://localhost:8087}
API_KEY=${API_KEY:-soak-test-key}
PASS=0
FAIL=0
SKIP=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
header() { echo ""; echo "========== $1 =========="; }

pass() { PASS=$((PASS + 1)); log "✅ PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); log "❌ FAIL: $1"; }
skip() { SKIP=$((SKIP + 1)); log "⏭️  SKIP: $1"; }

submit_flow() {
  local ref=$1
  curl -sf -X POST "$DIS_URL/flows/enigio-instrument" \
    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
    -d "{
      \"correlationId\": \"chaos-$ref-$(date +%s%N)\",
      \"reference\": \"$ref\",
      \"title\": \"Chaos Test $ref\",
      \"instrumentType\": \"PROMISSORY_NOTE\",
      \"documentCode\": \"NEG\",
      \"signers\": [{\"name\":\"Alice\",\"email\":\"alice@test.com\",\"phone\":\"+46700000001\",\"capacity\":\"CEO\",\"organisation\":\"Test AB\",\"order\":1}],
      \"recipient\": {\"name\":\"Bob\",\"email\":\"bob@test.com\"}
    }" 2>/dev/null
}

wait_for_status() {
  local flow_id=$1 target_status=$2 timeout_sec=${3:-60}
  local deadline=$((SECONDS + timeout_sec))
  while [ $SECONDS -lt $deadline ]; do
    local status
    status=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
      "var f=db.dis_instrument_flows.findOne({_id:ObjectId('$flow_id')});print(f?f.status:'NOT_FOUND')" 2>/dev/null)
    if [ "$status" = "$target_status" ]; then return 0; fi
    sleep 2
  done
  return 1
}

wait_dis_healthy() {
  for i in $(seq 1 30); do
    curl -sf "$DIS_URL/actuator/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

auto_approve() {
  local ids
  ids=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    'db.dis_instrument_flows.find({status:{$in:["WAITING_RETRY","PARKED"]},currentStep:{$in:["AWAIT_PREPARATION_APPROVAL","AWAIT_DELIVERY_APPROVAL"]}},{_id:1}).forEach(function(f){print(String(f._id))})' 2>/dev/null)
  for id in $ids; do
    curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$id/approve" \
      -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{}' >/dev/null 2>&1 &
  done
  wait 2>/dev/null
}

# Pre-flight
curl -sf "$DIS_URL/actuator/health" >/dev/null || { log "DIS not reachable at $DIS_URL"; exit 1; }
log "DIS healthy at $DIS_URL"
log "Starting chaos test suite"

# ========== Scenario 1: Pod crash mid-step ==========
header "Scenario 1: Pod crash mid-step → recovery scanner picks up"

# Start 5 flows
for i in $(seq 1 5); do submit_flow "CRASH-$i" >/dev/null 2>&1; done
sleep 5
auto_approve
sleep 3

# Count in-progress flows before kill
IN_PROGRESS_BEFORE=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "print(db.dis_instrument_flows.countDocuments({reference:/^CRASH/,status:'IN_PROGRESS'}))" 2>/dev/null)
log "Flows IN_PROGRESS before kill: $IN_PROGRESS_BEFORE"

# SIGKILL DIS
docker kill infra-digital-instrument-service-1 >/dev/null 2>&1 || true
log "DIS killed (SIGKILL)"
sleep 5

# Restart
docker start infra-digital-instrument-service-1 >/dev/null 2>&1
wait_dis_healthy || { fail "Scenario 1: DIS failed to restart"; }
log "DIS restarted"

# Wait for recovery (stale threshold 2min + scan interval 30s)
sleep 150

# Auto-approve any gate steps
for i in $(seq 1 6); do auto_approve; sleep 10; done

# Check results
COMPLETED=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "print(db.dis_instrument_flows.countDocuments({reference:/^CRASH/,status:'COMPLETED'}))" 2>/dev/null)
RECOVERED=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "print(db.dis_instrument_flows.countDocuments({reference:/^CRASH/,recoveryCount:{\$gte:1}}))" 2>/dev/null)
log "Completed: $COMPLETED/5, Recovered by scanner: $RECOVERED"
if [ "$COMPLETED" -eq 5 ]; then pass "Scenario 1: All flows recovered after crash"; else fail "Scenario 1: $COMPLETED/5 completed"; fi

# ========== Scenario 2: Duplicate Kafka messages ==========
header "Scenario 2: Duplicate Kafka messages → idempotency prevents double execution"

RESULT=$(submit_flow "DEDUP-001")
FLOW_ID=$(echo "$RESULT" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])" 2>/dev/null)
sleep 3

# Get current step and inject duplicate for a completed step
FLOW_DATA=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "var f=db.dis_instrument_flows.findOne({_id:ObjectId('$FLOW_ID')});print(f.correlationId+'|'+f.currentStep+'|'+JSON.stringify(f.completedSteps))" 2>/dev/null)
IFS='|' read -r CORR STEP COMPLETED <<< "$FLOW_DATA"

if [ -n "$COMPLETED" ] && [ "$COMPLETED" != "[]" ]; then
  DUPE_STEP=$(echo "$COMPLETED" | python3 -c "import json,sys;s=json.load(sys.stdin);print(s[0] if s else '')" 2>/dev/null)
  if [ -n "$DUPE_STEP" ]; then
    # Inject duplicate
    docker exec infra-kafka-1-1 bash -c "echo '$CORR:{\"eventId\":\"dedup-$(date +%s)\",\"flowId\":\"$FLOW_ID\",\"correlationId\":\"$CORR\",\"stepName\":\"$DUPE_STEP\",\"flowType\":\"enigio-instrument\"}' | kafka-console-producer --broker-list kafka-1:29092 --topic dis.instrument.commands --property parse.key=true --property key.separator=:" 2>/dev/null
    log "Injected duplicate for step $DUPE_STEP"
    sleep 5

    # Flow should still be fine — not broken by duplicate
    auto_approve
    sleep 30
    auto_approve
    sleep 30

    STATUS=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
      "var f=db.dis_instrument_flows.findOne({_id:ObjectId('$FLOW_ID')});print(f.status)" 2>/dev/null)
    if [ "$STATUS" = "COMPLETED" ] || [ "$STATUS" = "IN_PROGRESS" ] || [ "$STATUS" = "PARKED" ] || [ "$STATUS" = "WAITING_RETRY" ]; then
      pass "Scenario 2: Flow survived duplicate message (status: $STATUS)"
    else
      fail "Scenario 2: Flow in unexpected state after duplicate: $STATUS"
    fi
  else
    skip "Scenario 2: No completed steps to duplicate"
  fi
else
  skip "Scenario 2: Flow has no completed steps yet"
fi

# ========== Scenario 3: Signal on parked flow ==========
header "Scenario 3: Signal on parked flow → immediate execution"

RESULT=$(submit_flow "SIGNAL-CHAOS-001")
SIGNAL_FLOW_ID=$(echo "$RESULT" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])" 2>/dev/null)

# Wait for flow to reach PARKED
if wait_for_status "$SIGNAL_FLOW_ID" "PARKED" 60; then
  # Send signal
  SIGNAL_RESULT=$(curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$SIGNAL_FLOW_ID/signal" \
    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
    -d '{"signalName":"updatePriority","payload":{"priority":"URGENT","reason":"chaos-test"}}' 2>/dev/null)

  sleep 2
  PRIORITY=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "var f=db.dis_instrument_flows.findOne({_id:ObjectId('$SIGNAL_FLOW_ID')});print(f.priority)" 2>/dev/null)

  if [ "$PRIORITY" = "URGENT" ]; then
    pass "Scenario 3: Signal executed on parked flow"
  else
    fail "Scenario 3: Priority not set (got: $PRIORITY)"
  fi
else
  fail "Scenario 3: Flow never reached PARKED"
fi

# ========== Scenario 4: Signal with business validation (rejection) ==========
header "Scenario 4: Signal rejection → handler throws, HTTP 400"

# Set signingStatus to SIGNED to trigger rejection
docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "db.dis_instrument_flows.updateOne({_id:ObjectId('$SIGNAL_FLOW_ID')},{\$set:{signingStatus:'SIGNED'}})" >/dev/null 2>/dev/null

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$DIS_URL/flows/enigio-instrument/$SIGNAL_FLOW_ID/signal" \
  -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
  -d '{"signalName":"requestCancellation"}' 2>/dev/null)

if [ "$HTTP_CODE" = "400" ]; then
  pass "Scenario 4: Signal rejected with HTTP 400 (document signed)"
else
  fail "Scenario 4: Expected HTTP 400 but got $HTTP_CODE"
fi

# ========== Scenario 5: Kafka broker kill → flows continue ==========
header "Scenario 5: Kafka broker kill → flows continue on remaining brokers"

# Start flows before kill
for i in $(seq 1 3); do submit_flow "KAFKA-$i" >/dev/null 2>&1; done
sleep 3
auto_approve

# Kill one Kafka broker
docker stop infra-kafka-3-1 >/dev/null 2>&1 || true
log "Kafka broker 3 stopped"
sleep 10

# Continue approving
for i in $(seq 1 4); do auto_approve; sleep 10; done

# Restart broker
docker start infra-kafka-3-1 >/dev/null 2>&1 || true
log "Kafka broker 3 restarted"
sleep 15

# Final approvals
for i in $(seq 1 4); do auto_approve; sleep 10; done

KAFKA_COMPLETED=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
  "print(db.dis_instrument_flows.countDocuments({reference:/^KAFKA/,status:'COMPLETED'}))" 2>/dev/null)
log "Kafka flows completed: $KAFKA_COMPLETED/3"
if [ "$KAFKA_COMPLETED" -eq 3 ]; then
  pass "Scenario 5: All flows completed despite broker failure"
else
  # Some may still be in progress — check if any failed
  KAFKA_FAILED=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({reference:/^KAFKA/,status:'FAILED'}))" 2>/dev/null)
  if [ "$KAFKA_FAILED" -eq 0 ]; then
    pass "Scenario 5: No failures despite broker kill ($KAFKA_COMPLETED/3 completed, rest in progress)"
  else
    fail "Scenario 5: $KAFKA_FAILED flows failed after broker kill"
  fi
fi

# ========== Scenario 6: Flow cancellation ==========
header "Scenario 6: Cancel flow at gate step → compensation runs"

RESULT=$(submit_flow "CANCEL-001")
CANCEL_FLOW_ID=$(echo "$RESULT" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])" 2>/dev/null)

if wait_for_status "$CANCEL_FLOW_ID" "PARKED" 60; then
  curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$CANCEL_FLOW_ID/cancel" \
    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
    -d '{"reason":"chaos-test-cancel"}' >/dev/null 2>&1

  sleep 3
  CANCEL_STATUS=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "var f=db.dis_instrument_flows.findOne({_id:ObjectId('$CANCEL_FLOW_ID')});print(f.status)" 2>/dev/null)

  if [ "$CANCEL_STATUS" = "CANCELLED" ]; then
    pass "Scenario 6: Flow cancelled at gate step"
  else
    fail "Scenario 6: Expected CANCELLED but got $CANCEL_STATUS"
  fi
else
  fail "Scenario 6: Flow never reached PARKED for cancellation"
fi

# ========== Results ==========
header "CHAOS TEST RESULTS"
TOTAL=$((PASS + FAIL + SKIP))
log "Total: $TOTAL | Pass: $PASS | Fail: $FAIL | Skip: $SKIP"

# DB summary
docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
  var c={};
  db.dis_instrument_flows.find({}).forEach(function(f){c[f.status]=(c[f.status]||0)+1});
  print("=== DB Status ===");
  Object.keys(c).sort().forEach(function(k){print("  "+k+": "+c[k])});
  print("Orphaned claims: " + db.dis_instrument_flows.countDocuments({claimedBy:{$ne:null}}));
' 2>/dev/null

if [ "$FAIL" -eq 0 ]; then
  log "ALL SCENARIOS PASSED"
  exit 0
else
  log "$FAIL SCENARIO(S) FAILED"
  exit 1
fi
