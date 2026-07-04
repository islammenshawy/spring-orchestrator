#!/usr/bin/env bash
# Lossless-architecture chaos scenarios — complements chaos-test.sh with the crash
# states its Scenario 1 does NOT exercise. Each forces a precise "crashed" state in
# MongoDB, lets the recovery scanner run, and asserts NO flow/step/rollback is lost.
#
# Parameterized for both stacks:
#   single-cluster:  DIS_URL=http://localhost:8090 DIS_CONTAINER=infra-digital-instrument-service-1 KAFKA_CONTAINER=infra-kafka-1-1
#   multi-cluster:   (defaults below) DIS_URL=http://localhost:8087 DIS_CONTAINER=infra-dis-failover-1 KAFKA_CONTAINER=infra-kafka-a-1
#
# Requires the target stack to have a low execution-claim TTL for prompt reaping,
# e.g. ORCHESTRATOR_RECOVERY_EXECUTION_CLAIM_TTL_MINUTES=2 (set on the failover DIS).
set -uo pipefail

DIS_URL=${DIS_URL:-http://localhost:8087}
API_KEY=${API_KEY:-soak-test-key}
MONGO_CONTAINER=${MONGO_CONTAINER:-infra-mongodb-1}
DB=${DB:-digital_instrument_service}
COLL=dis_instrument_flows
PASS=0; FAIL=0

log() { echo "[$(date +%H:%M:%S)] $*"; }
header() { echo ""; echo "========== $1 =========="; }
pass() { PASS=$((PASS + 1)); log "✅ PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); log "❌ FAIL: $1"; }
mongo_eval() { docker exec "$MONGO_CONTAINER" mongosh --quiet "$DB" --eval "$1" 2>/dev/null; }

submit_flow() {
  local ref=$1
  curl -sf -X POST "$DIS_URL/flows/enigio-instrument" \
    -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" \
    -d "{\"correlationId\":\"lossless-$ref-$(date +%s%N)\",\"reference\":\"$ref\",\"title\":\"Lossless $ref\",\"instrumentType\":\"PROMISSORY_NOTE\",\"documentCode\":\"NEG\",\"signers\":[{\"name\":\"A\",\"email\":\"a@t.com\",\"phone\":\"+46700000001\",\"capacity\":\"CEO\",\"organisation\":\"T\",\"order\":1}],\"recipient\":{\"name\":\"B\",\"email\":\"b@t.com\"}}" 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])" 2>/dev/null
}

# wait until a flow has completed at least 1 step (so it has a real currentStep past INIT)
wait_started() {
  local fid=$1 deadline=$((SECONDS + 90))
  while [ $SECONDS -lt $deadline ]; do
    local n; n=$(mongo_eval "var f=db.$COLL.findOne({_id:ObjectId('$fid')});print(f?f.completedSteps.length:0)")
    [ "${n:-0}" -ge 1 ] 2>/dev/null && return 0
    sleep 2
  done
  return 1
}

curl -sf "$DIS_URL/actuator/health" >/dev/null || { log "DIS not reachable at $DIS_URL"; exit 1; }
log "DIS healthy at $DIS_URL — starting lossless chaos suite"

# ========== A: crash WHILE holding executingStep (F1 / GLM-5.2 LIB-6) ==========
# chaos-test.sh Scenario 1 leaves executingStep=null (benign between-steps crash). This forces
# the real mid-step-execution crash: a dead pod's executingStep must be reaped so the flow resumes,
# NOT burn recoveryCount to force-compensation.
header "A: pod crash holding executingStep → reaped, flow resumes (not force-failed)"
FID_A=$(submit_flow "LOSSLESS-A")
if [ -n "$FID_A" ] && wait_started "$FID_A"; then
  mongo_eval "db.$COLL.updateOne({_id:ObjectId('$FID_A')},{\$set:{status:'IN_PROGRESS',currentStep:'REGISTER_DOCUMENT',executingStep:'REGISTER_DOCUMENT',executingPod:'dead-pod-A',updatedAt:new Date(Date.now()-5*60*1000),claimedBy:null,claimedAt:null},\$pull:{completedSteps:'REGISTER_DOCUMENT'}})" >/dev/null
  log "  forced crash-mid-step state (executingStep=REGISTER_DOCUMENT, pod=dead-pod-A, 5min stale)"
  ok=false
  for i in $(seq 1 30); do
    read -r EXECPOD EXECSTEP STATUS <<<"$(mongo_eval "var f=db.$COLL.findOne({_id:ObjectId('$FID_A')});print((f.executingPod||'null')+' '+(f.executingStep||'null')+' '+f.status)")"
    if [ "$EXECPOD" != "dead-pod-A" ]; then ok=true; break; fi
    sleep 3
  done
  if [ "$ok" = true ]; then
    pass "A: dead pod's executingStep reaped (execPod=$EXECPOD, execStep=$EXECSTEP, status=$STATUS) — flow recoverable"
  else
    fail "A: executingStep still held by dead-pod-A after 90s — flow permanently stuck (F1 regression)"
  fi
else
  fail "A: flow did not start"
fi

# ========== C: crash DURING compensation → compensation resumes to terminal ==========
# A pod that dies mid-rollback leaves status=COMPENSATING. recoverStuckCompensation must resume it
# so the saga reaches a terminal state — no partial/abandoned rollback.
header "C: crash during compensation → resumes to terminal (no partial rollback)"
FID_C=$(submit_flow "LOSSLESS-C")
if [ -n "$FID_C" ] && wait_started "$FID_C"; then
  mongo_eval "db.$COLL.updateOne({_id:ObjectId('$FID_C')},{\$set:{status:'COMPENSATING',updatedAt:new Date(Date.now()-5*60*1000),claimedBy:null,claimedAt:null,errorMessage:'chaos: crashed mid-compensation'}})" >/dev/null
  log "  forced stuck COMPENSATING (5min stale)"
  ok=false
  for i in $(seq 1 30); do
    STATUS=$(mongo_eval "var f=db.$COLL.findOne({_id:ObjectId('$FID_C')});print(f.status)")
    case "$STATUS" in FAILED|COMPENSATION_FAILED) ok=true; break;; esac
    sleep 3
  done
  if [ "$ok" = true ]; then
    pass "C: compensation resumed to terminal status=$STATUS (no stuck COMPENSATING)"
  else
    fail "C: still COMPENSATING after 90s (status=$STATUS) — rollback abandoned"
  fi
else
  fail "C: flow did not start"
fi

# ========== F: lost reply (completed-but-not-advanced) → recovery advances it ==========
# A lost reply leaves currentStep IN completedSteps but never advanced. recoverCompletedButNotAdvanced
# must move it forward so the flow is not permanently stalled.
header "F: lost reply (currentStep already completed) → recovery advances (no stall)"
FID_F=$(submit_flow "LOSSLESS-F")
if [ -n "$FID_F" ] && wait_started "$FID_F"; then
  # pin currentStep to CREATE_DRAFT and ensure it's in completedSteps → "completed but not advanced"
  mongo_eval "db.$COLL.updateOne({_id:ObjectId('$FID_F')},{\$set:{status:'IN_PROGRESS',currentStep:'CREATE_DRAFT',executingStep:null,updatedAt:new Date(Date.now()-5*60*1000),claimedBy:null,claimedAt:null},\$addToSet:{completedSteps:'CREATE_DRAFT'}})" >/dev/null
  log "  forced completed-but-not-advanced (currentStep=CREATE_DRAFT ∈ completedSteps, 5min stale)"
  ok=false
  for i in $(seq 1 30); do
    STEP=$(mongo_eval "var f=db.$COLL.findOne({_id:ObjectId('$FID_F')});print(f.currentStep)")
    if [ "$STEP" != "CREATE_DRAFT" ]; then ok=true; break; fi
    sleep 3
  done
  if [ "$ok" = true ]; then
    pass "F: recovery advanced past the completed step (currentStep=$STEP) — no permanent stall"
  else
    fail "F: still stuck at CREATE_DRAFT after 90s — lost-reply stall not recovered"
  fi
else
  fail "F: flow did not start"
fi

echo ""
echo "========== LOSSLESS CHAOS RESULTS: $PASS passed, $FAIL failed =========="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
