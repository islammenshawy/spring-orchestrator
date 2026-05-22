#!/usr/bin/env bash
set -euo pipefail

DURATION=${DURATION:-5}
WAVE_SIZE=${WAVE_SIZE:-10}
WAVE_INTERVAL=${WAVE_INTERVAL:-15}
DIS_URL=${DIS_URL:-http://localhost:8090}
API_KEY=${API_KEY:-soak-test-key}
DRAIN_TIMEOUT=120
CHAOS=${CHAOS:-0}           # Set CHAOS=1 to enable chaos scenarios
POD_KILL_WAVE=${POD_KILL_WAVE:-0}  # Wave number to kill DIS-1 (0=auto at 40%)
DEDUP_INTERVAL=${DEDUP_INTERVAL:-5} # Inject duplicates every N waves

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Pre-flight
curl -sf "$DIS_URL/actuator/health" >/dev/null || { log "DIS not reachable at $DIS_URL"; exit 1; }
log "DIS healthy at $DIS_URL"

# Clean MongoDB
docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
  db.dis_instrument_flows.deleteMany({});
  db.orchestrator_outbox.deleteMany({});
  db.orchestrator_step_log.deleteMany({});
  db.orchestrator_processed_events.deleteMany({});
  db.orchestrator_consumer_offsets.deleteMany({});
  db.dis_additional_documents.deleteMany({});
' 2>/dev/null
log "MongoDB cleaned"

# Reset Kafka consumer offsets to latest (skip stale messages from seed data / previous runs)
for GROUP in digital-instrument-service-executor digital-instrument-service-dlt \
             digital-instrument-service-executor-dlt \
             digital-instrument-service-executor-retry-0 \
             digital-instrument-service-executor-retry-1 \
             digital-instrument-service-executor-retry-2; do
  docker exec infra-kafka-1-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 \
    --group "$GROUP" --reset-offsets --to-latest --all-topics --execute 2>/dev/null >/dev/null
done
log "Kafka consumer offsets reset to latest"

# Instrument types and doc codes
TYPES=("PROMISSORY_NOTE" "BILL_OF_EXCHANGE" "BILL_OF_LADING")
CODES=("NEG" "NON_NEG")

submit_flow() {
  local idx=$1
  local type=${TYPES[$((idx % ${#TYPES[@]}))]}
  local code=${CODES[$((idx % ${#CODES[@]}))]}
  local corr=$(python3 -c "import uuid; print(uuid.uuid4())")
  local ref="SOAK-$(printf '%04d' $idx)"

  curl -sf -X POST "$DIS_URL/flows/enigio-instrument" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d "{
      \"correlationId\": \"$corr\",
      \"reference\": \"$ref\",
      \"title\": \"Soak Test $ref\",
      \"instrumentType\": \"$type\",
      \"documentCode\": \"$code\",
      \"content\": \"Test content for $ref\",
      \"signers\": [
        {\"name\":\"Alice Smith\",\"email\":\"alice@test.com\",\"phone\":\"+46701234567\",\"capacity\":\"CEO\",\"organisation\":\"Test AB\",\"order\":1},
        {\"name\":\"Bob Jones\",\"email\":\"bob@test.com\",\"phone\":\"+46709876543\",\"capacity\":\"CFO\",\"organisation\":\"Test AB\",\"order\":2}
      ],
      \"recipient\": {\"name\":\"Charlie Brown\",\"email\":\"charlie@test.com\"}
    }" >/dev/null 2>&1 &
}

count_done() {
  docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({status:{\$in:['COMPLETED','FAILED','CANCELLED']}}))" 2>/dev/null
}

count_total() {
  docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({}))" 2>/dev/null
}

# Auto-approve all flows waiting at gate steps
auto_approve() {
  local ids
  ids=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval 'db.dis_instrument_flows.find({status:"WAITING_RETRY",currentStep:{$in:["AWAIT_PREPARATION_APPROVAL","AWAIT_DELIVERY_APPROVAL"]}},{_id:1}).forEach(function(f){print(String(f._id))})' 2>/dev/null)

  for id in $ids; do
    curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$id/approve" \
      -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '{}' >/dev/null 2>&1 &
  done
  wait 2>/dev/null
}

# ========== Chaos functions ==========

CHAOS_DEDUP_COUNT=0
CHAOS_POD_KILLED=0
CHAOS_STUCK_COUNT=0

# Inject duplicate Kafka messages for completed steps
chaos_inject_duplicates() {
  [ "$CHAOS" -eq 0 ] && return
  local count=0
  local flows
  flows=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
    db.dis_instrument_flows.find(
      {status:"WAITING_RETRY", completedSteps:{$exists:true,$ne:[]}},
      {_id:1, correlationId:1, currentStep:1, flowType:1, completedSteps:1}
    ).limit(3).forEach(function(f){
      var step = f.completedSteps[0];
      print(f._id + "|" + f.correlationId + "|" + step + "|" + (f.flowType || "enigio-instrument"));
    })
  ' 2>/dev/null)

  for line in $flows; do
    IFS='|' read -r fid corr step ftype <<< "$line"
    [ -z "$fid" ] && continue
    local eid="chaos-dedup-$(date +%s)-$RANDOM"
    docker exec infra-kafka-1-1 bash -c "echo '{\"eventId\":\"$eid\",\"flowId\":\"$fid\",\"correlationId\":\"$corr\",\"stepName\":\"$step\",\"flowType\":\"$ftype\"}' | kafka-console-producer --broker-list kafka-1:29092 --topic dis.instrument.commands --property parse.key=true --property key.separator=: <<< \"$corr:{\\\"eventId\\\":\\\"$eid\\\",\\\"flowId\\\":\\\"$fid\\\",\\\"correlationId\\\":\\\"$corr\\\",\\\"stepName\\\":\\\"$step\\\",\\\"flowType\\\":\\\"$ftype\\\"}\"" 2>/dev/null &
    count=$((count + 1))
  done
  wait 2>/dev/null
  if [ $count -gt 0 ]; then
    CHAOS_DEDUP_COUNT=$((CHAOS_DEDUP_COUNT + count))
    log "  [CHAOS] Injected $count duplicate messages (total: $CHAOS_DEDUP_COUNT)"
  fi
}

# Kill DIS-1 to simulate pod crash, leaving flows stuck IN_PROGRESS
chaos_kill_pod() {
  [ "$CHAOS" -eq 0 ] && return
  [ "$CHAOS_POD_KILLED" -eq 1 ] && return
  CHAOS_POD_KILLED=1

  # Count flows currently IN_PROGRESS — these will get stuck
  CHAOS_STUCK_COUNT=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({status:'IN_PROGRESS'}))" 2>/dev/null)

  log "  [CHAOS] Killing DIS-1 (${CHAOS_STUCK_COUNT} flows IN_PROGRESS will need recovery)"
  docker stop infra-digital-instrument-service-1 >/dev/null 2>&1 || true
}

# Restart DIS-1 after kill
chaos_restart_pod() {
  [ "$CHAOS" -eq 0 ] && return
  [ "$CHAOS_POD_KILLED" -eq 0 ] && return
  log "  [CHAOS] Restarting DIS-1"
  docker start infra-digital-instrument-service-1 >/dev/null 2>&1 || true
  # Wait for health
  for i in $(seq 1 30); do
    curl -sf http://localhost:8087/actuator/health >/dev/null 2>&1 && break
    sleep 2
  done
  log "  [CHAOS] DIS-1 back online"
}

# Report chaos metrics
chaos_report() {
  [ "$CHAOS" -eq 0 ] && return
  echo ""
  log "=== Chaos Report ==="
  log "  Duplicate messages injected: $CHAOS_DEDUP_COUNT"
  log "  Pod killed: $([ $CHAOS_POD_KILLED -eq 1 ] && echo 'YES (DIS-1)' || echo 'NO')"
  log "  Flows stuck at pod kill: $CHAOS_STUCK_COUNT"

  # Check recovery metrics
  local recovered
  recovered=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({recoveryCount:{\$gte:1}}))" 2>/dev/null)
  log "  Flows recovered by scanner: $recovered"

  # Check if any flow was claimed more than once (duplicate claiming)
  local multi_recovery
  multi_recovery=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({recoveryCount:{\$gt:1}}))" 2>/dev/null)
  log "  Flows with recoveryCount > 1: $multi_recovery (should be 0 — no duplicate claiming)"

  # Check processed events vs expected (dedup effectiveness)
  local processed
  processed=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.orchestrator_processed_events.countDocuments({}))" 2>/dev/null)
  log "  Total processed events: $processed (includes $CHAOS_DEDUP_COUNT chaos duplicates)"

  # Check recovery scanner logs from both pods
  local dis1_claims dis2_claims
  dis1_claims=$(docker logs infra-digital-instrument-service-1 2>&1 | grep -c "\[Recovery\] Claimed" || echo 0)
  dis2_claims=$(docker logs infra-digital-instrument-service-2-1 2>&1 | grep -c "\[Recovery\] Claimed" || echo 0)
  log "  Recovery claims: DIS-1=$dis1_claims, DIS-2=$dis2_claims"

  # Check orphan cleanup
  local orphans
  orphans=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({claimedBy:{\$ne:null}}))" 2>/dev/null)
  log "  Orphaned claims remaining: $orphans (should be 0)"
}

# Submit waves
END=$((SECONDS + DURATION * 60))
WAVE=0
TOTAL_SUBMITTED=0

log "Starting ${DURATION}m test — $WAVE_SIZE flows every ${WAVE_INTERVAL}s"
[ "$CHAOS" -eq 1 ] && log "CHAOS MODE ENABLED — duplicate injection every ${DEDUP_INTERVAL} waves, pod kill at 40%"

# Calculate pod kill wave (40% through test)
TOTAL_WAVES=$(( DURATION * 60 / WAVE_INTERVAL ))
[ "$POD_KILL_WAVE" -eq 0 ] && POD_KILL_WAVE=$(( TOTAL_WAVES * 40 / 100 ))

while [ $SECONDS -lt $END ]; do
  WAVE=$((WAVE + 1))
  DONE=$(count_done)
  TOTAL=$(count_total)
  REMAINING=$(( (END - SECONDS) / 60 ))
  log "Wave $WAVE — submitting $WAVE_SIZE flows ($DONE/$TOTAL done, ${REMAINING}m remaining)"

  for i in $(seq 1 $WAVE_SIZE); do
    submit_flow $((TOTAL_SUBMITTED + i))
  done
  wait
  TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + WAVE_SIZE))

  # Auto-approve gate steps between waves
  auto_approve

  # Chaos: inject duplicate messages every N waves
  if [ "$CHAOS" -eq 1 ] && [ $((WAVE % DEDUP_INTERVAL)) -eq 0 ]; then
    chaos_inject_duplicates
  fi

  # Chaos: kill DIS-1 at configured wave
  if [ "$CHAOS" -eq 1 ] && [ "$WAVE" -eq "$POD_KILL_WAVE" ]; then
    chaos_kill_pod
  fi

  # Chaos: restart pod at 70% through test
  RESTART_WAVE=$(( TOTAL_WAVES * 70 / 100 ))
  if [ "$CHAOS" -eq 1 ] && [ "$WAVE" -eq "$RESTART_WAVE" ] && [ "$CHAOS_POD_KILLED" -eq 1 ]; then
    chaos_restart_pod
  fi

  sleep $WAVE_INTERVAL
done

# Drain
TOTAL=$(count_total)
log "All waves done ($TOTAL_SUBMITTED submitted). Draining (max ${DRAIN_TIMEOUT}s)..."
DRAIN_END=$((SECONDS + DRAIN_TIMEOUT))
while [ $SECONDS -lt $DRAIN_END ]; do
  DONE=$(count_done)
  log "  Draining: $DONE/$TOTAL done"
  [ "$DONE" -ge "$TOTAL" ] && break
  auto_approve
  sleep 5
done

# Results
DONE=$(count_done)
log "Final: $DONE/$TOTAL done"

docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
  print("=== Flow Status ===");
  db.dis_instrument_flows.aggregate([{$group:{_id:"$status",count:{$sum:1}}}]).forEach(r => print("  " + r._id + ": " + r.count));
  print("=== Step Log ===");
  db.orchestrator_step_log.aggregate([
    {$group:{_id:{step:"$stepName",status:"$status"},count:{$sum:1}}},
    {$sort:{"_id.step":1,"_id.status":1}}
  ]).forEach(r => print("  " + r._id.step + " | " + r._id.status + " | " + r.count));
  print("=== Processed Events: " + db.orchestrator_processed_events.countDocuments({}) + " ===");
' 2>/dev/null

log "=== Kafka Offsets ==="
docker exec infra-kafka-1-1 bash -c '
T="dis.instrument.commands"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.commands.replies"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.commands-dlt"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.notifications"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
'

log "=== Webhook Registrations ==="
docker logs infra-mock-vendor-1 2>&1 | grep "Webhook registered" | tail -3
WEBHOOK_FIRED=$(docker logs infra-mock-vendor-1 2>&1 | grep -c "Webhook fired")
log "Total webhooks fired: $WEBHOOK_FIRED"

chaos_report

log "DONE"
