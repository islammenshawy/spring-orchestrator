#!/usr/bin/env bash
set -uo pipefail

# ============================================================
# Multi-DC Failover Stress Test
# Submits flows continuously, kills DC-A mid-flight, verifies
# DIS fails over to DC-B and all flows complete.
# ============================================================

DIS_URL=${DIS_URL:-http://localhost:8087}
API_KEY=${API_KEY:-soak-test-key}
DURATION=${DURATION:-15}          # minutes
WAVE_SIZE=${WAVE_SIZE:-5}
WAVE_INTERVAL=${WAVE_INTERVAL:-15}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-300}
FAILOVER_AT_PCT=${FAILOVER_AT_PCT:-40}  # kill DC-A at N% through test
METRICS_INTERVAL=${METRICS_INTERVAL:-5}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/collect-metrics.sh" 2>/dev/null || true

log() { echo "[$(date +%H:%M:%S)] $*"; }

TYPES=("PROMISSORY_NOTE" "BILL_OF_EXCHANGE" "BILL_OF_LADING")
CODES=("NEG" "NON_NEG")

submit_flow() {
  local idx=$1
  local type=${TYPES[$((idx % ${#TYPES[@]}))]}
  local code=${CODES[$((idx % ${#CODES[@]}))]}
  local corr=$(python3 -c "import uuid; print(uuid.uuid4())")
  local ref="FO-$(printf '%04d' $idx)"

  curl -sf -X POST "$DIS_URL/flows/enigio-instrument" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d "{
      \"correlationId\": \"$corr\",
      \"reference\": \"$ref\",
      \"title\": \"Failover Test $ref\",
      \"instrumentType\": \"$type\",
      \"documentCode\": \"$code\",
      \"content\": \"Failover test content for $ref\",
      \"signers\": [
        {\"name\":\"Alice\",\"email\":\"alice@test.com\",\"phone\":\"+46701234567\",\"capacity\":\"CEO\",\"organisation\":\"Test AB\",\"order\":1},
        {\"name\":\"Bob\",\"email\":\"bob@test.com\",\"phone\":\"+46709876543\",\"capacity\":\"CFO\",\"organisation\":\"Test AB\",\"order\":2}
      ],
      \"recipient\": {\"name\":\"Charlie\",\"email\":\"charlie@test.com\"}
    }" >/dev/null 2>&1 &
}

count_by_status() {
  docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({status:'$1'}))" 2>/dev/null
}

count_total() {
  docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({}))" 2>/dev/null
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

capture_metrics() {
  local label=$1
  local completed=$(count_by_status "COMPLETED")
  local failed=$(count_by_status "FAILED")
  local in_progress=$(count_by_status "IN_PROGRESS")
  local parked=$(count_by_status "PARKED")
  local waiting=$(count_by_status "WAITING_RETRY")
  local total=$(count_total)
  local dc_health=$(curl -sf "$DIS_URL/actuator/dc-health" 2>/dev/null || echo "unreachable")
  local active_dc=$(echo "$dc_health" | python3 -c "import json,sys; print(json.load(sys.stdin).get('activeDc','?'))" 2>/dev/null || echo "?")
  local supervisor=$(echo "$dc_health" | python3 -c "import json,sys; print(json.load(sys.stdin).get('supervisorState','?'))" 2>/dev/null || echo "?")

  log "[$label] DC=$active_dc state=$supervisor | total=$total completed=$completed failed=$failed in_progress=$in_progress parked=$parked waiting=$waiting"

  # Kafka lag
  local lag=$(docker exec infra-kafka-a-1 kafka-consumer-groups --bootstrap-server kafka-a:29092 \
    --group digital-instrument-service-executor --describe 2>/dev/null | awk 'NR>1{s+=$6}END{print s+0}' || echo "?")
  log "[$label] Kafka-A lag=$lag"

  # DIS container metrics
  local dis_stats=$(docker stats --no-stream --format "CPU={{.CPUPerc}} MEM={{.MemUsage}}" infra-dis-failover-1 2>/dev/null || echo "unavailable")
  log "[$label] DIS: $dis_stats"

  # Outbox + retry
  local outbox=$(docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.orchestrator_outbox.countDocuments({published:false}))" 2>/dev/null || echo "?")
  log "[$label] Outbox pending=$outbox"
}

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
' 2>/dev/null
log "MongoDB cleaned"

# Reset Kafka consumer offsets
for GROUP in digital-instrument-service-executor digital-instrument-service-orchestrator; do
  docker exec infra-kafka-a-1 kafka-consumer-groups --bootstrap-server kafka-a:29092 \
    --group "$GROUP" --reset-offsets --to-latest --all-topics --execute 2>/dev/null >/dev/null
done
log "Kafka offsets reset"

capture_metrics "BASELINE"
POLICY=$(curl -sf "$DIS_URL/actuator/dc-health" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('replicationPolicy','?'))" 2>/dev/null)
CYCLE_INTERVAL=${CYCLE_INTERVAL:-5}  # minutes between kill/restart cycles
KILL_COUNT=0

log "=========================================="
log "Starting ${DURATION}m failover stress test"
log "Mode: $POLICY | Wave: $WAVE_SIZE flows every ${WAVE_INTERVAL}s"
log "Failover cycle every ${CYCLE_INTERVAL}m (kill → wait → restart → repeat)"
log "=========================================="

END=$((SECONDS + DURATION * 60))
WAVE=0
TOTAL_SUBMITTED=0
TOTAL_WAVES=$(( DURATION * 60 / WAVE_INTERVAL ))
DC_A_DOWN=0
NEXT_CYCLE=$((SECONDS + CYCLE_INTERVAL * 60))

while [ $SECONDS -lt $END ]; do
  WAVE=$((WAVE + 1))
  REMAINING=$(( (END - SECONDS) / 60 ))
  COMPLETED=$(count_by_status "COMPLETED")
  TOTAL=$(count_total)
  log "Wave $WAVE/$TOTAL_WAVES — submitting $WAVE_SIZE flows ($COMPLETED/$TOTAL done, ${REMAINING}m remaining)"

  for i in $(seq 1 $WAVE_SIZE); do
    submit_flow $((TOTAL_SUBMITTED + i))
  done
  wait
  TOTAL_SUBMITTED=$((TOTAL_SUBMITTED + WAVE_SIZE))

  auto_approve

  # Periodic metrics
  if [ $((WAVE % METRICS_INTERVAL)) -eq 0 ]; then
    capture_metrics "WAVE-$WAVE"
  fi

  # Failover cycling: every CYCLE_INTERVAL minutes, toggle DC-A up/down
  if [ $SECONDS -ge $NEXT_CYCLE ]; then
    NEXT_CYCLE=$((SECONDS + CYCLE_INTERVAL * 60))
    if [ "$DC_A_DOWN" -eq 0 ]; then
      DC_A_DOWN=1
      KILL_COUNT=$((KILL_COUNT + 1))
      log "=========================================="
      log "CYCLE $KILL_COUNT: KILLING DC-A (kafka-a) at wave $WAVE"
      capture_metrics "PRE-KILL-$KILL_COUNT"
      docker stop infra-kafka-a-1 >/dev/null 2>&1
      log "kafka-a stopped — waiting for failover..."
      for i in $(seq 1 60); do
        active=$(curl -sf "$DIS_URL/actuator/dc-health" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('activeDc','?'))" 2>/dev/null || echo "?")
        state=$(curl -sf "$DIS_URL/actuator/dc-health" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('supervisorState','?'))" 2>/dev/null || echo "?")
        [ "$active" = "dcb" ] && { log "FAILOVER → DC-B (state=$state, ${i}s)"; break; }
        sleep 2
      done
      capture_metrics "POST-KILL-$KILL_COUNT"
      log "=========================================="
    else
      DC_A_DOWN=0
      log "=========================================="
      log "CYCLE $KILL_COUNT: RESTARTING DC-A (kafka-a) at wave $WAVE"
      docker start infra-kafka-a-1 >/dev/null 2>&1
      sleep 30  # Let kafka-a rejoin cluster
      capture_metrics "POST-RESTART-$KILL_COUNT"
      log "DC-A back online"
      log "=========================================="
    fi
  fi

  sleep $WAVE_INTERVAL
done

# Drain
TOTAL=$(count_total)
log "All waves done ($TOTAL_SUBMITTED submitted). Draining (max ${DRAIN_TIMEOUT}s)..."
DRAIN_END=$((SECONDS + DRAIN_TIMEOUT))
while [ $SECONDS -lt $DRAIN_END ]; do
  COMPLETED=$(count_by_status "COMPLETED")
  FAILED=$(count_by_status "FAILED")
  DONE=$((COMPLETED + FAILED))
  log "  Draining: $DONE/$TOTAL done (completed=$COMPLETED failed=$FAILED)"
  [ "$DONE" -ge "$TOTAL" ] && break
  auto_approve
  sleep 5
done

# Final results
capture_metrics "FINAL"

log "=========================================="
log "RESULTS"
log "=========================================="
docker exec infra-mongodb-1 mongosh --quiet digital_instrument_service --eval '
  print("=== Flow Status ===");
  db.dis_instrument_flows.aggregate([{$group:{_id:"$status",count:{$sum:1}}}]).forEach(r => print("  " + r._id + ": " + r.count));
  print("Total: " + db.dis_instrument_flows.countDocuments({}));
  print("=== Step Log ===");
  db.orchestrator_step_log.aggregate([
    {$group:{_id:{step:"$stepName",status:"$status"},count:{$sum:1}}},
    {$sort:{"_id.step":1,"_id.status":1}}
  ]).forEach(r => print("  " + r._id.step + " | " + r._id.status + " | " + r.count));
  print("Outbox pending: " + db.orchestrator_outbox.countDocuments({published:false}));
  print("DLT events: " + db.orchestrator_outbox.countDocuments({deadLettered:true}));
  print("Orphaned claims: " + db.dis_instrument_flows.countDocuments({claimedBy:{$ne:null}}));
  print("Executing steps: " + db.dis_instrument_flows.countDocuments({executingStep:{$ne:null}}));
' 2>/dev/null

COMPLETED=$(count_by_status "COMPLETED")
FAILED=$(count_by_status "FAILED")
TOTAL=$(count_total)
log "Final: $COMPLETED completed, $FAILED failed out of $TOTAL total"
log "Success rate: $(( COMPLETED * 100 / TOTAL ))%"

# Restart kafka-a if still down
if [ "$DC_A_DOWN" -eq 1 ]; then
  log "Restarting kafka-a..."
  docker start infra-kafka-a-1 >/dev/null 2>&1
fi

log "DONE"
