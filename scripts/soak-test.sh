#!/usr/bin/env bash
set -euo pipefail

DURATION=${DURATION:-5}
WAVE_SIZE=${WAVE_SIZE:-10}
WAVE_INTERVAL=${WAVE_INTERVAL:-15}
DIS_URL=${DIS_URL:-http://localhost:8090}
DRAIN_TIMEOUT=120

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Pre-flight
curl -sf "$DIS_URL/actuator/health" >/dev/null || { log "DIS not reachable at $DIS_URL"; exit 1; }
log "DIS healthy at $DIS_URL"

# Clean MongoDB
docker exec spring_orchestrator-mongodb-1 mongosh --quiet digital_instrument_service --eval '
  db.dis_instrument_flows.deleteMany({});
  db.orchestrator_outbox.deleteMany({});
  db.orchestrator_step_log.deleteMany({});
  db.orchestrator_processed_events.deleteMany({});
  db.orchestrator_consumer_offsets.deleteMany({});
  db.dis_additional_documents.deleteMany({});
' 2>/dev/null
log "MongoDB cleaned"

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
  docker exec spring_orchestrator-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({status:{\$in:['COMPLETED','FAILED','CANCELLED']}}))" 2>/dev/null
}

count_total() {
  docker exec spring_orchestrator-mongodb-1 mongosh --quiet digital_instrument_service --eval \
    "print(db.dis_instrument_flows.countDocuments({}))" 2>/dev/null
}

# Auto-approve all flows waiting at gate steps
auto_approve() {
  local ids
  ids=$(docker exec spring_orchestrator-mongodb-1 mongosh --quiet digital_instrument_service --eval 'db.dis_instrument_flows.find({status:"WAITING_RETRY",currentStep:{$in:["AWAIT_PREPARATION_APPROVAL","AWAIT_DELIVERY_APPROVAL"]}},{_id:1}).forEach(function(f){print(String(f._id))})' 2>/dev/null)

  for id in $ids; do
    curl -sf -X POST "$DIS_URL/flows/enigio-instrument/$id/approve" \
      -H "Content-Type: application/json" -d '{}' >/dev/null 2>&1 &
  done
  wait 2>/dev/null
}

# Submit waves
END=$((SECONDS + DURATION * 60))
WAVE=0
TOTAL_SUBMITTED=0

log "Starting ${DURATION}m test — $WAVE_SIZE flows every ${WAVE_INTERVAL}s"

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

docker exec spring_orchestrator-mongodb-1 mongosh --quiet digital_instrument_service --eval '
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
docker exec spring_orchestrator-kafka-1-1 bash -c '
T="dis.instrument.commands"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.commands.replies"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.commands-dlt"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
T="dis.instrument.notifications"; kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:29092 --topic $T 2>/dev/null | tr "\n" "," ; echo
'

log "=== Webhook Registrations ==="
docker logs spring_orchestrator-mock-vendor-1 2>&1 | grep "Webhook registered" | tail -3
WEBHOOK_FIRED=$(docker logs spring_orchestrator-mock-vendor-1 2>&1 | grep -c "Webhook fired")
log "Total webhooks fired: $WEBHOOK_FIRED"

log "DONE"
