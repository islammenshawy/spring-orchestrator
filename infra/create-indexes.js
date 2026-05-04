/**
 * MongoDB Index Creation Script
 *
 * Run with: mongosh <connection-uri> create-indexes.js
 * Example:  mongosh mongodb://localhost:27017/digital_instrument_service scripts/create-indexes.js
 *
 * Creates all required indexes for the orchestrator library.
 * Safe to re-run — uses createIndex which is idempotent.
 *
 * Collections:
 *   - dis_instrument_flows (or any flow collection)
 *   - orchestrator_outbox
 *   - orchestrator_processed_events
 *   - orchestrator_step_log
 *   - orchestrator_consumer_offsets
 */

const FLOW_COLLECTION = "dis_instrument_flows";

print("=== Orchestrator MongoDB Index Creation ===");
print("Database: " + db.getName());
print("");

// ===== Flow Collection =====
print("--- " + FLOW_COLLECTION + " ---");

db[FLOW_COLLECTION].createIndex(
    { status: 1, updatedAt: 1 },
    { name: "status_updated_idx", background: true }
);
print("  status_updated_idx (stale flow recovery)");

db[FLOW_COLLECTION].createIndex(
    { correlationId: 1 },
    { name: "correlationId_idx", unique: true, background: true }
);
print("  correlationId_idx (unique, lookup)");

db[FLOW_COLLECTION].createIndex(
    { flowType: 1 },
    { name: "flowType_idx", background: true }
);
print("  flowType_idx (multi-flow routing)");

db[FLOW_COLLECTION].createIndex(
    { status: 1 },
    { name: "status_idx", background: true }
);
print("  status_idx (dashboard filtering)");

db[FLOW_COLLECTION].createIndex(
    { currentStep: 1, status: 1 },
    { name: "currentStep_status_idx", background: true }
);
print("  currentStep_status_idx (WaitingFlowScheduler)");

db[FLOW_COLLECTION].createIndex(
    { createdAt: -1 },
    { name: "createdAt_desc_idx", background: true }
);
print("  createdAt_desc_idx (sorting)");

db[FLOW_COLLECTION].createIndex(
    { reference: 1 },
    { name: "reference_idx", background: true }
);
print("  reference_idx (business lookup)");

// ===== Outbox =====
print("");
print("--- orchestrator_outbox ---");

db.orchestrator_outbox.createIndex(
    { published: 1, createdAt: 1 },
    { name: "unpublished_idx", background: true }
);
print("  unpublished_idx (outbox publisher polling)");

db.orchestrator_outbox.createIndex(
    { publishedAt: 1 },
    { name: "publishedAt_ttl_7d", expireAfterSeconds: 604800, background: true }
);
print("  publishedAt_ttl_7d (auto-cleanup after 7 days)");

// ===== Processed Events (Idempotency) =====
print("");
print("--- orchestrator_processed_events ---");

db.orchestrator_processed_events.createIndex(
    { processedAt: 1 },
    { name: "processedAt_ttl_30d", expireAfterSeconds: 2592000, background: true }
);
print("  processedAt_ttl_30d (auto-cleanup after 30 days)");

// ===== Step Execution Log =====
print("");
print("--- orchestrator_step_log ---");

db.orchestrator_step_log.createIndex(
    { flowId: 1, stepName: 1, attemptNumber: 1 },
    { name: "flow_step_idx", background: true }
);
print("  flow_step_idx (step lookup)");

db.orchestrator_step_log.createIndex(
    { flowId: 1, startedAt: 1 },
    { name: "flow_started_idx", background: true }
);
print("  flow_started_idx (timeline view)");

db.orchestrator_step_log.createIndex(
    { completedAt: 1 },
    { name: "completedAt_ttl_90d", expireAfterSeconds: 7776000, background: true }
);
print("  completedAt_ttl_90d (auto-cleanup after 90 days)");

// ===== Consumer Offsets =====
print("");
print("--- orchestrator_consumer_offsets ---");
// _id is composite key (consumerGroup|topic|partition) — already indexed
print("  _id (composite key, auto-indexed)");

// ===== Verify =====
print("");
print("=== Verification ===");
[FLOW_COLLECTION, "orchestrator_outbox", "orchestrator_processed_events",
 "orchestrator_step_log", "orchestrator_consumer_offsets"].forEach(c => {
    var indexes = db.getCollection(c).getIndexes();
    print(c + ": " + indexes.length + " indexes");
    indexes.forEach(i => print("  " + i.name + " → " + JSON.stringify(i.key)));
});

print("");
print("=== Done ===");
