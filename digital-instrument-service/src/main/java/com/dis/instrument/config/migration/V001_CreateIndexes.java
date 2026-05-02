package com.dis.instrument.config.migration;

import io.mongock.api.annotations.ChangeUnit;
import io.mongock.api.annotations.Execution;
import io.mongock.api.annotations.RollbackExecution;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;

/**
 * V001: Create all MongoDB indexes for the Digital Instrument Service.
 *
 * This migration runs once during application startup (tracked by Mongock).
 * Safe across multiple pods — Mongock uses distributed locking.
 *
 * Indexes cover:
 *   - Flow collection (dis_instrument_flows): query, recovery, scheduling
 *   - Outbox (orchestrator_outbox): publisher polling, TTL cleanup
 *   - Idempotency (orchestrator_processed_events): TTL cleanup
 *   - Step logs (orchestrator_step_log): query, TTL cleanup
 */
@ChangeUnit(id = "V001-create-indexes", order = "001", author = "orchestrator")
public class V001_CreateIndexes {

    private static final String FLOWS = "dis_instrument_flows";

    @Execution
    public void execute(MongoTemplate mongoTemplate) {

        // ===== Flow Collection =====

        // Stale flow recovery: find IN_PROGRESS flows older than threshold
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("status", Sort.Direction.ASC)
                        .on("updatedAt", Sort.Direction.ASC)
                        .named("status_updated_idx"));

        // Business correlation lookup (unique)
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("correlationId", Sort.Direction.ASC)
                        .unique()
                        .named("correlationId_idx"));

        // Multi-flow routing
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("flowType", Sort.Direction.ASC)
                        .named("flowType_idx"));

        // Dashboard status filtering
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("status", Sort.Direction.ASC)
                        .named("status_idx"));

        // WaitingFlowScheduler: find flows in wait steps
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("currentStep", Sort.Direction.ASC)
                        .on("status", Sort.Direction.ASC)
                        .named("currentStep_status_idx"));

        // Sort by creation date
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("createdAt", Sort.Direction.DESC)
                        .named("createdAt_desc_idx"));

        // Business reference lookup
        mongoTemplate.indexOps(FLOWS).ensureIndex(
                new Index().on("reference", Sort.Direction.ASC)
                        .named("reference_idx"));

        // ===== Outbox =====

        // Outbox publisher polls for unpublished events every 500ms
        mongoTemplate.indexOps("orchestrator_outbox").ensureIndex(
                new Index().on("published", Sort.Direction.ASC)
                        .on("createdAt", Sort.Direction.ASC)
                        .named("unpublished_idx"));

        // TTL: auto-delete published outbox events after 7 days
        mongoTemplate.indexOps("orchestrator_outbox").ensureIndex(
                new Index().on("publishedAt", Sort.Direction.ASC)
                        .expire(Duration.ofDays(7))
                        .named("publishedAt_ttl_7d"));

        // ===== Processed Events (Idempotency) =====

        // TTL: auto-delete processed events after 30 days
        mongoTemplate.indexOps("orchestrator_processed_events").ensureIndex(
                new Index().on("processedAt", Sort.Direction.ASC)
                        .expire(Duration.ofDays(30))
                        .named("processedAt_ttl_30d"));

        // ===== Step Execution Log =====

        // Step lookup by flow + step
        mongoTemplate.indexOps("orchestrator_step_log").ensureIndex(
                new Index().on("flowId", Sort.Direction.ASC)
                        .on("stepName", Sort.Direction.ASC)
                        .on("attemptNumber", Sort.Direction.ASC)
                        .named("flow_step_idx"));

        // Timeline view by flow
        mongoTemplate.indexOps("orchestrator_step_log").ensureIndex(
                new Index().on("flowId", Sort.Direction.ASC)
                        .on("startedAt", Sort.Direction.ASC)
                        .named("flow_started_idx"));

        // TTL: auto-delete step logs after 90 days
        mongoTemplate.indexOps("orchestrator_step_log").ensureIndex(
                new Index().on("completedAt", Sort.Direction.ASC)
                        .expire(Duration.ofDays(90))
                        .named("completedAt_ttl_90d"));
    }

    @RollbackExecution
    public void rollback(MongoTemplate mongoTemplate) {
        // Indexes are safe to leave in place — no rollback needed
        // Dropping indexes could impact running queries
    }
}
