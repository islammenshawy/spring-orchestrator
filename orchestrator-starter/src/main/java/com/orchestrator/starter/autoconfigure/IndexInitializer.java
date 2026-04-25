package com.orchestrator.starter.autoconfigure;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.time.Duration;

/**
 * Creates TTL indexes on library collections at startup.
 * TTL durations are configurable via application.yml:
 *
 *   orchestrator.retention.outbox-days: 7
 *   orchestrator.retention.processed-events-days: 30
 *   orchestrator.retention.step-log-days: 90
 *
 * MongoDB automatically deletes documents older than the TTL.
 */
@Slf4j
@RequiredArgsConstructor
public class IndexInitializer {

    private final MongoTemplate mongoTemplate;
    private final OrchestratorProperties props;

    @EventListener(ApplicationReadyEvent.class)
    public void createIndexes() {
        OrchestratorProperties.RetentionConfig retention = props.getRetention();

        // TTL on published outbox events
        if (retention.getOutboxDays() > 0) {
            ensureTtlIndex("orchestrator_outbox", "publishedAt", retention.getOutboxDays());
        }

        // TTL on processed events (idempotency)
        if (retention.getProcessedEventsDays() > 0) {
            ensureTtlIndex("orchestrator_processed_events", "processedAt",
                    retention.getProcessedEventsDays());
        }

        // TTL on step execution logs
        if (retention.getStepLogDays() > 0) {
            ensureTtlIndex("orchestrator_step_log", "completedAt", retention.getStepLogDays());
        }
    }

    private void ensureTtlIndex(String collection, String field, int days) {
        try {
            mongoTemplate.indexOps(collection)
                    .ensureIndex(new Index()
                            .on(field, Sort.Direction.ASC)
                            .expire(Duration.ofDays(days))
                            .named(field + "_ttl_" + days + "d"));
            log.info("[Index] TTL on {}.{}: {} days", collection, field, days);
        } catch (Exception e) {
            log.warn("[Index] Failed to create TTL index on {}.{}: {}",
                    collection, field, e.getMessage());
        }
    }
}
