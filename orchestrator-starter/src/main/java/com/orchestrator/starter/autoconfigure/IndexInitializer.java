package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.annotation.SearchAttribute;
import com.orchestrator.starter.flow.FlowTypeDescriptor;
import com.orchestrator.starter.flow.FlowTypeRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Document;

import java.lang.reflect.Field;
import java.time.Duration;

/**
 * Creates all required indexes on library collections at startup.
 *
 * Includes:
 * - TTL indexes for automatic cleanup (outbox, idempotency, step logs)
 * - Compound indexes for query performance (status lookup, outbox polling, step queries)
 * - Indexes that @CompoundIndex annotations would create if auto-index was enabled
 *
 * Spring Boot 4 defaults to auto-index-creation=false for production safety,
 * so we create all indexes programmatically here.
 */
@Slf4j
public class IndexInitializer {

    private final MongoTemplate mongoTemplate;
    private final OrchestratorProperties props;
    private final FlowTypeRegistry flowTypeRegistry;

    public IndexInitializer(MongoTemplate mongoTemplate, OrchestratorProperties props,
                            FlowTypeRegistry flowTypeRegistry) {
        this.mongoTemplate = mongoTemplate;
        this.props = props;
        this.flowTypeRegistry = flowTypeRegistry;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createIndexes() {
        OrchestratorProperties.RetentionConfig retention = props.getRetention();

        // ===== TTL Indexes (automatic cleanup) =====

        if (retention.getOutboxDays() > 0) {
            ensureTtlIndex("orchestrator_outbox", "publishedAt", retention.getOutboxDays());
        }
        if (retention.getProcessedEventsDays() > 0) {
            ensureTtlIndex("orchestrator_processed_events", "processedAt",
                    retention.getProcessedEventsDays());
        }
        if (retention.getStepLogDays() > 0) {
            ensureTtlIndex("orchestrator_step_log", "completedAt", retention.getStepLogDays());
        }

        // ===== Outbox performance indexes =====

        // Outbox publisher polls for unpublished events every 500ms
        ensureIndex("orchestrator_outbox", "unpublished_idx",
                new Index().on("published", Sort.Direction.ASC)
                        .on("createdAt", Sort.Direction.ASC));

        // ===== Step log query indexes =====

        // Step log lookup by flow + step
        ensureIndex("orchestrator_step_log", "flow_step_idx",
                new Index().on("flowId", Sort.Direction.ASC)
                        .on("stepName", Sort.Direction.ASC)
                        .on("attemptNumber", Sort.Direction.ASC));

        // Step log timeline by flow
        ensureIndex("orchestrator_step_log", "flow_started_idx",
                new Index().on("flowId", Sort.Direction.ASC)
                        .on("startedAt", Sort.Direction.ASC));

        // ===== Flow entity indexes (from AbstractFlow annotations) =====
        // These should be created by @CompoundIndex/@Indexed but auto-index is off

        // Stale flow recovery: find IN_PROGRESS flows older than threshold
        // Already defined in @CompoundIndex on AbstractFlow but needs explicit creation
        // Note: collection name varies per flow type — we create on all known collections
        for (var desc : getFlowCollections()) {
            ensureIndex(desc, "status_updated_idx",
                    new Index().on("status", Sort.Direction.ASC)
                            .on("updatedAt", Sort.Direction.ASC));

            ensureIndex(desc, "correlationId_idx",
                    new Index().on("correlationId", Sort.Direction.ASC).unique());

            ensureIndex(desc, "flowType_idx",
                    new Index().on("flowType", Sort.Direction.ASC));

            // Status alone for dashboard filtering
            ensureIndex(desc, "status_idx",
                    new Index().on("status", Sort.Direction.ASC));

            // currentStep for WaitingFlowScheduler
            ensureIndex(desc, "currentStep_idx",
                    new Index().on("currentStep", Sort.Direction.ASC)
                            .on("status", Sort.Direction.ASC));

            // createdAt for sorting
            ensureIndex(desc, "createdAt_idx",
                    new Index().on("createdAt", Sort.Direction.DESC));

            // Batch claiming: recovery + waiting scheduler claim queries
            ensureIndex(desc, "status_updated_claimed_idx",
                    new Index().on("status", Sort.Direction.ASC)
                            .on("updatedAt", Sort.Direction.ASC)
                            .on("claimedBy", Sort.Direction.ASC));

            // Orphan cleanup: find claimed flows by claimedAt
            ensureIndex(desc, "claimedBy_claimedAt_idx",
                    new Index().on("claimedBy", Sort.Direction.ASC)
                            .on("claimedAt", Sort.Direction.ASC));
        }

        // ===== Search attribute indexes =====
        createSearchAttributeIndexes();

        // ===== Consumer offset store =====
        // Uses _id as composite key (consumerGroup|topic|partition) — already indexed
    }

    /**
     * Scan entity classes for @SearchAttribute fields and create indexes.
     */
    private void createSearchAttributeIndexes() {
        if (flowTypeRegistry == null) return;

        for (FlowTypeDescriptor descriptor : flowTypeRegistry.getAll()) {
            Class<?> entityClass = descriptor.getEntityClass();
            if (entityClass == null || entityClass == Object.class) continue;

            String collection = resolveCollectionName(entityClass);
            if (collection == null) continue;

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(SearchAttribute.class)) {
                    String fieldName = field.getName();
                    ensureIndex(collection, fieldName + "_search_idx",
                            new Index().on(fieldName, Sort.Direction.ASC));
                    log.info("[Index] Search attribute index on {}.{}", collection, fieldName);
                }
            }
        }
    }

    private String resolveCollectionName(Class<?> entityClass) {
        Document doc = entityClass.getAnnotation(Document.class);
        if (doc != null && !doc.collection().isEmpty()) return doc.collection();
        // Fallback: lowercase class name
        return entityClass.getSimpleName().substring(0, 1).toLowerCase()
                + entityClass.getSimpleName().substring(1);
    }

    /**
     * Discover flow collection names from MongoDB.
     * Finds collections that look like flow collections (have status field).
     */
    private java.util.List<String> getFlowCollections() {
        var collections = new java.util.ArrayList<String>();
        for (String name : mongoTemplate.getCollectionNames()) {
            // Skip library collections
            if (name.startsWith("orchestrator_") || name.startsWith("system.")) continue;
            // Check if it looks like a flow collection (has status field)
            try {
                var sample = mongoTemplate.getCollection(name).find().limit(1).first();
                if (sample != null && sample.containsKey("status") && sample.containsKey("currentStep")) {
                    collections.add(name);
                }
            } catch (Exception ignored) {}
        }
        if (collections.isEmpty()) {
            // Fallback: common flow collection names
            collections.add("dis_instrument_flows");
            collections.add("enigio_flows");
        }
        log.info("[Index] Flow collections found: {}", collections);
        return collections;
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
            log.warn("[Index] Failed to create TTL index on {}.{}: {}", collection, field, e.getMessage());
        }
    }

    private void ensureIndex(String collection, String name, Index index) {
        try {
            mongoTemplate.indexOps(collection).ensureIndex(index.named(name));
            log.info("[Index] Created {}.{}", collection, name);
        } catch (Exception e) {
            // Index may already exist with same definition — safe to ignore
            if (!e.getMessage().contains("already exists")) {
                log.warn("[Index] Failed to create {}.{}: {}", collection, name, e.getMessage());
            }
        }
    }
}
