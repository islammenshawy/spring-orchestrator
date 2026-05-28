package com.orchestrator.starter.failover;

import com.orchestrator.starter.autoconfigure.OrchestratorProperties;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.DcConfig;
import com.orchestrator.starter.autoconfigure.OrchestratorProperties.ReplicationPolicy;

/**
 * Resolves the actual Kafka topic name for a given DC.
 *
 * IDENTITY policy: same topic name on all DCs (requires IdentityReplicationPolicy in MM2).
 * PREFIXED policy: MM2 default — replicated topics are {source-alias}.{topic} on the target DC.
 *
 * Example with PREFIXED, originating DC = dc-a, target DC = dc-b:
 *   "dis.instrument.commands" → "dc-a.dis.instrument.commands" (on DC-B)
 */
public class TopicResolver {

    private final ReplicationPolicy policy;
    private final OrchestratorProperties.FailoverConfig config;

    public TopicResolver(OrchestratorProperties.FailoverConfig config) {
        this.config = config;
        this.policy = config.getReplicationPolicy();
    }

    /**
     * Resolve the topic name for a given DC.
     *
     * @param originalTopic the logical topic name (e.g., "dis.instrument.commands")
     * @param targetDc      the DC where the consumer/producer will connect
     * @param originatingDc the DC where the message was originally produced
     * @return the actual topic name on the target DC
     */
    public String resolve(String originalTopic, String targetDc, String originatingDc) {
        if (policy == ReplicationPolicy.IDENTITY) {
            return originalTopic;
        }

        // PREFIXED: if consuming on a different DC than where the message originated,
        // the topic is prefixed with the originating DC's source alias
        if (targetDc.equals(originatingDc)) {
            return originalTopic; // local topic — no prefix
        }

        DcConfig originConfig = config.getDcs().get(originatingDc);
        if (originConfig == null || originConfig.getSourceAlias() == null) {
            return originalTopic; // no alias configured — fall back to original
        }

        return originConfig.getSourceAlias() + "." + originalTopic;
    }

    /**
     * Resolve topic for the currently active DC reading its own messages.
     * Shorthand for the common case: active DC reads from its own local topics.
     */
    public String resolveLocal(String originalTopic) {
        return originalTopic; // local topics are always unprefixed
    }

    /**
     * Resolve topic for failover: target DC reading the originating DC's replicated data.
     */
    public String resolveForFailover(String originalTopic, String targetDc, String originatingDc) {
        return resolve(originalTopic, targetDc, originatingDc);
    }
}
