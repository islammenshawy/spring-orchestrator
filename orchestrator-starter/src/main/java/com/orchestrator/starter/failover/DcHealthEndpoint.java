package com.orchestrator.starter.failover;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Actuator endpoint: /actuator/dc-health
 *
 * Shows real-time DC health, active/standby status, supervisor state,
 * and topic resolution for both DCs.
 *
 * Example response:
 * {
 *   "activeDc": "dca",
 *   "supervisorState": "HEALTHY",
 *   "replicationPolicy": "PREFIXED",
 *   "dcs": {
 *     "dca": { "bootstrap": "kafka-a:29092", "healthy": true, "role": "ACTIVE" },
 *     "dcb": { "bootstrap": "kafka-b:29093", "healthy": true, "role": "STANDBY" }
 *   },
 *   "topicMapping": {
 *     "dis.instrument.commands": "dis.instrument.commands",
 *     "dis.instrument.commands (on dcb)": "dca.dis.instrument.commands"
 *   }
 * }
 */
@Endpoint(id = "dc-health")
@RequiredArgsConstructor
public class DcHealthEndpoint {

    private final DcFailoverSupervisor supervisor;
    private final DcHealthProbe probe;
    private final DcAwareKafkaManager kafkaManager;
    private final TopicResolver topicResolver;
    private final com.orchestrator.starter.autoconfigure.OrchestratorProperties.FailoverConfig config;

    @ReadOperation
    public Map<String, Object> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("activeDc", supervisor.getActiveDc());
        result.put("supervisorState", supervisor.getState().name());
        result.put("replicationPolicy", config.getReplicationPolicy().name());

        // Per-DC health
        Map<String, Object> dcs = new LinkedHashMap<>();
        String activeDc = supervisor.getActiveDc();
        config.getDcs().forEach((dcId, dcConfig) -> {
            Map<String, Object> dc = new LinkedHashMap<>();
            dc.put("bootstrap", dcConfig.getBootstrap());
            dc.put("sourceAlias", dcConfig.getSourceAlias());
            dc.put("healthy", probe.probe(dcId));
            dc.put("role", dcId.equals(activeDc) ? "ACTIVE" : "STANDBY");
            dcs.put(dcId, dc);
        });
        result.put("dcs", dcs);

        // Topic mapping example
        String sampleTopic = "dis.instrument.commands";
        Map<String, String> topics = new LinkedHashMap<>();
        config.getDcs().keySet().forEach(dcId -> {
            String resolved = topicResolver.resolve(sampleTopic, dcId, activeDc);
            topics.put(sampleTopic + " (on " + dcId + ")", resolved);
        });
        result.put("topicMapping", topics);

        return result;
    }
}
