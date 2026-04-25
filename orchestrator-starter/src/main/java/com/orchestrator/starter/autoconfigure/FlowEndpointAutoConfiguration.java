package com.orchestrator.starter.autoconfigure;

import com.orchestrator.starter.domain.AbstractFlow;
import com.orchestrator.starter.domain.OrchestratorFlow;
import com.orchestrator.starter.domain.OrchestratorFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.UUID;

@Slf4j
@AutoConfiguration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "orchestrator.endpoints.enabled", havingValue = "true", matchIfMissing = true)
public class FlowEndpointAutoConfiguration {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @RestController
    @RequestMapping("${orchestrator.endpoints.base-path:/flows}")
    public static class FlowEndpointController {

        @Autowired private FlowOrchestrator orchestrator;
        @Autowired private OrchestratorFlowRepository repository;
        @Autowired private ObjectMapper objectMapper;
        @Autowired private ApplicationContext context;

        private Class<?> entityClass;

        @PostConstruct
        void init() {
            // Discover entity class from @Flow bean's FlowDefinition<F> generic
            var flowBeans = context.getBeansWithAnnotation(
                    com.orchestrator.starter.annotation.Flow.class);
            for (Object flowDef : flowBeans.values()) {
                java.lang.reflect.Type superclass = flowDef.getClass().getGenericSuperclass();
                if (superclass instanceof java.lang.reflect.ParameterizedType pt) {
                    java.lang.reflect.Type[] args = pt.getActualTypeArguments();
                    if (args.length > 0 && args[0] instanceof Class<?> c) {
                        entityClass = c;
                        log.info("Auto-endpoint: entity class = {}", c.getSimpleName());
                        return;
                    }
                }
            }
            entityClass = AbstractFlow.class;
        }

        @PostMapping
        public ResponseEntity<?> startFlow(@RequestBody Map<String, Object> body) {
            try {
                body.putIfAbsent("correlationId", UUID.randomUUID().toString());
                String json = objectMapper.writeValueAsString(body);
                OrchestratorFlow flow = (OrchestratorFlow) objectMapper.readValue(json, entityClass);
                var saved = repository.save(flow);
                var started = orchestrator.startFlow((OrchestratorFlow) saved);

                return ResponseEntity.accepted().body(Map.of(
                        "id", started.getId(),
                        "correlationId", started.getCorrelationId(),
                        "status", started.getStatus().name(),
                        "currentStep", started.getCurrentStep(),
                        "message", "Flow started. Poll GET /flows/" + started.getId() + " for status."
                ));
            } catch (Exception e) {
                return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
            }
        }

        @GetMapping("/{id}")
        public ResponseEntity<?> getFlow(@PathVariable String id) {
            Object found = repository.findById(id).orElse(null);
            return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
        }

        @GetMapping("/{id}/status")
        public ResponseEntity<?> getStatus(@PathVariable String id) {
            Object found = repository.findById(id).orElse(null);
            if (found == null) return ResponseEntity.notFound().build();
            OrchestratorFlow flow = (OrchestratorFlow) found;
            return ResponseEntity.ok(Map.of(
                    "id", flow.getId(),
                    "status", flow.getStatus().name(),
                    "currentStep", flow.getCurrentStep() != null ? flow.getCurrentStep() : "",
                    "retryCount", flow.getRetryCount(),
                    "errorMessage", flow.getErrorMessage() != null ? flow.getErrorMessage() : ""
            ));
        }

        @GetMapping("/correlation/{correlationId}")
        public ResponseEntity<?> getByCorrelation(@PathVariable String correlationId) {
            Object found = repository.findByCorrelationId(correlationId).orElse(null);
            return found != null ? ResponseEntity.ok(found) : ResponseEntity.notFound().build();
        }
    }
}
