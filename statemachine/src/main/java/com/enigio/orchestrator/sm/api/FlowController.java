package com.enigio.orchestrator.sm.api;

import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/sm/flows")
@RequiredArgsConstructor
public class FlowController {

    private final DocumentFlowRepository flowRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<DocumentFlow> startFlow(@RequestBody Map<String, String> request) throws JsonProcessingException {
        DocumentFlow flow = DocumentFlow.builder()
                .correlationId(UUID.randomUUID().toString())
                .title(request.get("title"))
                .content(request.get("content"))
                .signerEmail(request.get("signerEmail"))
                .metadata(request.get("metadata"))
                .pattern("statemachine")
                .build();

        flow = flowRepository.save(flow);

        FlowCommandMessage command = FlowCommandMessage.builder()
                .eventId(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .build();

        kafkaTemplate.send(KafkaTopics.SM_EVENTS, flow.getId(),
                objectMapper.writeValueAsString(command));

        return ResponseEntity.ok(flow);
    }

    @GetMapping("/{id}")
    public ResponseEntity<DocumentFlow> getFlow(@PathVariable String id) {
        return flowRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/correlation/{correlationId}")
    public ResponseEntity<DocumentFlow> getFlowByCorrelation(@PathVariable String correlationId) {
        return flowRepository.findByCorrelationId(correlationId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
