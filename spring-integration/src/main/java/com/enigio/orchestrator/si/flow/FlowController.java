package com.enigio.orchestrator.si.flow;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.common.kafka.FlowCommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/si/flows")
@RequiredArgsConstructor
public class FlowController {

    private final DocumentFlowRepository flowRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<DocumentFlow> startFlow(@RequestBody Map<String, String> request) throws Exception {
        DocumentFlow flow = DocumentFlow.builder()
                .correlationId(UUID.randomUUID().toString())
                .title(request.get("title"))
                .content(request.get("content"))
                .signerEmail(request.get("signerEmail"))
                .metadata(request.get("metadata"))
                .pattern("spring-integration")
                .build();

        flow = flowRepository.save(flow);

        // Publish first step command to Kafka — same pattern as Saga/SM
        FlowCommandMessage command = FlowCommandMessage.builder()
                .eventId(UUID.randomUUID().toString())
                .flowId(flow.getId())
                .correlationId(flow.getCorrelationId())
                .step(flow.getCurrentStep())
                .build();

        kafkaTemplate.send("enigio.si.commands", flow.getId(),
                objectMapper.writeValueAsString(command));

        return ResponseEntity.ok(flow);
    }

    @GetMapping("/{id}")
    public ResponseEntity<DocumentFlow> getFlow(@PathVariable String id) {
        return flowRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
