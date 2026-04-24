package com.enigio.orchestrator.saga.api;

import com.enigio.orchestrator.common.domain.DocumentFlow;
import com.enigio.orchestrator.common.domain.DocumentFlowRepository;
import com.enigio.orchestrator.saga.saga.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/saga/flows")
@RequiredArgsConstructor
public class FlowController {

    private final SagaOrchestrator orchestrator;
    private final DocumentFlowRepository flowRepository;

    @PostMapping
    public ResponseEntity<DocumentFlow> startFlow(@RequestBody Map<String, String> request) {
        DocumentFlow flow = DocumentFlow.builder()
                .correlationId(UUID.randomUUID().toString())
                .title(request.get("title"))
                .content(request.get("content"))
                .signerEmail(request.get("signerEmail"))
                .metadata(request.get("metadata"))
                .pattern("saga")
                .build();

        flow = flowRepository.save(flow);
        orchestrator.startFlow(flow);

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
