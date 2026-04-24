package com.example.enigio.config;

import com.example.enigio.flow.EnigioFlow;
import com.example.enigio.flow.EnigioFlowRepository;
import com.orchestrator.starter.flow.FlowOrchestrator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/flows")
@RequiredArgsConstructor
public class FlowController {

    @SuppressWarnings("unchecked")
    private final FlowOrchestrator<EnigioFlow> orchestrator;
    private final EnigioFlowRepository flowRepository;

    @PostMapping
    public ResponseEntity<EnigioFlow> startFlow(@RequestBody Map<String, String> request) {
        EnigioFlow flow = EnigioFlow.builder()
                .correlationId(UUID.randomUUID().toString())
                .title(request.get("title"))
                .content(request.get("content"))
                .signerEmail(request.get("signerEmail"))
                .build();

        flow = flowRepository.save(flow);
        flow = orchestrator.startFlow(flow);
        return ResponseEntity.ok(flow);
    }

    @GetMapping("/{id}")
    public ResponseEntity<EnigioFlow> getFlow(@PathVariable String id) {
        return flowRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
