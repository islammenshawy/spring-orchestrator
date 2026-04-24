package com.enigio.orchestrator.dashboard.controller;

import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
public class StepLogController {

    private final MongoTemplate mongoTemplate;

    @GetMapping("/flows/{flowId}/steps")
    public ResponseEntity<List<Document>> getStepLogs(@PathVariable String flowId) {
        // Check saga_step_logs
        Query sagaQuery = new Query(Criteria.where("flowId").is(flowId))
                .with(Sort.by(Sort.Direction.ASC, "startedAt"));
        List<Document> sagaLogs = mongoTemplate.find(sagaQuery, Document.class, "saga_step_logs");

        // Check sm_step_logs
        Query smQuery = new Query(Criteria.where("flowId").is(flowId))
                .with(Sort.by(Sort.Direction.ASC, "createdAt"));
        List<Document> smLogs = mongoTemplate.find(smQuery, Document.class, "sm_step_logs");

        List<Document> allLogs = new ArrayList<>();
        allLogs.addAll(sagaLogs);
        allLogs.addAll(smLogs);

        return ResponseEntity.ok(allLogs);
    }
}
