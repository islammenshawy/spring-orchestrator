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
        List<Document> allLogs = new ArrayList<>();

        // orchestrator-starter library logs
        Query libQuery = new Query(Criteria.where("flowId").is(flowId))
                .with(Sort.by(Sort.Direction.ASC, "startedAt"));
        allLogs.addAll(mongoTemplate.find(libQuery, Document.class, "orchestrator_step_log"));

        // Legacy saga_step_logs
        allLogs.addAll(mongoTemplate.find(libQuery, Document.class, "saga_step_logs"));

        // Legacy sm_step_logs
        Query smQuery = new Query(Criteria.where("flowId").is(flowId))
                .with(Sort.by(Sort.Direction.ASC, "createdAt"));
        allLogs.addAll(mongoTemplate.find(smQuery, Document.class, "sm_step_logs"));

        return ResponseEntity.ok(allLogs);
    }
}
