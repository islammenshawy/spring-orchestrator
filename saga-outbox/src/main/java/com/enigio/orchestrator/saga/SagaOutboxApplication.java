package com.enigio.orchestrator.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableMongoRepositories(basePackages = {
        "com.enigio.orchestrator.saga",
        "com.enigio.orchestrator.common"
})
@SpringBootApplication(scanBasePackages = {
        "com.enigio.orchestrator.saga",
        "com.enigio.orchestrator.common"
})
public class SagaOutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOutboxApplication.class, args);
    }
}
