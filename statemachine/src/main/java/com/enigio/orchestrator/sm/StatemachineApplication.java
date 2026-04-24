package com.enigio.orchestrator.sm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableMongoRepositories(basePackages = {
        "com.enigio.orchestrator.sm",
        "com.enigio.orchestrator.common"
})
@SpringBootApplication(scanBasePackages = {
        "com.enigio.orchestrator.sm",
        "com.enigio.orchestrator.common"
})
public class StatemachineApplication {

    public static void main(String[] args) {
        SpringApplication.run(StatemachineApplication.class, args);
    }
}
