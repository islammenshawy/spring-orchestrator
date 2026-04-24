package com.enigio.orchestrator.si;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableIntegration
@EnableMongoRepositories(basePackages = {
        "com.enigio.orchestrator.si",
        "com.enigio.orchestrator.common"
})
@SpringBootApplication(scanBasePackages = {
        "com.enigio.orchestrator.si",
        "com.enigio.orchestrator.common"
})
public class SpringIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringIntegrationApplication.class, args);
    }
}
