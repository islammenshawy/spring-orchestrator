package com.enigio.orchestrator.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@EnableMongoRepositories(basePackages = {
        "com.enigio.orchestrator.dashboard",
        "com.enigio.orchestrator.common"
})
@SpringBootApplication(scanBasePackages = {
        "com.enigio.orchestrator.dashboard",
        "com.enigio.orchestrator.common"
})
public class DashboardApplication {

    public static void main(String[] args) {
        SpringApplication.run(DashboardApplication.class, args);
    }
}
