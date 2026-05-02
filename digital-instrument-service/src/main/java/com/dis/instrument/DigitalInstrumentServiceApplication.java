package com.dis.instrument;

import io.mongock.runner.springboot.EnableMongock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableMongock
@EnableMongoRepositories(basePackages = "com.dis.instrument")
public class DigitalInstrumentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DigitalInstrumentServiceApplication.class, args);
    }
}
