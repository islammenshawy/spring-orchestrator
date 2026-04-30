package com.dis.instrument;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DigitalInstrumentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DigitalInstrumentServiceApplication.class, args);
    }
}
