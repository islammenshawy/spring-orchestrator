package com.docuseal.signing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DocuSealSigningApplication {
    public static void main(String[] args) {
        SpringApplication.run(DocuSealSigningApplication.class, args);
    }
}
