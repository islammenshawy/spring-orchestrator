package com.dis.instrument.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Digital Instrument Service API")
                        .description("Orchestrates digital trade instrument lifecycle " +
                                "(promissory notes, bills of exchange, bills of lading) " +
                                "across vendor integrations. Enigio trace:original is the " +
                                "first supported vendor.")
                        .version("1.0.0")
                        .contact(new Contact().name("DIS Team")))
                .servers(List.of(
                        new Server().url("http://localhost:8087").description("Local"),
                        new Server().url("http://digital-instrument-service:8087").description("Docker")
                ));
    }

    @Bean
    public GroupedOpenApi flowManagement() {
        return GroupedOpenApi.builder()
                .group("1-flow-management")
                .displayName("Flow Management")
                .pathsToMatch("/flows/enigio-instrument", "/flows/enigio-instrument/{id}",
                        "/flows/enigio-instrument/{id}/status")
                .build();
    }

    @Bean
    public GroupedOpenApi flowControl() {
        return GroupedOpenApi.builder()
                .group("2-flow-control")
                .displayName("Flow Control")
                .pathsToMatch("/flows/enigio-instrument/{id}/approve",
                        "/flows/enigio-instrument/{id}/approval-status",
                        "/flows/enigio-instrument/{id}/cancel")
                .build();
    }

    @Bean
    public GroupedOpenApi additionalDocuments() {
        return GroupedOpenApi.builder()
                .group("3-additional-documents")
                .displayName("Additional Documents")
                .pathsToMatch("/documents/additional/**")
                .build();
    }

    @Bean
    public GroupedOpenApi vendorSync() {
        return GroupedOpenApi.builder()
                .group("4-vendor-sync")
                .displayName("Vendor Sync")
                .pathsToMatch("/vendor/**")
                .build();
    }

    @Bean
    public GroupedOpenApi webhooks() {
        return GroupedOpenApi.builder()
                .group("5-webhooks")
                .displayName("Enigio Webhooks")
                .pathsToMatch("/webhooks/**")
                .build();
    }
}
