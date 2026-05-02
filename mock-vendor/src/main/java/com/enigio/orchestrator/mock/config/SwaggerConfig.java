package com.enigio.orchestrator.mock.config;

import io.swagger.v3.oas.models.OpenAPI;
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
                        .title("Enigio trace:original Mock API")
                        .description("Mock implementation of the Enigio trace:original " +
                                "Fullnode API v3.3. Endpoints match the real API contract " +
                                "at docs.traceoriginal.com.")
                        .version("3.3.0"))
                .servers(List.of(
                        new Server().url("http://localhost:8081").description("Local"),
                        new Server().url("http://mock-vendor:8081").description("Docker")
                ));
    }

    @Bean
    public GroupedOpenApi documents() {
        return GroupedOpenApi.builder()
                .group("1-documents")
                .displayName("Documents")
                .pathsToMatch("/api/v1/documents/**")
                .build();
    }

    @Bean
    public GroupedOpenApi signatures() {
        return GroupedOpenApi.builder()
                .group("2-signatures")
                .displayName("Required Signatures")
                .pathsToMatch("/api/v1/required-signatures/**")
                .build();
    }

    @Bean
    public GroupedOpenApi envelopes() {
        return GroupedOpenApi.builder()
                .group("3-envelopes")
                .displayName("Envelopes")
                .pathsToMatch("/api/v1/envelopes/**")
                .build();
    }

    @Bean
    public GroupedOpenApi webhooksGroup() {
        return GroupedOpenApi.builder()
                .group("4-webhooks")
                .displayName("Webhooks")
                .pathsToMatch("/api/v1/notifications/**")
                .build();
    }

    @Bean
    public GroupedOpenApi admin() {
        return GroupedOpenApi.builder()
                .group("5-admin")
                .displayName("Admin / Testing")
                .pathsToMatch("/admin/**")
                .build();
    }
}
