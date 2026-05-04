package com.enigio.orchestrator.dashboard.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

/**
 * Provides a second MongoTemplate for the digital-instrument-service database.
 * The primary MongoTemplate (auto-configured) connects to enigio_orchestrator.
 * This one connects to digital_instrument_service.
 */
@Configuration
public class DisMongoConfig {

    @Value("${dis.mongodb.uri:mongodb://localhost:27017/digital_instrument_service}")
    private String disMongoUri;

    @Bean("disMongoTemplate")
    public MongoTemplate disMongoTemplate() {
        MongoClient client = MongoClients.create(disMongoUri);
        String dbName = disMongoUri.substring(disMongoUri.lastIndexOf('/') + 1);
        return new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, dbName));
    }
}
