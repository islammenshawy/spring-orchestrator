package com.dis.instrument.vendor.enigio.feign;

import feign.Logger;
import org.springframework.context.annotation.Bean;

/**
 * Feign configuration for Enigio API clients.
 * Timeouts configured in application.yml under spring.cloud.openfeign.client.config.
 */
public class EnigioFeignConfig {

    @Bean
    public Logger.Level enigioFeignLoggerLevel() {
        return Logger.Level.BASIC;
    }
}
