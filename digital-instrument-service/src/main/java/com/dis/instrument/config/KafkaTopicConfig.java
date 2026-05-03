package com.dis.instrument.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic notificationTopic(
            @Value("${dis.notifications.topic:dis.instrument.notifications}") String topic) {
        return TopicBuilder.name(topic).partitions(6).build();
    }
}
