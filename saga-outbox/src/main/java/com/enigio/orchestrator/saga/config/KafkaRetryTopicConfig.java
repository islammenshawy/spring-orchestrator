package com.enigio.orchestrator.saga.config;

import com.enigio.orchestrator.common.config.JitteredExponentialBackOffPolicy;
import com.enigio.orchestrator.common.config.KafkaTopics;
import com.enigio.orchestrator.common.exception.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaRetryTopicConfig {

    private final RetryTopicProperties props;

    private JitteredExponentialBackOffPolicy buildBackoff() {
        JitteredExponentialBackOffPolicy backoff = new JitteredExponentialBackOffPolicy();
        backoff.setInitialInterval(props.getInitialIntervalMs());
        backoff.setMultiplier(props.getMultiplier());
        backoff.setMaxInterval(props.getMaxIntervalMs());
        backoff.setJitterFactor(props.getJitterFactor());
        return backoff;
    }

    @Bean
    public RetryTopicConfiguration sagaStepsRetryConfig(KafkaTemplate<String, String> template) {
        log.info("Saga steps retry: attempts={}, delay={}ms, multiplier={}, maxDelay={}ms, jitter={}",
                props.getMaxAttempts(), props.getInitialIntervalMs(),
                props.getMultiplier(), props.getMaxIntervalMs(), props.getJitterFactor());

        return RetryTopicConfigurationBuilder
                .newInstance()
                .customBackoff(buildBackoff())
                .maxAttempts(props.getMaxAttempts())
                .includeTopic(KafkaTopics.SAGA_STEPS)
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .dltProcessingFailureStrategy(DltStrategy.ALWAYS_RETRY_ON_ERROR)
                .retryOn(RetryableException.class)
                .create(template);
    }

    @Bean
    public RetryTopicConfiguration sagaRepliesRetryConfig(KafkaTemplate<String, String> template) {
        return RetryTopicConfigurationBuilder
                .newInstance()
                .customBackoff(buildBackoff())
                .maxAttempts(props.getMaxAttempts())
                .includeTopic(KafkaTopics.SAGA_REPLIES)
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .dltProcessingFailureStrategy(DltStrategy.ALWAYS_RETRY_ON_ERROR)
                .retryOn(RuntimeException.class)
                .create(template);
    }
}
