package com.enigio.orchestrator.si.config;

import com.enigio.orchestrator.common.config.JitteredExponentialBackOffPolicy;
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
public class SiKafkaRetryConfig {

    private final IntegrationRetryProperties props;

    @Bean
    public RetryTopicConfiguration siCommandsRetryConfig(KafkaTemplate<String, String> template) {
        log.info("SI retry: attempts={}, delay={}ms, multiplier={}, maxDelay={}ms, jitter={}",
                props.getMaxAttempts(), props.getInitialIntervalMs(),
                props.getMultiplier(), props.getMaxIntervalMs(), props.getJitterFactor());

        JitteredExponentialBackOffPolicy backoff = new JitteredExponentialBackOffPolicy();
        backoff.setInitialInterval(props.getInitialIntervalMs());
        backoff.setMultiplier(props.getMultiplier());
        backoff.setMaxInterval(props.getMaxIntervalMs());
        backoff.setJitterFactor(props.getJitterFactor());

        return RetryTopicConfigurationBuilder
                .newInstance()
                .customBackoff(backoff)
                .maxAttempts(props.getMaxAttempts())
                .includeTopic("enigio.si.commands")
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
                .dltProcessingFailureStrategy(DltStrategy.ALWAYS_RETRY_ON_ERROR)
                .retryOn(RetryableException.class)
                .create(template);
    }
}
