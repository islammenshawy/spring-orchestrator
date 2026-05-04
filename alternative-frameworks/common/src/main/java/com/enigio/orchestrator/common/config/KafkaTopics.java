package com.enigio.orchestrator.common.config;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String FLOW_COMMANDS = "enigio.flow.commands";
    public static final String SAGA_STEPS = "enigio.saga.steps";
    public static final String SAGA_REPLIES = "enigio.saga.replies";
    public static final String SM_EVENTS = "enigio.sm.events";
    public static final String DLQ = "enigio.dlq";
}
