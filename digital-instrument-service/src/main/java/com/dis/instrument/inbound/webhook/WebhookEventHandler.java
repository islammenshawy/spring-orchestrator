package com.dis.instrument.inbound.webhook;

import com.dis.instrument.model.WebhookEvent;

import java.util.Map;
import java.util.Set;

/**
 * Strategy interface for handling Enigio webhook events.
 * Each implementation handles one or more {@link WebhookEvent} types.
 */
public interface WebhookEventHandler {

    Set<WebhookEvent> getSupportedEvents();

    void handle(String traceOriginalId, Map<String, Object> payload);
}
