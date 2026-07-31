package com.demo.order.config;

import io.namastack.outbox.context.OutboxContextProvider;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Attaches correlation ids to every outbox record.
 *
 * <p>This solves a problem specific to the outbox pattern: the record is written during the inbound
 * HTTP request but published later, from a background poller thread with no trace context of its
 * own. Capturing the ids at schedule time and storing them on the row is what lets the eventual
 * Kafka message be correlated back to the request that caused it.
 *
 * <p>Implemented as a provider rather than per-event annotation attributes so it applies to every
 * payload type without repetition. {@code KafkaOutboxRoutingConfig} copies these values into Kafka
 * headers on the way out.
 */
@Component
public class TracingContextProvider implements OutboxContextProvider {

    @Override
    public Map<String, String> provide() {
        Map<String, String> context = new HashMap<>();
        putIfPresent(context, "traceId");
        putIfPresent(context, "spanId");
        return context;
    }

    private static void putIfPresent(Map<String, String> context, String key) {
        String value = MDC.get(key);
        if (value != null && !value.isBlank()) {
            context.put(key, value);
        }
    }
}
