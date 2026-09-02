package com.demo.order.config;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import com.demo.events.avro.AvroEventMapper;
import io.namastack.outbox.kafka.KafkaOutboxRouting;
import io.namastack.outbox.routing.selector.OutboxPayloadSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps outbox payload types onto Kafka topics, keys and headers.
 *
 * <p>Declaring this bean replaces namastack's default routing, which would otherwise send
 * everything to {@code namastack.outbox.kafka.default-topic}.
 *
 * <p>Also where the wire format is decided. The outbox row stores the domain record as Jackson
 * JSON; at relay time {@code route.mapping(...)} converts it to the generated Avro
 * {@code SpecificRecord}, which is what {@code KafkaAvroSerializer} receives. namastack resolves the
 * topic, key and headers from the <em>original</em> payload before applying the mapping, so those
 * lambdas keep working on the domain records. Because the mapping is a pure function of the stored
 * record, a retry or a replayed duplicate produces byte-identical Avro — the property the inbox
 * relies on.
 *
 * <p>The library is written in Kotlin but exposes a Java-friendly surface deliberately:
 * {@code builder()} is {@code @JvmStatic}, and {@code route(...)}/{@code defaults(...)} have
 * {@code Consumer<OutboxRoute.Builder>} overloads alongside the Kotlin-lambda versions (which are
 * {@code @JvmSynthetic}, so they are invisible from Java). No Kotlin plugin or interop shim needed.
 */
@Configuration
public class KafkaOutboxRoutingConfig {

    @Bean
    public KafkaOutboxRouting kafkaOutboxRouting() {
        return KafkaOutboxRouting.builder()
                .route(
                        OutboxPayloadSelector.type(OrderCreated.class),
                        route -> {
                            route.target("orders.v1");
                            // Kafka key == outbox ordering key, so per-order ordering survives
                            // all the way to the consumer's partition assignment.
                            route.key((payload, metadata) -> ((OrderCreated) payload).orderId());
                            route.headers(
                                    (payload, metadata) ->
                                            headers(
                                                    "OrderCreated",
                                                    ((OrderCreated) payload).eventId(),
                                                    metadata.getContext()));
                            route.mapping((payload, metadata) -> AvroEventMapper.toAvro(payload));
                        })
                .route(
                        OutboxPayloadSelector.type(OrderCancelled.class),
                        route -> {
                            route.target("orders.v1.cancelled");
                            route.key((payload, metadata) -> ((OrderCancelled) payload).orderId());
                            route.headers(
                                    (payload, metadata) ->
                                            headers(
                                                    "OrderCancelled",
                                                    ((OrderCancelled) payload).eventId(),
                                                    metadata.getContext()));
                            route.mapping((payload, metadata) -> AvroEventMapper.toAvro(payload));
                        })
                // Anything without an explicit route still gets published rather than silently
                // stranded in the outbox table. It goes through the same mapper: a payload with
                // no Avro schema then fails with a message naming its class, instead of the
                // serializer's opaque "Unsupported Avro type" ten retries in a row.
                //
                // The braces are load-bearing. Builder.route/defaults expose both a Kotlin
                // Function1 overload and a java.util.function.Consumer one, and unlike the inner
                // route-builder methods these are not @JvmSynthetic, so Java sees both. An
                // expression lambda is ambiguous between them; a block-bodied lambda is
                // void-compatible only, which resolves to the Consumer overload.
                .defaults(
                        route -> {
                            route.target("domain-events");
                            route.mapping((payload, metadata) -> AvroEventMapper.toAvro(payload));
                        })
                .build();
    }

    /**
     * Copies the event id and correlation ids into Kafka headers.
     *
     * <p>The {@code eventId} header is redundant with the payload field — consumers dedupe on the
     * payload — but it lets an operator spot duplicates in Conduktor without deserializing, and
     * lets infrastructure route or filter without parsing the body.
     */
    private static Map<String, String> headers(
            String eventType, String eventId, Map<String, String> outboxContext) {

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("eventType", eventType);
        headers.put("eventId", eventId);

        // Populated by TracingContextProvider on every outbox record.
        copyIfPresent(outboxContext, headers, "traceId");
        copyIfPresent(outboxContext, headers, "spanId");
        return headers;
    }

    private static void copyIfPresent(
            Map<String, String> source, Map<String, String> target, String key) {
        String value = source.get(key);
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }
}
