package com.demo.order.config;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import com.demo.events.OrderEvent;
import com.demo.events.avro.AvroEventMapper;
import com.demo.events.avro.AvroTrust;
import io.namastack.outbox.kafka.KafkaOutboxRouting;
import io.namastack.outbox.routing.selector.OutboxPayloadSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps outbox payloads onto a Kafka topic, key and headers.
 *
 * <p>Declaring this bean replaces namastack's default routing, which would otherwise send
 * everything to {@code namastack.outbox.kafka.default-topic}.
 *
 * <p>Every order event goes to one topic, {@code orders.v1}, keyed by the bare {@code orderId}.
 * The single topic is what carries per-order ordering through to the consumer. The relay
 * publishes one order's events in sequence, because they share the outbox key
 * {@code "order-" + orderId} (see {@code OrderService}), but Kafka preserves that sequence only
 * within one topic-partition. With cancellations on a topic of their own, a cancellation could be
 * consumed before its own creation. (The Kafka key is the partition key; the outbox key is
 * namastack's aggregate id. They are different strings, but both derive from the order id alone;
 * see {@code OrderEvent#orderId}.) Two Avro record types on one topic is why the producer sets
 * {@code value.subject.name.strategy=RecordNameStrategy}; see {@code application.yml}.
 *
 * <p>Also where the wire format is decided. The outbox row stores the domain record as Jackson
 * JSON; at relay time {@code route.mapping(...)} converts it to the generated Avro
 * {@code SpecificRecord}, which is what {@code KafkaAvroSerializer} receives. namastack resolves the
 * topic, key and headers from the <em>original</em> payload before applying the mapping, so those
 * lambdas keep working on the domain records. Because the mapping is a pure function of the stored
 * record, a retry or a replayed duplicate produces byte-identical Avro — the property the inbox
 * relies on.
 *
 * <p>Trace context crosses the outbox without any code here. The record is written during the
 * HTTP request but published later, from a poller thread that has no request context of its own,
 * so namastack's observability module stores the W3C trace context on the outbox row at schedule
 * time and restores it around the relay's send. The KafkaTemplate's observation
 * ({@code spring.kafka.template.observation-enabled}) then writes a {@code traceparent} header
 * that carries the trace id of the originating request.
 *
 * <p>The lambdas passed to {@code route(...)} and {@code defaults(...)} are block-bodied, and the
 * braces are load-bearing. namastack is written in Kotlin, and {@code KafkaOutboxRouting.Builder}
 * exposes both a Kotlin {@code Function1} overload and a {@code java.util.function.Consumer} one
 * for those two methods. An expression lambda is ambiguous between them; a block-bodied lambda is
 * void-compatible only, which resolves to the {@code Consumer} overload. The methods inside a
 * route ({@code target}, {@code key}, {@code headers}, {@code mapping}) hide their Kotlin
 * overloads with {@code @JvmSynthetic}, so plain expression lambdas work there.
 */
@Configuration
public class KafkaOutboxRoutingConfig {

    static {
        // The serializer resolves the schema to its generated class by name (the mock:// registry
        // used in tests does so on every send), which Avro 1.12.2 rejects unless allow-listed.
        AvroTrust.trustEventSchemas();
    }

    @Bean
    public KafkaOutboxRouting kafkaOutboxRouting() {
        return KafkaOutboxRouting.builder()
                .route(
                        OutboxPayloadSelector.type(OrderEvent.class),
                        route -> {
                            route.target("orders.v1");
                            route.key((payload, metadata) -> ((OrderEvent) payload).orderId());
                            route.headers((payload, metadata) -> headers((OrderEvent) payload));
                            route.mapping((payload, metadata) -> AvroEventMapper.toAvro(payload));
                        })
                // Anything without an explicit route still gets published rather than silently
                // stranded in the outbox table. It goes through the same mapper: a payload with
                // no Avro schema then fails with a message naming its class, instead of the
                // serializer's opaque "Unsupported Avro type" ten retries in a row.
                .defaults(
                        route -> {
                            route.target("domain-events");
                            route.mapping((payload, metadata) -> AvroEventMapper.toAvro(payload));
                        })
                .build();
    }

    /**
     * The event type and event id as Kafka headers. No trace headers are added here: the
     * KafkaTemplate's observation writes {@code traceparent} itself.
     *
     * <p>The {@code eventId} header is redundant with the payload field — consumers dedupe on the
     * payload — but it lets an operator spot duplicates in Conduktor without deserializing, and
     * lets infrastructure route or filter without parsing the body.
     */
    private static Map<String, String> headers(OrderEvent event) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("eventType", eventType(event));
        headers.put("eventId", event.eventId());
        return headers;
    }

    /**
     * Spelled out rather than taken from {@code getSimpleName()}, so renaming a class cannot
     * silently change what goes on the wire. The switch is exhaustive over the sealed
     * {@link OrderEvent}, so a new event type does not compile until it is named here.
     */
    private static String eventType(OrderEvent event) {
        return switch (event) {
            case OrderCreated _ -> "OrderCreated";
            case OrderCancelled _ -> "OrderCancelled";
        };
    }
}
