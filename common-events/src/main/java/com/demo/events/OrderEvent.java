package com.demo.events;

import java.time.Instant;

/**
 * Contract for everything published on the order topics.
 *
 * <p>{@code eventId} is the idempotency key the consumer-side inbox dedupes on. It is
 * deliberately part of the <em>payload</em> rather than derived from outbox infrastructure:
 * {@code OutboxRecordMetadata} does not expose the outbox record id, and more importantly the
 * event id belongs to the published contract. Because the outbox stores the serialized payload
 * once and replays that same bytes on retry, every duplicate delivery of a given event carries
 * an identical {@code eventId} — which is exactly what makes inbox dedup sound.
 *
 * <p>Sealed so the compiler can check exhaustiveness when switching over event types.
 */
public sealed interface OrderEvent permits OrderCreated, OrderCancelled {

    /** Stable, globally unique id for this event. The inbox idempotency key. */
    String eventId();

    /**
     * Aggregate id. The outbox ordering key ({@code "order-" + orderId}) and the Kafka message key
     * ({@code orderId}) are both derived from it, which pins ordering and partitioning per order.
     */
    String orderId();

    /** When the business fact occurred, not when it was published. */
    Instant occurredAt();
}
