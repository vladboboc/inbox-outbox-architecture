package com.demo.events;

import java.time.Instant;

/**
 * Contract for everything published on the order topics.
 *
 * <p>{@code eventId} is the idempotency key the consumer-side inbox dedupes on. It is
 * deliberately part of the <em>payload</em> rather than derived from outbox infrastructure:
 * {@code OutboxRecordMetadata} does not expose the outbox record id, and more importantly the
 * event id belongs to the published contract. Because the outbox stores the record once and the
 * relay maps it to Avro with a pure function on every attempt, every duplicate delivery of a given
 * event carries an identical {@code eventId} — which is exactly what makes inbox dedup sound.
 *
 * <p>These records are the in-process and outbox-table form. The wire form is the Avro schema of
 * the same name in {@code src/main/avro}, bridged by {@code com.demo.events.avro.AvroEventMapper}.
 *
 * <p>Sealed so the compiler can check exhaustiveness when switching over event types.
 */
public sealed interface OrderEvent permits OrderCreated, OrderCancelled {

    /** Stable, globally unique id for this event. The inbox idempotency key. */
    String eventId();

    /**
     * Aggregate id: the one value both keys below are derived from. They are two different
     * strings with two different jobs, so never join or dedupe on one using the other.
     *
     * <ul>
     *   <li><b>Outbox key</b> = {@code "order-" + orderId}. In namastack the key is the aggregate
     *       id: records sharing it are relayed strictly one at a time, in the order they were
     *       scheduled. It orders the <em>relay</em> (outbox table to Kafka). Both producer paths
     *       must build it identically, see {@code OrderService} and {@code OrderCancelled}.
     *   <li><b>Kafka key</b> = the bare {@code orderId}, set in {@code KafkaOutboxRoutingConfig}.
     *       Kafka hashes the record key to pick a partition, so every event of one order lands on
     *       the same partition and is consumed in publish order. It orders the
     *       <em>consumption</em> (Kafka to listener).
     * </ul>
     */
    String orderId();

    /** When the business fact occurred, not when it was published. */
    Instant occurredAt();
}
