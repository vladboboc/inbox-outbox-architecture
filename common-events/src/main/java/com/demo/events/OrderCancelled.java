package com.demo.events;

import io.namastack.outbox.annotation.OutboxEvent;
import io.namastack.outbox.annotation.OutboxEvent.OutboxContextEntry;

import java.time.Instant;
import java.util.UUID;

/**
 * Published when an order is cancelled.
 *
 * <p>This one demonstrates the <em>declarative</em> producer path: the domain code just calls
 * {@code ApplicationEventPublisher.publishEvent(...)} and never touches the outbox API.
 * namastack's multicaster sees the {@code @OutboxEvent} annotation and persists the record
 * inside the caller's transaction before any listener runs.
 *
 * <p>{@code key} is a SpEL expression evaluated against the event instance, so cancellations
 * share the {@code orderId} ordering key with {@link OrderCreated}. The {@code context} entries
 * are event-specific metadata stored on the outbox row; cross-cutting values such as
 * {@code traceId} come from an {@code OutboxContextProvider} bean instead, so they don't have to
 * be repeated on every annotation.
 */
@OutboxEvent(
        // Must produce exactly the same key as OrderService's explicit path ("order-" + id).
        // namastack serialises records per key, so a bare "#this.orderId" here would put
        // cancellations in a different ordering group from creations for the same order — the two
        // could then be relayed concurrently and a cancellation could overtake its own creation.
        key = "'order-' + #this.orderId",
        context = {
            @OutboxContextEntry(key = "reason", value = "#this.reason"),
            @OutboxContextEntry(key = "eventType", value = "'OrderCancelled'")
        })
public record OrderCancelled(String eventId, String orderId, String reason, Instant occurredAt)
        implements OrderEvent {

    public static OrderCancelled of(String orderId, String reason) {
        return new OrderCancelled(UUID.randomUUID().toString(), orderId, reason, Instant.now());
    }
}
