package com.demo.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published when an order is accepted.
 *
 * <p>Written to the outbox through the <em>explicit</em> API
 * ({@code outbox.schedule(payload, key)}) inside the same transaction as the order row.
 */
public record OrderCreated(
        String eventId,
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        Instant occurredAt) implements OrderEvent {

    public static OrderCreated of(String orderId, String customerId, BigDecimal totalAmount) {
        return new OrderCreated(
                UUID.randomUUID().toString(), orderId, customerId, totalAmount, Instant.now());
    }
}
