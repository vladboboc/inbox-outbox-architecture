package com.demo.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "orders")
public class OrderEntity {

    public enum Status {
        CREATED,
        CANCELLED
    }

    @Id
    @Column(name = "id", nullable = false, length = 64)
    private String id;

    @Column(name = "customer_id", nullable = false, length = 64)
    private String customerId;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private Status status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "cancelled_at")
    private Instant cancelledAt;

    /**
     * The event id emitted for this order's OrderCreated event.
     *
     * <p>Stored so the duplicate-delivery demo can replay the event under its original id through
     * the real outbox and Kafka path, rather than fabricating one on a side channel. To the
     * consumer's inbox, which compares only event ids, the replay is indistinguishable from the
     * redelivery an at-least-once relay would produce.
     */
    @Column(name = "created_event_id", nullable = false, length = 64)
    private String createdEventId;

    protected OrderEntity() {
        // for JPA
    }

    public OrderEntity(String id, String customerId, BigDecimal totalAmount, String createdEventId) {
        this.id = id;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.createdEventId = createdEventId;
        this.status = Status.CREATED;
        this.createdAt = Instant.now();
    }

    public void cancel() {
        if (status == Status.CANCELLED) {
            throw new IllegalStateException("order " + id + " is already cancelled");
        }
        this.status = Status.CANCELLED;
        this.cancelledAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCancelledAt() {
        return cancelledAt;
    }

    public String getCreatedEventId() {
        return createdEventId;
    }
}
