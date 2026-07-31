package com.demo.shipping;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shipments")
public class Shipment {

    public enum Status {
        PENDING,
        CANCELLED
    }

    @Id
    @Column(name = "id", nullable = false, length = 64)
    private String id;

    @Column(name = "order_id", nullable = false, length = 64)
    private String orderId;

    @Column(name = "customer_id", nullable = false, length = 64)
    private String customerId;

    @Column(name = "order_total", nullable = false, precision = 12, scale = 2)
    private BigDecimal orderTotal;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private Status status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Shipment() {
        // for JPA
    }

    public Shipment(String orderId, String customerId, BigDecimal orderTotal) {
        this.id = "SHP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderTotal = orderTotal;
        this.status = Status.PENDING;
        this.createdAt = Instant.now();
    }

    public void cancel() {
        this.status = Status.CANCELLED;
    }

    public String getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getOrderTotal() {
        return orderTotal;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
