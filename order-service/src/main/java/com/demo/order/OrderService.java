package com.demo.order;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import io.namastack.outbox.Outbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Business logic, plus a side-by-side demonstration of the two ways to write to the outbox.
 *
 * <p>Both paths share the property that matters: the event row is written by the same transaction
 * that writes the business row. There is no window in which an order exists without its event, or
 * an event exists for an order that was rolled back.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final Outbox outbox;
    private final ApplicationEventPublisher eventPublisher;

    public OrderService(
            OrderRepository orderRepository,
            Outbox outbox,
            ApplicationEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.outbox = outbox;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Explicit producer path: {@code outbox.schedule(payload, key)}.
     *
     * <p>The key is {@code order-<id>}: the outbox key, i.e. the aggregate id as namastack sees it
     * (see {@code OrderEvent#orderId}). namastack processes records sharing a key strictly
     * sequentially, so every event for one order is relayed in the order it was scheduled. The
     * routing then publishes them all to one topic under one Kafka key, the bare order id, which
     * Kafka uses as the partition key (see
     * {@code KafkaOutboxRoutingConfig}), so they share a partition and the consumer sees them in
     * that same order.
     */
    @Transactional
    public OrderEntity createOrder(String customerId, BigDecimal totalAmount) {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        OrderCreated event = OrderCreated.of(orderId, customerId, totalAmount);

        OrderEntity order =
                orderRepository.save(
                        new OrderEntity(orderId, customerId, totalAmount, event.eventId()));

        outbox.schedule(
                event,
                orderKey(orderId),
                Map.of("customerId", customerId, "orderTotal", totalAmount.toPlainString()));

        log.info("order created id={} outboxEventId={}", orderId, event.eventId());
        return order;
    }

    /**
     * Re-schedules an existing order's {@link OrderCreated} under its original event id.
     *
     * <p>This is the demo stand-in for the one case the outbox cannot rule out: the relay sends to
     * Kafka successfully but crashes before marking the record complete, so on recovery it sends the
     * same event again. The payload is rebuilt from the order row, so it is not byte-identical to
     * the original ({@code occurredAt} comes from {@code created_at}). Only the event id is
     * guaranteed to match, and that is the one field the consumer's inbox compares.
     */
    @Transactional
    public OrderCreated replayCreatedEvent(String orderId) {
        OrderEntity order =
                orderRepository
                        .findById(orderId)
                        .orElseThrow(() -> new OrderNotFoundException(orderId));

        OrderCreated duplicate =
                new OrderCreated(
                        order.getCreatedEventId(),
                        order.getId(),
                        order.getCustomerId(),
                        order.getTotalAmount(),
                        order.getCreatedAt());

        outbox.schedule(duplicate, orderKey(orderId));

        log.warn(
                "DEMO: re-scheduled duplicate of eventId={} for order={} — the inbox should suppress it",
                duplicate.eventId(),
                orderId);
        return duplicate;
    }

    /**
     * Declarative producer path: a plain Spring application event.
     *
     * <p>{@link OrderCancelled} is annotated {@code @OutboxEvent}, so namastack's multicaster
     * intercepts the publication and persists an outbox record inside this transaction. Note that
     * this method never references the outbox API — the domain layer stays free of messaging
     * concerns, at the cost of the persistence being implicit.
     */
    @Transactional
    public OrderEntity cancelOrder(String orderId, String reason) {
        OrderEntity order =
                orderRepository
                        .findById(orderId)
                        .orElseThrow(() -> new OrderNotFoundException(orderId));

        order.cancel();
        orderRepository.save(order);

        OrderCancelled event = OrderCancelled.of(orderId, reason);
        eventPublisher.publishEvent(event);

        log.info("order cancelled id={} outboxEventId={} reason={}", orderId, event.eventId(), reason);
        return order;
    }

    @Transactional(readOnly = true)
    public List<OrderEntity> findAll() {
        return orderRepository.findAll();
    }

    @Transactional(readOnly = true)
    public OrderEntity findById(String orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private static String orderKey(String orderId) {
        return "order-" + orderId;
    }
}
