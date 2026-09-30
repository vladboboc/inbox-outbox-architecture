package com.demo.shipping;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import com.demo.inbox.InboxGuard;
import com.demo.inbox.InboxMessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
// Testcontainers 2.x moved the container classes into per-module packages; the old
// org.testcontainers.containers.* locations are legacy.
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The behaviour the whole consumer side depends on: a repeated event must not do the work twice, and
 * a failed attempt must not leave a claim behind.
 *
 * <p>Kafka is deliberately absent here. These are properties of the claim-plus-write transaction, and
 * testing them directly is both faster and a sharper signal than driving them through a broker.
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class InboxDeduplicationTest {

    // Testcontainers 2.x dropped the self-type generic: these classes are no longer parameterized.
    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.10");

    private static final String CONSUMER = "shipping-service";

    @Autowired InboxGuard inboxGuard;
    @Autowired OrderEventListener orderEventListener;
    @Autowired ShipmentService shipmentService;
    @Autowired ShipmentRepository shipmentRepository;
    @Autowired InboxMessageRepository inboxMessageRepository;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired MeterRegistry meterRegistry;

    @BeforeEach
    void reset() {
        shipmentRepository.deleteAll();
        inboxMessageRepository.deleteAll();
    }

    /** Mirrors OrderEventListener: claim, then act, in one transaction. */
    private boolean handle(OrderCreated event) {
        return Boolean.TRUE.equals(
                transactionTemplate.execute(
                        status -> {
                            if (!inboxGuard.claim(event.eventId(), CONSUMER, "orders.v1", 0, 1L)) {
                                return false;
                            }
                            shipmentService.createShipment(event);
                            return true;
                        }));
    }

    /** Only the claim, in its own transaction, recorded under {@code consumer}. */
    private boolean claimAs(OrderCreated event, String consumer) {
        return Boolean.TRUE.equals(
                transactionTemplate.execute(
                        status ->
                                inboxGuard.claim(event.eventId(), consumer, "orders.v1", 0, 1L)));
    }

    @Test
    void redeliveryOfTheSameEventCreatesOnlyOneShipment() {
        OrderCreated event = OrderCreated.of("ORD-1", "CUST-1", new BigDecimal("99.99"));

        assertThat(handle(event)).as("first delivery is accepted").isTrue();
        assertThat(handle(event)).as("second delivery is suppressed").isFalse();
        assertThat(handle(event)).as("third delivery is suppressed").isFalse();

        assertThat(shipmentRepository.findByOrderId("ORD-1")).hasSize(1);
        assertThat(inboxMessageRepository.countByConsumer(CONSUMER)).isEqualTo(1);
    }

    @Test
    void distinctEventsForTheSameOrderAreBothProcessed() {
        // Guards against over-eager deduplication: the key is the event, not the aggregate.
        // Each of() call mints a fresh eventId.
        OrderCreated first = OrderCreated.of("ORD-2", "CUST-1", new BigDecimal("10.00"));
        OrderCreated second = OrderCreated.of("ORD-2", "CUST-1", new BigDecimal("20.00"));

        assertThat(handle(first)).isTrue();
        assertThat(handle(second)).isTrue();

        assertThat(shipmentRepository.findByOrderId("ORD-2")).hasSize(2);
    }

    @Test
    void aFailedAttemptLeavesNoClaimSoTheRetryIsTreatedAsFirstDelivery() {
        OrderCreated poison =
                OrderCreated.of("ORD-3", ShipmentService.POISON_CUSTOMER_ID, new BigDecimal("1.00"));

        assertThatThrownBy(() -> handle(poison)).isInstanceOf(IllegalStateException.class);

        // This is the property that keeps failures recoverable: had the claim survived the rollback,
        // the event would be permanently marked as handled and silently lost.
        assertThat(inboxMessageRepository.count()).isZero();
        assertThat(shipmentRepository.count()).isZero();
    }

    @Test
    void onlyAttemptsThatCommitAreCountedAsProcessed() {
        // Counters are not transactional. A poison event's claim is inserted and then rolled back;
        // counting at claim time would report work that never happened, once per retry.
        double before = processedCount();

        handle(OrderCreated.of("ORD-6", "CUST-1", new BigDecimal("3.00")));
        OrderCreated poison =
                OrderCreated.of("ORD-7", ShipmentService.POISON_CUSTOMER_ID, new BigDecimal("1.00"));
        assertThatThrownBy(() -> handle(poison)).isInstanceOf(IllegalStateException.class);

        assertThat(processedCount() - before).isEqualTo(1.0);
    }

    private double processedCount() {
        return meterRegistry.find("inbox.messages.processed").tag("consumer", CONSUMER).counters()
                .stream()
                .mapToDouble(Counter::count)
                .sum();
    }

    @Test
    void twoConsumersEachProcessTheSameEventOnce() {
        // Fan-out: the ledger is per consumer, so a second consumer is not starved by the first.
        OrderCreated event = OrderCreated.of("ORD-4", "CUST-1", new BigDecimal("5.00"));

        assertThat(claimAs(event, "shipping-service")).isTrue();
        assertThat(claimAs(event, "analytics-service")).isTrue();
        assertThat(claimAs(event, "analytics-service")).isFalse();
    }

    @Test
    void aCancellationFollowingItsCreationCancelsTheShipment() {
        // Through the real listener, which takes both event types from the one topic. They share
        // a partition, so the cancellation always follows its creation, as it does here.
        OrderCreated created = OrderCreated.of("ORD-5", "CUST-1", new BigDecimal("7.00"));
        OrderCancelled cancelled = OrderCancelled.of("ORD-5", "changed mind");

        orderEventListener.onOrderEvent(created, "orders.v1", 0, 10L);
        orderEventListener.onOrderEvent(cancelled, "orders.v1", 0, 11L);

        assertThat(shipmentRepository.findByOrderId("ORD-5"))
                .singleElement()
                .extracting(Shipment::getStatus)
                .isEqualTo(Shipment.Status.CANCELLED);
    }

    @Test
    void claimingWithoutATransactionIsRejected() {
        // Propagation.MANDATORY turns a dangerous mistake into an obvious one: a claim committing on
        // its own could mark an event processed while the business write is still pending.
        assertThatThrownBy(
                        () ->
                                inboxGuard.claim(
                                        UUID.randomUUID().toString(), CONSUMER, "orders.v1", 0, 1L))
                .isInstanceOf(IllegalTransactionStateException.class);
    }
}
