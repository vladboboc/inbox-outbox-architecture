package com.demo.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Producer-side guarantees against a real Postgres and a real broker.
 *
 * <p>Two distinct claims are checked: that the event cannot outlive a rolled-back business
 * transaction, and that a committed event does actually reach Kafka. Neither can be established with
 * mocks — the first is a property of the database transaction and the second of the relay.
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class OutboxAtomicityTest {

    // Testcontainers 2.x dropped the self-type generic: these classes are no longer parameterized.
    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.10");

    @Container
    @ServiceConnection
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.2");

    @Autowired OrderService orderService;
    @Autowired OrderRepository orderRepository;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired TransactionTemplate transactionTemplate;

    @Test
    void committedOrderWritesAnOutboxRecordAndIsEventuallyPublished() {
        OrderEntity order = orderService.createOrder("CUST-1", new BigDecimal("149.90"));

        String key = "order-" + order.getId();
        assertThat(outboxRecordCount(key)).as("record written by the business transaction").isPositive();

        // The relay publishes asynchronously; COMPLETED is only set after the broker acknowledges,
        // so reaching it proves the event genuinely left the process.
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(
                        () ->
                                assertThat(statusesFor(key))
                                        .isNotEmpty()
                                        .allSatisfy(s -> assertThat(s).isEqualTo("COMPLETED")));
    }

    @Test
    void rollbackDiscardsTheOrderAndItsEventTogether() {
        long ordersBefore = orderRepository.count();
        long recordsBefore = totalOutboxRecords();

        // createOrder is @Transactional(REQUIRED), so it joins this transaction rather than
        // starting its own — marking the outer transaction rollback-only discards both writes.
        transactionTemplate.execute(
                status -> {
                    orderService.createOrder("CUST-ROLLBACK", new BigDecimal("10.00"));
                    status.setRollbackOnly();
                    return null;
                });

        assertThat(orderRepository.count()).isEqualTo(ordersBefore);
        assertThat(totalOutboxRecords())
                .as("an event must never survive a rolled-back business transaction")
                .isEqualTo(recordsBefore);
    }

    @Test
    void bothProducerPathsShareTheSameOrderingKey() {
        // The explicit path builds "order-" + id in Java; the declarative path rebuilds it from a
        // SpEL expression in @OutboxEvent, in a different module. Nothing but this test connects the
        // two, and if they drift, namastack puts creations and cancellations into separate ordering
        // groups — a cancellation could then be relayed ahead of its own creation. The failure is
        // invisible in normal runs, which is exactly why it is pinned here.
        OrderEntity order = orderService.createOrder("CUST-ORDER", new BigDecimal("25.00"));
        orderService.cancelOrder(order.getId(), "changed mind");

        String expectedKey = "order-" + order.getId();

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(
                        () ->
                                assertThat(recordTypesFor(expectedKey))
                                        .as("both events must be keyed identically")
                                        .contains(
                                                "com.demo.events.OrderCreated",
                                                "com.demo.events.OrderCancelled"));
    }

    private List<String> recordTypesFor(String recordKey) {
        return jdbcTemplate.queryForList(
                "SELECT record_type FROM outbox_record WHERE record_key = ?",
                String.class,
                recordKey);
    }

    private Integer outboxRecordCount(String recordKey) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_record WHERE record_key = ?", Integer.class, recordKey);
    }

    private List<String> statusesFor(String recordKey) {
        return jdbcTemplate.queryForList(
                "SELECT status FROM outbox_record WHERE record_key = ?", String.class, recordKey);
    }

    private Integer totalOutboxRecords() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_record", Integer.class);
    }
}
