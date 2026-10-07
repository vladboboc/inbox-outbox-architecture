package com.demo.order;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Producer-side guarantees against a real Postgres and a real broker.
 *
 * <p>The claims checked: that the event cannot outlive a rolled-back business transaction, that a
 * committed event does actually reach Kafka, that one order's events reach Kafka on one partition
 * in the order they happened, and that a record carries the trace of the request that scheduled
 * it. None can be established with mocks — the first is a property of the database transaction,
 * the others of the relay and the broker.
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class OutboxAtomicityTest {

    // Testcontainers 2.x dropped the self-type generic: these classes are no longer parameterized.
    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.10");

    // Three partitions per auto-created topic, as docker/kafka/create-topics.sh provisions them, so
    // "one partition" in the ordering test is a real assertion rather than the only possibility.
    @Container
    @ServiceConnection
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.2")
                    .withEnv("KAFKA_NUM_PARTITIONS", "3");

    @Autowired OrderService orderService;
    @Autowired OrderRepository orderRepository;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired TransactionTemplate transactionTemplate;
    @Autowired Tracer tracer;

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

    @Test
    void creationAndCancellationShareAPartitionAndArriveInOrder() {
        // The relay sequences one order's events, but Kafka preserves a sequence only within one
        // topic-partition. This pins the routing that keeps both events there — one topic, one key.
        // With cancellations on a topic of their own, a cancellation could be consumed before its
        // creation and find no shipment to cancel.
        OrderEntity order = orderService.createOrder("CUST-SEQ", new BigDecimal("12.00"));
        orderService.cancelOrder(order.getId(), "changed mind");

        List<ConsumerRecord<String, String>> published = publishedFor(order.getId(), 2);

        assertThat(published)
                .extracting(record -> header(record, "eventType"))
                .as("both events on orders.v1, creation first")
                .containsExactly("OrderCreated", "OrderCancelled");
        assertThat(published)
                .extracting(ConsumerRecord::partition)
                .containsOnly(published.getFirst().partition());
        assertThat(partitionCount("orders.v1")).as("with one partition this proves nothing").isEqualTo(3);
    }

    @Test
    void thePublishedRecordCarriesTheTraceOfTheRequestThatScheduledIt() {
        // The relay publishes from a poller thread with no request context, yet the trace must
        // arrive: namastack stores it on the outbox row at schedule time and restores it around
        // the send, and the KafkaTemplate's observation writes it out as a traceparent header.
        Span request = tracer.nextSpan().name("test-request").start();
        OrderEntity order;
        try (Tracer.SpanInScope _ = tracer.withSpan(request)) {
            order = orderService.createOrder("CUST-TRACE", new BigDecimal("5.00"));
        } finally {
            request.end();
        }

        List<ConsumerRecord<String, String>> published = publishedFor(order.getId(), 1);

        assertThat(published).hasSize(1);
        assertThat(published.getFirst().headers().lastHeader("traceparent"))
                .as("W3C trace context on the Kafka record")
                .isNotNull();
        // traceparent is version-traceId-parentId-flags.
        String traceparent = header(published.getFirst(), "traceparent");
        assertThat(traceparent.split("-")[1]).isEqualTo(request.context().traceId());
    }

    /**
     * Reads {@code orders.v1} from the start until {@code expected} records keyed by
     * {@code orderId} have arrived, or a minute has passed.
     */
    private static List<ConsumerRecord<String, String>> publishedFor(String orderId, int expected) {
        List<ConsumerRecord<String, String>> published = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig())) {
            consumer.subscribe(List.of("orders.v1"));
            Instant deadline = Instant.now().plusSeconds(60);
            while (published.size() < expected && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (orderId.equals(record.key())) {
                        published.add(record);
                    }
                }
            }
        }
        return published;
    }

    private static int partitionCount(String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig())) {
            return consumer.partitionsFor(topic).size();
        }
    }

    private static Map<String, Object> consumerConfig() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
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
