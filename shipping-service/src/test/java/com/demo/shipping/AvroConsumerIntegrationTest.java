package com.demo.shipping;

import com.demo.events.OrderCreated;
import com.demo.events.avro.AvroEventMapper;
import com.demo.inbox.InboxMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The consumer's wire contract, end to end: Confluent Avro bytes on a real broker, through
 * {@code ErrorHandlingDeserializer -> KafkaAvroDeserializer}, the inbox guard, and the service.
 *
 * <p>{@link InboxDeduplicationTest} deliberately bypasses Kafka to test the claim transaction; this
 * class covers what it leaves out — that the serializer configuration on both sides actually agrees.
 * Producer and consumer share the {@code mock://shipping-test} registry scope from
 * {@code application-test.yml}, so schema ids resolve without a Schema Registry container.
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=true")
@Testcontainers
@ActiveProfiles("test")
class AvroConsumerIntegrationTest {

    private static final String CONSUMER = "shipping-service";

    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.10");

    @Container
    @ServiceConnection
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:8.2.2");

    /** The application's own template, i.e. the DelegatingByTypeSerializer-backed DLT producer. */
    @Autowired KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired ShipmentRepository shipmentRepository;
    @Autowired InboxMessageRepository inboxMessageRepository;
    @Autowired KafkaListenerEndpointRegistry listenerRegistry;

    @BeforeEach
    void resetStateAndWaitForAssignment() {
        shipmentRepository.deleteAll();
        inboxMessageRepository.deleteAll();
        // Sending before the group has partitions would still work (auto-offset-reset=earliest),
        // but waiting keeps the timing deterministic.
        for (String id : List.of("orders", "orders-dlt")) {
            MessageListenerContainer container = listenerRegistry.getListenerContainer(id);
            assertThat(container).as("listener container %s", id).isNotNull();
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    @Test
    void avroEventIsConsumedOnceEvenWhenDeliveredTwice() throws Exception {
        // Scale 1 on purpose: the mapper must normalise to the schema's scale 2.
        OrderCreated event = OrderCreated.of("ORD-AVRO-1", "CUST-1", new BigDecimal("149.9"));
        var avro = AvroEventMapper.toAvro(event);

        kafkaTemplate.send("orders.v1", event.orderId(), avro).get(15, TimeUnit.SECONDS);
        kafkaTemplate.send("orders.v1", event.orderId(), avro).get(15, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(
                        () -> assertThat(inboxMessageRepository.countByConsumer(CONSUMER))
                                .as("one claim for two deliveries of the same eventId")
                                .isEqualTo(1));

        List<Shipment> shipments = shipmentRepository.findByOrderId("ORD-AVRO-1");
        assertThat(shipments).hasSize(1);
        assertThat(shipments.getFirst().getCustomerId()).isEqualTo("CUST-1");
        assertThat(shipments.getFirst().getOrderTotal()).isEqualByComparingTo("149.90");
    }

    @Test
    void poisonEventIsDeadLetteredAsAvroAndLeavesNoClaim() throws Exception {
        OrderCreated poison =
                OrderCreated.of("ORD-AVRO-POISON", ShipmentService.POISON_CUSTOMER_ID, new BigDecimal("1.00"));

        kafkaTemplate.send("orders.v1", poison.orderId(), AvroEventMapper.toAvro(poison))
                .get(15, TimeUnit.SECONDS);

        // The recoverer receives the deserialized SpecificRecord, so the DelegatingByTypeSerializer
        // must route it to KafkaAvroSerializer: the dead letter starts with Confluent's magic byte.
        try (KafkaConsumer<String, byte[]> raw = rawConsumer()) {
            raw.subscribe(List.of("orders.v1.DLT"));
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(
                            () -> {
                                ConsumerRecords<String, byte[]> records = raw.poll(Duration.ofMillis(500));
                                assertThat(records.count()).isPositive();
                                var record = records.iterator().next();
                                assertThat(record.key()).isEqualTo(poison.orderId());
                                assertThat(record.value()[0]).as("Confluent magic byte").isEqualTo((byte) 0);
                                // The diagnostics the DLT listener logs come from these headers; the
                                // recoverer writes kafka_dlt-*, not the retry-topic kafka_original-*.
                                assertThat(headerText(record, KafkaHeaders.DLT_ORIGINAL_TOPIC))
                                        .isEqualTo("orders.v1");
                                assertThat(headerText(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                                        .isEqualTo(IllegalStateException.class.getName());
                                assertThat(headerText(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                                        .contains(ShipmentService.POISON_CUSTOMER_ID);
                            });
        }

        // Every attempt rolled back with the exception, so the inbox has no record of the event.
        assertThat(inboxMessageRepository.countByConsumer(CONSUMER)).isZero();
        assertThat(shipmentRepository.findByOrderId("ORD-AVRO-POISON")).isEmpty();
    }

    private static String headerText(ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static KafkaConsumer<String, byte[]> rawConsumer() {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "dlt-inspector",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),
                new StringDeserializer(),
                new ByteArrayDeserializer());
    }
}
