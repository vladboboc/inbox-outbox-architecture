package com.demo.shipping;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import com.demo.events.avro.AvroEventMapper;
import com.demo.inbox.InboxGuard;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
// org.apache.kafka.common.header.Header is referenced fully qualified below: importing it would
// clash with the @Header annotation used on the listener parameters.
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Consumes order events with an inbox guard in front of every handler.
 *
 * <p>The shape is always the same: claim, then act, in one transaction.
 *
 * <ul>
 *   <li>First delivery — the claim inserts, the business write happens, both commit together.
 *   <li>Duplicate delivery — the claim finds an existing row, the method returns, the offset is
 *       committed. No second shipment.
 *   <li>Handler throws — claim and business write roll back as one, so the redelivery is treated as
 *       a genuine first attempt rather than being written off as a duplicate.
 * </ul>
 *
 * <p>{@code @Transactional} covers the database only. The Kafka offset commit is a separate,
 * non-atomic step, and that is exactly the gap the inbox exists to cover: if the offset commit
 * fails after the database commit, Kafka redelivers and the claim turns the replay into a no-op.
 *
 * <p>Payloads arrive as the generated Avro classes ({@code com.demo.events.avro.*}, materialised
 * by {@code KafkaAvroDeserializer} with {@code specific.avro.reader}) and are mapped to the domain
 * records on the first line, so the inbox guard and {@link ShipmentService} stay format-agnostic.
 * The Avro types are referenced fully qualified: they share simple names with the domain records.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    /** Identifies this consumer in the dedup ledger; matches the Kafka consumer group. */
    private static final String CONSUMER = "shipping-service";

    private final InboxGuard inboxGuard;
    private final ShipmentService shipmentService;

    public OrderEventListener(InboxGuard inboxGuard, ShipmentService shipmentService) {
        this.inboxGuard = inboxGuard;
        this.shipmentService = shipmentService;
    }

    @KafkaListener(
            id = "orders-created",
            topics = "orders.v1",
            groupId = CONSUMER,
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onOrderCreated(
            @Payload com.demo.events.avro.OrderCreated payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderCreated event = AvroEventMapper.fromAvro(payload);
        if (!inboxGuard.claim(event.eventId(), CONSUMER, topic, partition, offset)) {
            return;
        }
        shipmentService.createShipment(event);
    }

    @KafkaListener(
            id = "orders-cancelled",
            topics = "orders.v1.cancelled",
            groupId = CONSUMER,
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onOrderCancelled(
            @Payload com.demo.events.avro.OrderCancelled payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderCancelled event = AvroEventMapper.fromAvro(payload);
        if (!inboxGuard.claim(event.eventId(), CONSUMER, topic, partition, offset)) {
            return;
        }
        shipmentService.cancelShipment(event);
    }

    /**
     * Observation point for the dead-letter path. Anything that lands here has exhausted its
     * retries; in a real system this is where an operator alert would be raised.
     *
     * <p>Takes the raw {@link ConsumerRecord} rather than {@code @Header} parameters. The
     * {@code kafka_dlt-*} headers the recoverer adds arrive as raw {@code byte[]} and are not
     * reliably converted for annotated parameters, which produced log lines reading
     * "topic=unknown ... null" — worse than useless on the one path where diagnostics matter most.
     */
    @KafkaListener(
            id = "orders-dlt",
            topics = "orders.v1.DLT",
            groupId = CONSUMER + "-dlt",
            containerFactory = "kafkaListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<?, ?> record) {
        log.error(
                "DLT: key={} originalTopic={} originalPartition={} originalOffset={} cause={}: {}",
                record.key(),
                header(record, KafkaHeaders.ORIGINAL_TOPIC),
                header(record, KafkaHeaders.ORIGINAL_PARTITION),
                header(record, KafkaHeaders.ORIGINAL_OFFSET),
                header(record, KafkaHeaders.EXCEPTION_FQCN),
                header(record, KafkaHeaders.EXCEPTION_MESSAGE));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return "n/a";
        }
        byte[] value = header.value();
        // The recoverer writes ints and longs as big-endian binary, everything else as UTF-8.
        return switch (value.length) {
            case 4 -> String.valueOf(ByteBuffer.wrap(value).getInt());
            case 8 -> String.valueOf(ByteBuffer.wrap(value).getLong());
            default -> new String(value, StandardCharsets.UTF_8);
        };
    }
}
