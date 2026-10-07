package com.demo.shipping;

import com.demo.events.OrderCancelled;
import com.demo.events.OrderCreated;
import com.demo.events.OrderEvent;
import com.demo.events.avro.AvroEventMapper;
import com.demo.inbox.InboxGuard;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Consumes order events with an inbox guard in front of the handler.
 *
 * <p>Creations and cancellations share one topic and one key per order, so both events for an
 * order sit on the same partition and arrive in the order they were published: a cancellation
 * cannot overtake its own creation. One listener therefore handles both types.
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
            id = "orders",
            topics = "orders.v1",
            groupId = CONSUMER,
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onOrderEvent(
            @Payload SpecificRecord payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        OrderEvent event = AvroEventMapper.fromAvro(payload);
        if (!inboxGuard.claim(event.eventId(), CONSUMER, topic, partition, offset)) {
            return;
        }
        // Exhaustive over the sealed OrderEvent: a new event type does not compile until it is
        // handled here.
        switch (event) {
            case OrderCreated created -> shipmentService.createShipment(created);
            case OrderCancelled cancelled -> shipmentService.cancelShipment(cancelled);
        }
    }

    /**
     * Observation point for the dead-letter path. Anything that lands here has exhausted its
     * retries; in a real system this is where an operator alert would be raised.
     *
     * <p>Takes the raw {@link ConsumerRecord} rather than {@code @Header} parameters. The
     * {@code kafka_dlt-*} headers the recoverer adds arrive as raw {@code byte[]} and are not
     * reliably converted for annotated parameters, which produced log lines reading
     * "topic=unknown ... null" — worse than useless on the one path where diagnostics matter most.
     *
     * <p>The header names are the {@code DLT_*} constants. {@code KafkaHeaders.ORIGINAL_TOPIC},
     * {@code EXCEPTION_FQCN} and friends look right but belong to the retry-topic feature
     * ({@code kafka_original-*}); {@code DeadLetterPublishingRecoverer} writes
     * {@code kafka_dlt-original-*} and {@code kafka_dlt-exception-*}, and reading the wrong set
     * silently yields "n/a" for every field.
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
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, OrderEventListener::utf8),
                header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer::getInt),
                header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer::getLong),
                header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, OrderEventListener::utf8),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE, OrderEventListener::utf8));
    }

    /**
     * Decodes one {@code kafka_dlt-*} header. The recoverer writes the original partition as a
     * big-endian int, the original offset as a big-endian long and everything else as UTF-8, so
     * each caller names its decoding rather than guessing from the value's length.
     */
    private static Object header(
            ConsumerRecord<?, ?> record, String name, Function<ByteBuffer, ?> decoder) {
        var header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return "n/a";
        }
        try {
            return decoder.apply(ByteBuffer.wrap(header.value()));
        } catch (BufferUnderflowException notFromTheRecoverer) {
            // Too short for its type, so some other producer wrote it. This listener only logs,
            // and a throw here would send the record through retries and dead-lettering itself.
            return "malformed";
        }
    }

    private static String utf8(ByteBuffer value) {
        return StandardCharsets.UTF_8.decode(value).toString();
    }
}
