package com.demo.events.avro;

import com.demo.events.OrderEvent;
import org.apache.avro.specific.SpecificRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Converts between the in-process event records and their Avro wire form.
 *
 * <p>The domain records ({@link com.demo.events.OrderCreated}, {@link com.demo.events.OrderCancelled})
 * are what the business code creates and what the outbox stores. The generated Avro classes in this
 * package are what actually travels on Kafka. The producer applies {@link #toAvro(Object)} in its
 * outbox routing (at relay time, not at scheduling time), and the consumer applies
 * {@link #fromAvro} before handing the event to its service layer — so neither side's business code
 * ever sees an Avro type.
 *
 * <p>Both directions are pure functions of the input, which is what keeps duplicate deliveries
 * byte-identical: the same stored record always maps to the same Avro bytes.
 */
public final class AvroEventMapper {

    /** Matches the {@code scale} declared for {@code totalAmount} in {@code OrderCreated.avsc}. */
    private static final int AMOUNT_SCALE = 2;

    private AvroEventMapper() {}

    /**
     * Maps any outbox payload to its Avro record.
     *
     * <p>The two typed cases cover the sealed {@link OrderEvent} hierarchy; the default branch only
     * exists for payloads outside it, and turns what would otherwise be an opaque "Unsupported Avro
     * type" failure inside the serializer into a message that names the offending class.
     */
    public static SpecificRecord toAvro(Object payload) {
        return switch (payload) {
            case com.demo.events.OrderCreated e -> toAvro(e);
            case com.demo.events.OrderCancelled e -> toAvro(e);
            default ->
                    throw new IllegalArgumentException(
                            "no Avro schema for outbox payload type "
                                    + payload.getClass().getName());
        };
    }

    public static OrderCreated toAvro(com.demo.events.OrderCreated event) {
        return OrderCreated.newBuilder()
                .setEventId(event.eventId())
                .setOrderId(event.orderId())
                .setCustomerId(event.customerId())
                .setTotalAmount(normaliseAmount(event.totalAmount()))
                .setOccurredAt(event.occurredAt())
                .build();
    }

    public static OrderCancelled toAvro(com.demo.events.OrderCancelled event) {
        return OrderCancelled.newBuilder()
                .setEventId(event.eventId())
                .setOrderId(event.orderId())
                .setReason(event.reason())
                .setOccurredAt(event.occurredAt())
                .build();
    }

    public static com.demo.events.OrderCreated fromAvro(OrderCreated record) {
        return new com.demo.events.OrderCreated(
                record.getEventId(),
                record.getOrderId(),
                record.getCustomerId(),
                record.getTotalAmount(),
                record.getOccurredAt());
    }

    public static com.demo.events.OrderCancelled fromAvro(OrderCancelled record) {
        return new com.demo.events.OrderCancelled(
                record.getEventId(), record.getOrderId(), record.getReason(), record.getOccurredAt());
    }

    /**
     * Avro's decimal conversion rejects a value whose scale differs from the schema's — it does not
     * rescale. A REST caller sending {@code 149.9} produces scale 1, so normalise here, with the same
     * rounding the {@code NUMERIC(12,2)} columns apply on insert.
     */
    private static BigDecimal normaliseAmount(BigDecimal amount) {
        return amount.setScale(AMOUNT_SCALE, RoundingMode.HALF_UP);
    }
}
