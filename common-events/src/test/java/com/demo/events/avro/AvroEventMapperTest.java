package com.demo.events.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mapper is the only code that touches both representations, so it is where a drift between
 * the Java records and the .avsc files would surface. The binary round trip matters because the
 * decimal and timestamp logical types are only exercised by an actual encode/decode, not by the
 * builder alone.
 */
class AvroEventMapperTest {

    @BeforeAll
    static void trustGeneratedClasses() {
        // Same call the services make at startup; without it every schema-to-class lookup below
        // (and in KafkaAvroDeserializer) is rejected by Avro's allow-list.
        AvroTrust.trustEventSchemas();
    }

    @Test
    void generatedClassesResolveFromTheirSchemaOnceTrusted() {
        // This is the exact lookup Confluent's deserializer performs with specific.avro.reader.
        assertThat(SpecificData.get().getClass(OrderCreated.getClassSchema()))
                .isEqualTo(OrderCreated.class);
        assertThat(SpecificData.get().getClass(OrderCancelled.getClassSchema()))
                .isEqualTo(OrderCancelled.class);
    }

    @Test
    void orderCreatedSurvivesAnAvroBinaryRoundTrip() throws IOException {
        com.demo.events.OrderCreated original =
                com.demo.events.OrderCreated.of("ORD-1", "CUST-1", new BigDecimal("149.90"));

        OrderCreated decoded = roundTrip(AvroEventMapper.toAvro(original), OrderCreated.class);
        com.demo.events.OrderCreated back = AvroEventMapper.fromAvro(decoded);

        assertThat(back.eventId()).isEqualTo(original.eventId());
        assertThat(back.orderId()).isEqualTo(original.orderId());
        assertThat(back.customerId()).isEqualTo(original.customerId());
        assertThat(back.totalAmount()).isEqualByComparingTo(original.totalAmount());
        // timestamp-millis: the wire format has no room for the micro/nano part Instant.now() carries.
        assertThat(back.occurredAt()).isEqualTo(original.occurredAt().truncatedTo(ChronoUnit.MILLIS));
    }

    @Test
    void orderCancelledSurvivesAnAvroBinaryRoundTrip() throws IOException {
        com.demo.events.OrderCancelled original =
                com.demo.events.OrderCancelled.of("ORD-2", "changed mind");

        OrderCancelled decoded = roundTrip(AvroEventMapper.toAvro(original), OrderCancelled.class);
        com.demo.events.OrderCancelled back = AvroEventMapper.fromAvro(decoded);

        assertThat(back.eventId()).isEqualTo(original.eventId());
        assertThat(back.orderId()).isEqualTo(original.orderId());
        assertThat(back.reason()).isEqualTo(original.reason());
        assertThat(back.occurredAt()).isEqualTo(original.occurredAt().truncatedTo(ChronoUnit.MILLIS));
    }

    @Test
    void amountIsRescaledToTheSchemaScaleBeforeEncoding() throws IOException {
        // REST input "149.9" arrives with scale 1; Avro's decimal conversion would reject it as-is.
        com.demo.events.OrderCreated original =
                com.demo.events.OrderCreated.of("ORD-3", "CUST-1", new BigDecimal("149.9"));

        OrderCreated avro = AvroEventMapper.toAvro(original);
        assertThat(avro.getTotalAmount()).isEqualTo(new BigDecimal("149.90"));

        OrderCreated decoded = roundTrip(avro, OrderCreated.class);
        assertThat(decoded.getTotalAmount()).isEqualTo(new BigDecimal("149.90"));
    }

    @Test
    void sameStoredEventAlwaysProducesTheSameBytes() throws IOException {
        // The inbox relies on a redelivered event being indistinguishable from the first delivery.
        com.demo.events.OrderCreated stored =
                new com.demo.events.OrderCreated(
                        "evt-1",
                        "ORD-4",
                        "CUST-1",
                        new BigDecimal("10.00"),
                        Instant.parse("2026-09-02T10:15:30.123Z"));

        assertThat(encode(AvroEventMapper.toAvro(stored)))
                .isEqualTo(encode(AvroEventMapper.toAvro(stored)));
    }

    @Test
    void unknownPayloadTypesAreRejectedWithTheClassName() {
        assertThatThrownBy(() -> AvroEventMapper.toAvro("not an event"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("java.lang.String");
    }

    private static <T extends SpecificRecord> T roundTrip(T record, Class<T> type)
            throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(encode(record), null);
        return new SpecificDatumReader<>(type).read(null, decoder);
    }

    private static byte[] encode(SpecificRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
