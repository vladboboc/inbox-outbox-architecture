package com.demo.shipping.config;

import com.demo.events.avro.AvroTrust;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Retry and dead-letter policy for the consumer.
 *
 * <p>Failures get three quick retries; if the cause is transient (a brief database blip) the event
 * succeeds without operator involvement. If it persists, the record is routed to
 * {@code orders.v1.DLT} and the consumer moves on rather than blocking the partition — a genuinely
 * bad event must not stall every well-formed event queued behind it.
 *
 * <p>Because each attempt runs in its own transaction and each rollback undoes the inbox claim,
 * these retries are indistinguishable from first attempts. Only a committed attempt leaves a claim.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    static {
        // KafkaAvroDeserializer (specific.avro.reader) resolves the writer schema's full name to a
        // class by name, and Avro 1.12.2 rejects any class not on its allow-list. Runs when Spring
        // loads this configuration, well before the listener containers start polling.
        AvroTrust.trustEventSchemas();
    }

    /**
     * Producer behind the dead-letter publisher.
     *
     * <p>Hand-built because the recoverer publishes two value shapes and no single serializer takes
     * both: the deserialized Avro record when the listener threw, and the original {@code byte[]}
     * when deserialization itself failed (the {@code ErrorHandlingDeserializer} hands the raw bytes
     * to the error handler). {@link DelegatingByTypeSerializer} routes each to the right delegate,
     * and byte-exact dead letters are what an operator needs to diagnose a bad payload.
     *
     * <p>Declaring a {@link ProducerFactory} makes Boot's auto-configured one back off, which also
     * means Boot no longer applies {@code @ServiceConnection} details for us — hence
     * {@link KafkaConnectionDetails} is consulted explicitly, so the Testcontainers broker is used in
     * tests and {@code spring.kafka.bootstrap-servers} everywhere else. All other
     * {@code spring.kafka.producer.*} and {@code spring.kafka.properties.*} values (acks,
     * {@code schema.registry.url}) still apply through {@code buildProducerProperties()}; the factory
     * calls {@code configure()} on the serializer instances, and the delegating serializer forwards
     * it, so the Avro delegate learns the registry URL the normal way.
     */
    @Bean
    public ProducerFactory<Object, Object> kafkaProducerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails) {

        Map<String, Object> config = kafkaProperties.buildProducerProperties();
        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                connectionDetails.getProducer().getBootstrapServers());
        // Serializer instances below take precedence; drop the class-name keys so the client does
        // not warn about supplying both.
        config.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        config.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        // Deserialization failed: republish the original bytes untouched.
        delegates.put(byte[].class, new ByteArraySerializer());
        // Listener threw on a valid event: republish as Avro (registers <topic>.DLT-value).
        delegates.put(SpecificRecord.class, new KafkaAvroSerializer());

        @SuppressWarnings("unchecked")
        Serializer<Object> keySerializer = (Serializer<Object>) (Serializer<?>) new StringSerializer();
        // assignable=true: match generated OrderCreated/OrderCancelled as SpecificRecord subtypes.
        return new DefaultKafkaProducerFactory<>(
                config, keySerializer, new DelegatingByTypeSerializer(delegates, true));
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaOperations,
                        (record, exception) -> {
                            log.error(
                                    "routing to DLT: topic={} partition={} offset={} cause={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    exception.getMessage());
                            // Keep the partition so ordering within a key is preserved in the DLT too.
                            return new TopicPartition(record.topic() + ".DLT", record.partition());
                        });

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));

        // A missing event id can never succeed on retry, so don't waste attempts on it.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setLogLevel(org.springframework.kafka.KafkaException.Level.WARN);
        return handler;
    }

    /**
     * Container factory wired to the error handler above.
     *
     * <p>Built through Boot's {@code ConcurrentKafkaListenerContainerFactoryConfigurer} so all the
     * {@code spring.kafka.*} properties still apply — defining the factory from scratch would
     * quietly discard them.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
