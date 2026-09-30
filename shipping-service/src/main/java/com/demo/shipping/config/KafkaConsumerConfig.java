package com.demo.shipping.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retry and dead-letter policy for the consumer.
 *
 * <p>Failures get three quick retries; if the cause is transient (a brief database blip) the event
 * succeeds without operator involvement. If it persists, the record is routed to
 * {@code <topic>.DLT} — {@code orders.v1.DLT} for every order event — and the consumer moves on
 * rather than blocking the partition: a genuinely bad event must not stall every well-formed event
 * queued behind it.
 *
 * <p>Because each attempt runs in its own transaction and each rollback undoes the inbox claim,
 * these retries are indistinguishable from first attempts. Only a committed attempt leaves a claim.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

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

        // Three retries one second apart: four attempts in all before the record is dead-lettered.
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));

        // A missing event id can never succeed on retry, so don't waste attempts on it.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setLogLevel(KafkaException.Level.WARN);
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
