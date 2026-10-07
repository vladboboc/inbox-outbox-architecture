package com.demo.inbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;

/**
 * Turns at-least-once delivery into effectively-once processing.
 *
 * <p>Usage is a single guard clause at the top of a listener:
 *
 * <pre>{@code
 * @KafkaListener(topics = "orders.v1", groupId = "shipping-service")
 * @Transactional
 * public void on(OrderCreated event, ...) {
 *     if (!inboxGuard.claim(event.eventId(), CONSUMER, topic, partition, offset)) {
 *         return;                      // already handled, nothing to do
 *     }
 *     shipmentService.create(event);   // commits together with the claim
 * }
 * }</pre>
 *
 * <p>The guarantee rests on the claim and the business write sharing one database transaction.
 * Either both land or neither does, so there is no state in which work was performed but not
 * recorded as performed. If the handler throws, the claim is rolled back with it and the
 * redelivery is treated as a genuine first attempt.
 *
 * <p>What this deliberately does <em>not</em> attempt is atomicity between the database commit and
 * the Kafka offset commit — no such thing exists without XA. If the offset commit fails after the
 * database commit succeeds, Kafka redelivers, and the second delivery hits an existing claim and
 * becomes a no-op. That gap is precisely what this class exists to cover.
 */
public class InboxGuard {

    private static final Logger log = LoggerFactory.getLogger(InboxGuard.class);

    private final InboxMessageRepository repository;
    private final MeterRegistry meterRegistry;

    public InboxGuard(InboxMessageRepository repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Attempts to claim {@code eventId} for {@code consumer}.
     *
     * <p>{@link Propagation#MANDATORY} is not cosmetic: calling this outside a transaction throws
     * {@code IllegalTransactionStateException} on the very first call. Without an enclosing
     * transaction the claim would commit on its own, and a subsequent business failure would leave
     * the event permanently marked as processed — silently dropping it. Failing loudly is much
     * better than losing an event.
     *
     * @return {@code true} if this is the first delivery and processing should continue,
     *     {@code false} if the event was already handled.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean claim(
            String eventId, String consumer, String topic, int partitionNo, long recordOffset) {

        if (eventId == null || eventId.isBlank()) {
            // No id means no way to dedupe. Refusing is safer than guessing: silently processing
            // would make duplicates undetectable.
            throw new IllegalArgumentException(
                    "eventId is required for inbox deduplication (topic=%s, partition=%d, offset=%d)"
                            .formatted(topic, partitionNo, recordOffset));
        }

        int inserted =
                repository.insertIfAbsent(
                        eventId, consumer, topic, partitionNo, recordOffset, Instant.now());

        Tags tags = Tags.of("consumer", consumer, "topic", topic);

        if (inserted == 0) {
            log.info(
                    "inbox: duplicate suppressed eventId={} consumer={} topic={} partition={} offset={}",
                    eventId,
                    consumer,
                    topic,
                    partitionNo,
                    recordOffset);
            countOnCommit(
                    "inbox.messages.duplicate", "Deliveries suppressed as already processed", tags);
            return false;
        }

        log.debug("inbox: claimed eventId={} consumer={}", eventId, consumer);
        countOnCommit("inbox.messages.processed", "Deliveries processed and committed", tags);
        return true;
    }

    /**
     * Increments the counter when the surrounding transaction commits, and not at all if it rolls
     * back. Counters are not transactional: incremented directly, a handler that throws would count
     * as processed on every retry, even though its claim and its work were rolled back each time.
     */
    private void countOnCommit(String name, String description, Tags tags) {
        Counter counter =
                Counter.builder(name).description(description).tags(tags).register(meterRegistry);
        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        counter.increment();
                    }
                });
    }
}
