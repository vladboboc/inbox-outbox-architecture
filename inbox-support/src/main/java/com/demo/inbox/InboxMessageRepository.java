package com.demo.inbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface InboxMessageRepository
        extends JpaRepository<InboxMessage, InboxMessage.Key> {

    /**
     * Atomically claims an event for processing. Returns 1 on a first-time delivery and 0 when
     * this consumer has already handled the event.
     *
     * <p>{@code ON CONFLICT DO NOTHING} does the check and the write in a single statement, which
     * is the point: a {@code SELECT}-then-{@code INSERT} would leave a window in which two
     * concurrent deliveries of the same event both see "not processed" and both proceed. That
     * window is not hypothetical — after a rebalance, a partition's new owner can start on an event
     * its previous owner is still processing, landing it on two threads at nearly the same moment.
     * Here the loser of the race gets 0 rows back from the database itself.
     *
     * <p>Native query because JPQL has no vendor-specific upsert syntax.
     */
    @Modifying
    @Query(
            value =
                    """
                    INSERT INTO inbox_message
                        (event_id, consumer, topic, partition_no, record_offset, received_at)
                    VALUES
                        (:eventId, :consumer, :topic, :partitionNo, :recordOffset, :receivedAt)
                    ON CONFLICT (event_id, consumer) DO NOTHING
                    """,
            nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") String eventId,
            @Param("consumer") String consumer,
            @Param("topic") String topic,
            @Param("partitionNo") int partitionNo,
            @Param("recordOffset") long recordOffset,
            @Param("receivedAt") Instant receivedAt);

    /** Retention housekeeping. Without this the ledger grows without bound. */
    @Modifying
    @Query("DELETE FROM InboxMessage m WHERE m.receivedAt < :cutoff")
    int deleteReceivedBefore(@Param("cutoff") Instant cutoff);

    List<InboxMessage> findTop100ByOrderByReceivedAtDesc();

    long countByConsumer(String consumer);
}
