package com.demo.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A record that this consumer has already handled a given event.
 *
 * <p>The primary key is {@code (eventId, consumer)} rather than {@code eventId} alone. That lets
 * several independent consumers fan out from the same topic and each keep its own dedup ledger —
 * a shared {@code eventId} key would mean the first consumer to process an event would silently
 * suppress it for everyone else.
 *
 * <p>The topic/partition/offset columns are diagnostics only. They answer "where did this
 * delivery come from?" when investigating a duplicate, and are not part of the dedup decision.
 */
@Entity
@Table(name = "inbox_message")
@IdClass(InboxMessage.Key.class)
public class InboxMessage {

    @Id
    @Column(name = "event_id", nullable = false, length = 255)
    private String eventId;

    @Id
    @Column(name = "consumer", nullable = false, length = 255)
    private String consumer;

    @Column(name = "topic", nullable = false, length = 255)
    private String topic;

    @Column(name = "partition_no", nullable = false)
    private int partitionNo;

    @Column(name = "record_offset", nullable = false)
    private long recordOffset;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected InboxMessage() {
        // for JPA
    }

    public String getEventId() {
        return eventId;
    }

    public String getConsumer() {
        return consumer;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    /** Composite key holder required by {@link IdClass}. */
    public static class Key implements Serializable {

        private String eventId;
        private String consumer;

        public Key() {
            // for JPA
        }

        public Key(String eventId, String consumer) {
            this.eventId = eventId;
            this.consumer = consumer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof Key key
                    && Objects.equals(eventId, key.eventId)
                    && Objects.equals(consumer, key.consumer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventId, consumer);
        }
    }
}
