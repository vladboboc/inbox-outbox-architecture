package com.demo.inbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Purges claims older than the configured retention.
 *
 * <p>An unbounded dedup ledger is a slow-motion outage: the table and its index grow forever and
 * the upsert on the hot path degrades with them. Trimming it is not optional in a real system.
 *
 * <p>The trade-off is explicit — retention <em>is</em> the deduplication window. Anything
 * redelivered after it will be processed a second time, so retention must stay well above the
 * topic's own retention.
 */
public class InboxCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(InboxCleanupJob.class);

    private final InboxMessageRepository repository;
    private final InboxProperties properties;

    public InboxCleanupJob(InboxMessageRepository repository, InboxProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${demo.inbox.cleanup.interval:1h}")
    @Transactional
    public void purgeExpiredClaims() {
        Instant cutoff = Instant.now().minus(properties.getCleanup().getRetention());
        int deleted = repository.deleteReceivedBefore(cutoff);
        if (deleted > 0) {
            log.info("inbox: purged {} claim(s) received before {}", deleted, cutoff);
        }
    }
}
