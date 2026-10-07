package com.demo.inbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "demo.inbox")
public class InboxProperties {

    /** Whether the inbox support beans are registered at all. */
    private boolean enabled = true;

    /** Housekeeping settings for the dedup ledger. */
    private final Cleanup cleanup = new Cleanup();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Cleanup getCleanup() {
        return cleanup;
    }

    public static class Cleanup {

        /** Whether to periodically purge old claims. */
        private boolean enabled = true;

        /**
         * How long a claim is kept. This is the real dedup window: an event redelivered after the
         * retention period will be processed again. It must comfortably exceed the topic's
         * retention, otherwise a consumer resetting to the earliest offset would reprocess history.
         */
        private Duration retention = Duration.ofDays(7);

        /**
         * How often the purge runs. {@link InboxCleanupJob} reads this key through its
         * {@code @Scheduled} placeholder, whose fallback value must match this default.
         */
        private Duration interval = Duration.ofHours(1);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration interval) {
            this.interval = interval;
        }
    }
}
