package com.demo.inbox;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wires the inbox so a consuming application only has to inject {@link InboxGuard}.
 *
 * <p>Registered through {@code META-INF/spring/...AutoConfiguration.imports}, so no
 * {@code @Import} is needed in the application.
 *
 * <p>Note what is deliberately absent: {@code @EntityScan} and {@code @EnableJpaRepositories}.
 * Declaring either one anywhere in the context switches off Boot's own scanning defaults, which
 * would leave the <em>application's</em> entities and repositories undiscovered — a confusing
 * failure to debug, and caused by a library rather than the application. Because
 * {@link InboxMessage} and {@link InboxMessageRepository} live outside the application's package
 * tree, the consuming application declares both annotations itself and lists every package,
 * including {@code com.demo.inbox}. See {@code ShippingServiceApplication}.
 *
 * <p>In Spring Boot 4 the Hibernate auto-configuration moved to
 * {@code org.springframework.boot.hibernate.autoconfigure} as part of the modularization; the old
 * {@code org.springframework.boot.autoconfigure.orm.jpa} package no longer exists.
 */
@AutoConfiguration(after = HibernateJpaAutoConfiguration.class)
@ConditionalOnBean(EntityManagerFactory.class)
@ConditionalOnProperty(prefix = "demo.inbox", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(InboxProperties.class)
public class InboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public InboxGuard inboxGuard(InboxMessageRepository repository, MeterRegistry meterRegistry) {
        return new InboxGuard(repository, meterRegistry);
    }

    /**
     * Housekeeping is isolated in a nested configuration so that {@code @EnableScheduling} — which
     * is only valid on a type — is switched on solely when the cleanup job is actually wanted. A
     * library turning on the application's scheduler unconditionally would be overreach.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "demo.inbox.cleanup", name = "enabled", matchIfMissing = true)
    @EnableScheduling
    public static class InboxCleanupConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public InboxCleanupJob inboxCleanupJob(
                InboxMessageRepository repository, InboxProperties properties) {
            return new InboxCleanupJob(repository, properties);
        }
    }
}
