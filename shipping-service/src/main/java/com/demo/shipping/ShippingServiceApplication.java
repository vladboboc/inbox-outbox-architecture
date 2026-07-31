package com.demo.shipping;

import com.demo.inbox.InboxMessage;
import com.demo.inbox.InboxMessageRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Inbox consumer.
 *
 * <p>{@code inbox-support} keeps its entity and repository in {@code com.demo.inbox}, outside this
 * application's package tree, so Boot's default scanning would not find them. The scan is declared
 * here rather than inside the library's auto-configuration on purpose: {@code @EntityScan} and
 * {@code @EnableJpaRepositories} replace Boot's defaults wherever they appear, so a library
 * declaring them would silently stop this application's own entities and repositories from being
 * discovered. Owning the declaration here means both package lists are visible in one place.
 *
 * <p>In Spring Boot 4, {@code @EntityScan} lives in
 * {@code org.springframework.boot.persistence.autoconfigure} (it moved from
 * {@code org.springframework.boot.autoconfigure.domain}).
 */
@SpringBootApplication
@EntityScan(basePackageClasses = {ShippingServiceApplication.class, InboxMessage.class})
@EnableJpaRepositories(
        basePackageClasses = {ShippingServiceApplication.class, InboxMessageRepository.class})
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}
