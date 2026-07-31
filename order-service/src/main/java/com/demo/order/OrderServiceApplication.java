package com.demo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Outbox producer.
 *
 * <p>No {@code @EntityScan} is needed for namastack's own entities: the JPA module registers them
 * through a {@code HibernatePropertiesCustomizer} rather than by package scanning, so Boot's
 * default scanning of this package is left intact.
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
