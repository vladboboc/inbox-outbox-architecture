package com.demo.events.avro;

import org.apache.avro.util.ClassSecurityValidator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers the generated event classes with Avro's class allow-list.
 *
 * <p>Since 1.12.1 Avro refuses to load <em>any</em> class by name that is not on an explicit
 * allow-list (CVE-2024-47561 hardening in {@code ClassSecurityValidator}). That check sits on the
 * path Confluent's {@code KafkaAvroDeserializer} uses with {@code specific.avro.reader=true}: it
 * resolves the writer schema's full name ({@code com.demo.events.avro.OrderCreated}) to a class
 * through {@code SpecificData.getClass}, which fails with {@code SecurityException: Forbidden ...}
 * until the class is trusted. The alternative is the JVM flag
 * {@code -Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.demo.events.avro}; doing it in code means the
 * services and the tests cannot forget it.
 *
 * <p>The existing global predicate is kept and extended rather than replaced, so anything Avro
 * trusts by default (boxed primitives, {@code String}, {@code BigDecimal}) stays trusted.
 */
public final class AvroTrust {

    private static final AtomicBoolean APPLIED = new AtomicBoolean();

    private AvroTrust() {}

    /** Idempotent; safe to call from several configuration classes and tests. */
    public static void trustEventSchemas() {
        if (!APPLIED.compareAndSet(false, true)) {
            return;
        }
        var previous = ClassSecurityValidator.getGlobal();
        var generated =
                ClassSecurityValidator.builder()
                        .add(OrderCreated.class)
                        .add(OrderCancelled.class)
                        .build();
        ClassSecurityValidator.setGlobal(
                clazz -> previous.isTrusted(clazz) || generated.isTrusted(clazz));
    }
}
