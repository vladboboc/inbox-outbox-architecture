# Why `AvroTrust` exists

## The problem

When a Kafka consumer configured with `specific.avro.reader=true` receives a message, Confluent's `KafkaAvroDeserializer` reads the schema id from the message header, fetches the writer schema from the Schema Registry, and resolves its full name (e.g. `com.demo.events.avro.OrderCreated`) to a generated Java class via `SpecificData.getClass(...)`.

Starting with Avro **1.11.4 / 1.12.0**, that class lookup is no longer unconditional. Those releases fixed [CVE-2024-47561](https://thehackernews.com/2024/10/critical-apache-avro-sdk-flaw-allows.html) (CVSS 9.3) — a flaw where a schema carrying an attacker-controlled `java-class` annotation could make Avro instantiate an arbitrary class on the classpath, a classic insecure-deserialization RCE path. The fix, tracked as [AVRO-3985](https://issues.apache.org/jira/browse/AVRO-3985), put a trust check in front of every such lookup: only classes on an explicit allow-list are resolved; everything else fails with a `SecurityException`.

The allow-list mechanism was hardened twice more since:

- **1.11.5 / 1.12.1** (Oct 2025) removed the small set of JDK packages that were trusted by default and introduced the `org.apache.avro.SERIALIZABLE_CLASSES` system property for trusting individual classes, alongside the older, now-deprecated `org.apache.avro.SERIALIZABLE_PACKAGES`.
- **1.12.2** (Aug 2026) replaced the static, read-once-at-classload system-property mechanism with `org.apache.avro.util.ClassSecurityValidator` — a live, swappable, code-configurable global predicate. This is the version `AvroTrust` targets.

None of the generated classes (`OrderCreated`, `OrderCancelled`, …) are on the default allow-list, so out of the box every message fails to deserialize with:

```
java.lang.SecurityException: Forbidden com.demo.events.avro.OrderCreated!
This class is not trusted to be included in Avro schema using java-class...
```

## Two ways to fix it

**Option one — a JVM flag at startup**, e.g. `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.demo.events.avro`. Avro reads this once, in a static initializer, the first time its internal classes load. It works, but it lives entirely outside the codebase: it has to be copied into the Dockerfile, the docker-compose file, every CI/test runner, and every developer's IDE run configuration. Miss it in one place and that environment fails on the first message with a confusing security error that has nothing obviously to do with a missing flag.

**Option two — configure trust in code.** This is what `AvroTrust` does. Because the decision lives in a class that ships inside the jar, every environment that runs the code gets the trust configuration automatically — there's no separate checklist item to forget.

## What `AvroTrust` does, line by line

```java
package com.demo.events.avro;

import org.apache.avro.util.ClassSecurityValidator;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AvroTrust {

    private static final AtomicBoolean APPLIED = new AtomicBoolean();

    private AvroTrust() {}

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
```

**It targets the 1.12.2 API, not the older system properties.** `ClassSecurityValidator` exposes a live global validator via `getGlobal()` / `setGlobal(ClassSecurityPredicate)`, where a `ClassSecurityPredicate` is simply `boolean isTrusted(Class<?>)`. Out of the box the global validator is `ClassSecurityValidator.DEFAULT`, itself a composite of `DEFAULT_TRUSTED_CLASSES` (JDK boxed types, `String`, `BigDecimal`, …) and `SYSTEM_PROPERTIES` (whatever is set via `SERIALIZABLE_CLASSES` / `SERIALIZABLE_PACKAGES`). So this API sits on top of the older mechanism rather than replacing it — the JVM flag still works and is still visible to it.

**It trusts specific classes, not a whole package.** `ClassSecurityValidator.builder().add(OrderCreated.class).add(OrderCancelled.class).build()` builds a predicate that trusts exactly those two classes, referenced directly rather than by string. Compared to `SERIALIZABLE_PACKAGES=com.demo.events.avro`, this is both compile-checked (rename or delete `OrderCreated` and the build breaks, instead of leaving a stale string nobody notices) and narrower — it can't accidentally trust some unrelated class that ends up in that package later.

**It extends the existing trust rather than replacing it.** `getGlobal()` captures whatever validator is currently installed (the JDK defaults, anything from the system properties) before `setGlobal` is called. The new global validator is `clazz -> previous.isTrusted(clazz) || generated.isTrusted(clazz)` — a class is trusted if either the old validator already trusted it *or* the new one does. Calling `setGlobal(generated)` directly would have silently dropped all previously-trusted classes, including the JDK defaults Avro needs for ordinary fields like `String` or `BigDecimal`. (The library also ships `ClassSecurityValidator.composite(previous, generated)` as a named helper for the same union — functionally identical to the hand-rolled lambda here.)

**It only mutates global state once.** `setGlobal` replaces one JVM-wide static field, and the class is explicitly designed to be called from multiple places ("safe to call from several configuration classes and tests," per its javadoc) — a Spring `@Configuration` bean and a JUnit test base class might both call it. Without a guard, each call would wrap the validator in another layer of lambda. `AtomicBoolean.compareAndSet(false, true)` makes `trustEventSchemas()` idempotent and thread-safe: only the first caller does any work, every later call is a no-op.

**It removes the classloading-order footgun of the old approach, but not the ordering requirement itself.** `SERIALIZABLE_CLASSES` / `SERIALIZABLE_PACKAGES` are read once in a static initializer, so setting them with `System.setProperty(...)` in code only works if that call happens before Avro's classes are first touched anywhere in the JVM — easy to get wrong by accident (a Kafka client factory or a Spring bean initializing in the wrong order is enough to break it silently). `setGlobal` has no such restriction: it can be called at any time and takes effect for every trust check after that point. The only requirement left is the intuitive one — call `AvroTrust.trustEventSchemas()` before the first message is deserialized, not before some unrelated class happens to load.

## What still has to happen manually

`AvroTrust` doesn't invoke itself. Something in the application's bootstrap — a `@PostConstruct` method, a static block in the consumer's entry point, or a JUnit extension for tests — still needs to call `AvroTrust.trustEventSchemas()` before the first message is consumed. What Option two removes is the need to replicate that decision across every environment's *configuration*; it doesn't remove the need to wire it into the application's *startup path* exactly once.

## Note on the version referenced in the class's own javadoc

The pasted class's javadoc says "Since 1.12.1 Avro refuses to load any class by name that is not on an explicit allow-list... in `ClassSecurityValidator`." That should read **1.12.2**: 1.12.1 (Oct 2025) only removed the default trusted packages and added the `SERIALIZABLE_CLASSES` property; `ClassSecurityValidator` itself — the `getGlobal()`/`setGlobal()`/`builder()` API this class depends on — was introduced in **1.12.2** (Aug 2026). Worth fixing before this ships, so nobody chasing the CVE reference lands on the wrong changelog.

## References

- [CVE-2024-47561 — critical Apache Avro SDK RCE](https://thehackernews.com/2024/10/critical-apache-avro-sdk-flaw-allows.html)
- [AVRO-3985 — Restrict trusted packages in ReflectData and SpecificData](https://issues.apache.org/jira/browse/AVRO-3985)
- [Avro 1.11.4 release notes](https://avro.apache.org/blog/2024/09/22/avro-1.11.4/)
- [Avro 1.12.1 release notes](https://avro.apache.org/blog/2025/10/16/avro-1.12.1/)
- [Avro 1.12.2 release notes](https://avro.apache.org/blog/2026/08/12/avro-1.12.2/)
- [`ClassSecurityValidator.java`, release-1.12.2](https://raw.githubusercontent.com/apache/avro/release-1.12.2/lang/java/avro/src/main/java/org/apache/avro/util/ClassSecurityValidator.java)
- [CAMEL-24578 — camel-avro-rpc fails with Avro 1.12.2 due to new `ClassSecurityValidator`](http://www.mail-archive.com/issues@camel.apache.org/msg131342.html)
