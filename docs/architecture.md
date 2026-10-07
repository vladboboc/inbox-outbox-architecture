# Architecture

Three views of the same system: the end-to-end flow, what happens when it breaks, and what actually
runs in Docker. All diagrams are Mermaid, so they render on GitHub without any build step.

---

## 1. End-to-end flow

The two shaded blocks are the whole design. Everything else is plumbing.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant OS as order-service
    participant ODB as orderdb
    participant K as Kafka
    participant SS as shipping-service
    participant SDB as shippingdb

    C->>OS: POST /orders

    rect rgba(127, 119, 221, 0.12)
        Note over OS,ODB: ONE transaction — both rows or neither
        OS->>ODB: INSERT orders
        OS->>ODB: INSERT outbox_record (status NEW)
    end

    OS-->>C: 201 Created

    Note over OS: relay polls, adaptive 0.5s to 5s
    OS->>K: send to orders.v1, key = orderId
    K-->>OS: ack — acks=all, min.insync.replicas=2
    OS->>ODB: UPDATE outbox_record to COMPLETED

    Note over OS,K: Separate commit. If this one fails after<br/>the send succeeded, the event is sent again.

    K->>SS: deliver OrderCreated

    rect rgba(29, 158, 117, 0.12)
        Note over SS,SDB: ONE transaction — claim authorises the work
        SS->>SDB: INSERT inbox_message ... ON CONFLICT DO NOTHING
        alt First delivery — 1 row inserted
            SS->>SDB: INSERT shipments
        else Duplicate — 0 rows inserted
            SS-->>SS: return early, count the suppression
        end
    end

    SS->>K: commit offset
```

Why each step matters:

- **Steps 2–3** are why nothing is lost. The event is not a side effect of the business write; it is
  part of the same commit.
- **Step 4** returns before publication. A `201` means *durably recorded and guaranteed to publish*,
  not *already on the topic*.
- **Steps 5–6** are a blocking send: the relay waits for the broker acknowledgement, so with
  `acks=all` and an ISR floor of 2 the ack means genuinely replicated.
- **Step 7** is a *second* commit, and that is the unavoidable gap — see the note that follows it.
- **Steps 9–11** close that gap. The claim and the business write share one transaction, so the
  system can never be in a state where work happened but was not recorded as having happened.

The idempotency key is `eventId`, carried **in the payload** and generated inside the producer's
transaction. The outbox stores the domain record once; at relay time the routing maps it to its Avro
`SpecificRecord` (a pure function of the stored record) and `KafkaAvroSerializer` writes it, so every
retry or replay produces identical bytes and an identical id.

---

## 2. What happens when it breaks

```mermaid
flowchart LR
    subgraph F1["Kafka unreachable at commit time"]
        direction TB
        A1["business tx commits<br/>HTTP 201 returned"] --> A2["outbox_record stays NEW<br/>failure_count climbs"]
        A2 --> A3["brokers return<br/>relay drains the backlog"]
        A3 --> A4(["nothing lost"])
    end

    subgraph F2["Event delivered twice"]
        direction TB
        B1["same eventId arrives"] --> B2["ON CONFLICT DO NOTHING<br/>inserts 0 rows"]
        B2 --> B3["listener returns early"]
        B3 --> B4(["no second shipment"])
    end

    subgraph F3["Handler throws"]
        direction TB
        C1["ShipmentService fails"] --> C2["claim and business write<br/>roll back together"]
        C2 --> C3["3 retries, then orders.v1.DLT"]
        C3 --> C4(["no false claim"])
    end
```

The third column is the subtle one. If the claim survived a rolled-back handler, the event would be
permanently marked as processed and silently dropped — the retry would be mistaken for a duplicate.
Rolling the claim back with the transaction is what keeps failures recoverable.

Two guardrails make these properties hard to break by accident:

- `InboxGuard.claim` is `@Transactional(propagation = MANDATORY)`, so calling it outside a
  transaction fails loudly instead of committing a claim on its own.
- The dedup insert is a single statement. A `SELECT`-then-`INSERT` would leave a window where two
  concurrent deliveries both conclude "not processed yet"; here the loser of the race gets 0 rows
  back from the database itself.

---

## 3. What runs in Docker

```mermaid
flowchart TB
    subgraph apps["Applications"]
        direction LR
        OS["order-service<br/>:8090"]
        SS["shipping-service<br/>:8091"]
    end

    subgraph kafka["Kafka — KRaft, no ZooKeeper"]
        direction LR
        K1["kafka-1<br/>:19092"]
        K2["kafka-2<br/>:29092"]
        K3["kafka-3<br/>:39092"]
    end

    subgraph data["Databases"]
        direction LR
        ODB[("orderdb")]
        SDB[("shippingdb")]
    end

    subgraph obs["Observability"]
        direction LR
        PROM["Prometheus<br/>:9090"]
        GRAF["Grafana<br/>:3000"]
        KEXP["kafka-exporter<br/>:9308"]
        CDK["Conduktor<br/>:8080"]
    end

    SR["Schema Registry<br/>:8081"]

    OS --> ODB
    SS --> SDB
    OS -->|"orders.v1<br/>(Avro)"| kafka
    kafka --> SS
    SS -->|"orders.v1.DLT"| kafka
    OS -.->|"register schema"| SR
    SS -.->|"fetch schema by id"| SR

    kafka --- SR
    kafka --- KEXP
    KEXP --> PROM
    OS -.->|"/actuator/prometheus"| PROM
    SS -.->|"/actuator/prometheus"| PROM
    PROM --> GRAF
    kafka --- CDK
    SR --- CDK
```

`orderdb` and `shippingdb` are separate databases in one Postgres instance. Each service owns its own
schema and migrations, with no shared tables and no cross-service joins — that separation is the
reason these patterns are needed at all.

All three brokers are always-on by design: the KRaft controller quorum has three voters and needs two
to elect a leader, so a single broker would never form a quorum. `RF=3` topics and
`min.insync.replicas=2` depend on all three as well.

---

## 4. Where the numbers come from

The metrics behind the Grafana dashboard, and which side of the system each one tells you about:

| Signal | Metric | Answers |
|---|---|---|
| Outbox backlog | `outbox_records{outbox_record_status="new"}` | Is the producer stuck? |
| Publish latency | `outbox_record_process_seconds_bucket` | Is the relay slow? |
| Consumer lag | `kafka_consumergroup_lag` | Is the consumer behind? |
| Duplicates suppressed | `inbox_messages_duplicate_total` | Is the inbox earning its keep? |
| Events processed | `inbox_messages_processed_total` | Throughput of real work (counted on commit, so a rolled-back attempt never shows) |

Backlog and lag answer genuinely different questions, which is why both are on the dashboard: a
backlog means the event never reached Kafka, while lag means it did and the consumer has not caught
up. Confusing the two sends you debugging the wrong service.
