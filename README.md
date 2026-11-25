# Order Events: Transactional Outbox + Kafka

An event-driven order service built with **Java 21, Spring Boot 3, Spring Kafka, PostgreSQL and
Flyway**. It solves the classic *dual-write* problem: how do you save an order **and** publish an
event about it without ever losing one or the other?

## How it works

```
POST /api/orders
      │
      ▼
┌───────────────────── one DB transaction ─────────────────────┐
│  INSERT orders            INSERT outbox_events (unpublished)  │
└───────────────────────────────────────────────────────────────┘
      │
      ▼  OutboxRelay (@Scheduled, FOR UPDATE SKIP LOCKED)
Kafka topic: orders.placed   (key = orderId → per-order ordering)
      │
      ▼  InventoryReservationConsumer
  dedupe on processed_events ──► reserve stock ──► order RESERVED / REJECTED
      │ on failure
      ▼
orders.placed-retry-0..2 (1s, 2s, 4s backoff) ──► orders.placed-dlt
```

## What it demonstrates

- **Transactional outbox.** The order and its event commit atomically, so a crash can't leave an
  order without an event or an event without an order.
- **Horizontally scalable relay.** `FOR UPDATE SKIP LOCKED` lets several instances poll the outbox
  together without double-publishing. A partial index keeps polling cheap.
- **Ordering.** Events are keyed by order id, so they stay in order per order. The relay stops a
  batch on the first failure so it never publishes events out of order.
- **Idempotent consumer.** Delivery is at-least-once. The consumer records each event id in
  `processed_events` in the same transaction as its state change, so a duplicate is a no-op.
- **Non-blocking retries + DLT.** `@RetryableTopic` with exponential backoff sends poison messages
  to a dead-letter topic instead of blocking the partition. Deserialization errors skip retries.
- **Safe producer settings.** `acks=all` and `enable.idempotence=true`.
- **End-to-end tests** against real Postgres and Kafka with Testcontainers and Awaitility.

## Run locally

```bash
docker compose up -d
mvn spring-boot:run

curl -X POST localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","sku":"SKU-BOOK","quantity":2,"total":39.98}'
# Then GET /api/orders/{id}. The status moves from PLACED to RESERVED.
# Use sku "SKU-FAIL-1" to watch retries and the dead-letter topic in the logs.
```

## Tests

```bash
mvn verify   # needs Docker running
```

## Next steps

- Replace polling with Debezium change data capture on the outbox table
- Avro/Protobuf schemas with a schema registry
- An outbox cleanup job and metrics for relay lag
