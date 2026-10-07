# MiniShop Order Saga

A hands-on pet project for closing the "no production microservices experience" gap.
It covers, in working Java 21 / Spring Boot 3.4 code:

- **Saga (orchestration)** with persisted state, compensation and stuck-saga recovery
- **Transactional Outbox** and **Idempotent Consumer** (the patterns that make sagas actually reliable)
- **Kafka** with per-order ordering, retries and dead-letter topics
- **Distributed tracing** with OpenTelemetry + Jaeger, including trace propagation *through the outbox*
- **Resilience** on a synchronous dependency: timeouts, retry with backoff + jitter, circuit breaker, idempotency keys
- **Incident drills**: you break the system on purpose and diagnose it from traces and logs

---

## 1. The scenario

A customer orders a product. The order can be approved only if **stock is reserved** (Inventory Service)
**and the customer is charged** (Payment Service → external bank). Each service owns its own database,
so there is no single ACID transaction. If payment fails after stock was reserved, the stock must be released.

```
Client ──HTTP──> Order Service (saga orchestrator) :8080
                   │   commands ──> inventory.commands / payment.commands
                   │   replies  <── order.saga-replies
          ┌────────┴─────────┐
  Inventory Service :8081   Payment Service :8082 ──HTTP──> Fake Bank :8083
```

Saga state machine (lives in `OrderSagaOrchestrator`):

```
RESERVE_STOCK ──StockReserved──> CHARGE_PAYMENT ──PaymentCharged──> DONE / COMPLETED   → order APPROVED
      │                                │
StockReserveFailed               PaymentFailed
      ▼                                ▼
DONE / ROLLED_BACK            RELEASE_STOCK / COMPENSATING ──StockReleased──> DONE / ROLLED_BACK
→ order REJECTED                                                              → order REJECTED
```

Charging money is the step that's hardest to undo, so it runs **last** (the *pivot transaction*).

| Part | Technology |
|---|---|
| Services | Java 21, Spring Boot 3.4, Spring Kafka, JdbcTemplate, Flyway |
| Messaging | Apache Kafka 3.8 (KRaft, no Zookeeper) |
| Database | PostgreSQL 16, one database per service |
| Tracing | OpenTelemetry Java agent → OTel Collector → Jaeger v2 |
| Resilience | Resilience4j (retry, circuit breaker) |

---

## 2. How to use this repo

**Recommended: build it yourself, step by step (section 5), using this repo as the reference solution.**
Typing it yourself and hitting the problems is the practical experience. Commit after each step
(`git tag step-1` ...) so you can show your progression.

Then do the **incident drills** (section 6). They're the part that gives you real production stories.

---

## 3. Run it

Prerequisites: JDK 21, Maven 3.9+, Docker Desktop (or Docker + Compose v2).

```bash
mvn clean package -DskipTests
docker compose up --build -d
docker compose logs -f order-service inventory-service payment-service
```

| URL | What |
|---|---|
| http://localhost:8080/orders | Order API |
| http://localhost:8081/products | Stock levels |
| http://localhost:8082/actuator/health | Payment health incl. circuit breaker state |
| http://localhost:8083/chaos | Fake bank chaos switches |
| http://localhost:16686 | **Jaeger** (traces) |
| http://localhost:8090 | Kafka UI (topics, messages, DLQs) |

**Running a service from IntelliJ instead:** start the infrastructure with
`docker compose up -d postgres kafka otel-collector jaeger fake-bank`, download the
[OTel Java agent 2.11.0](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/tag/v2.11.0)
and add these VM options to the run configuration:

```
-javaagent:/path/to/opentelemetry-javaagent.jar
-Dotel.service.name=order-service
-Dotel.metrics.exporter=none -Dotel.logs.exporter=none
-Dotel.instrumentation.spring-scheduling.enabled=false
```

The `application.yml` defaults already point to `localhost:5432` and Kafka's external listener `localhost:29092`.

---

## 4. Try the three outcomes

```bash
# 1) Happy path -> APPROVED
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","productId":"p-1","quantity":2,"amount":50}'

# 2) Out of stock (p-2 has 0 units) -> REJECTED, nothing to compensate
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","productId":"p-2","quantity":1,"amount":20}'

# 3) Bank declines amounts > 1000 -> stock reserved, payment fails, stock RELEASED -> REJECTED
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","productId":"p-1","quantity":1,"amount":5000}'

# Check the result (the saga is async, so give it a second)
curl -s localhost:8080/orders/<orderId>
curl -s localhost:8081/products
```

In Jaeger choose service `order-service`, operation `POST /orders`, and open a trace. Scenario 3 should show
**one trace** spanning all four services: reserve → charge (bank 402) → release → rejected.
To find a specific order use **Tags**: `order.id=<orderId>`.

---

## 5. Implementation steps (the practical task)

### Step 1 — Infrastructure (1–2 days)

**Task:** `docker-compose.yml` with Postgres (3 databases via `infra/postgres/init.sql`), Kafka in KRaft mode,
Kafka UI, OTel Collector, Jaeger. Multi-module Maven project: `common`, three services, `fake-bank`.

**Why:** a reviewer must be able to run your system with one command. Kafka has two listeners:
`kafka:9092` for containers and `localhost:29092` for your IDE. That's the most common local Kafka pitfall.

**Verify:** `docker compose up -d postgres kafka jaeger` is healthy, Jaeger UI opens.

### Step 2 — Happy-path saga (week 1)

**Task:** `POST /orders` creates a `PENDING` order plus a `saga_instance` row and sends `ReserveStock`.
Inventory replies `StockReserved`, the orchestrator sends `ChargePayment`, Payment replies `PaymentCharged`,
the order becomes `APPROVED`.

**Files:** `Messages.java` (contracts), `OrderSagaOrchestrator`, `SagaRepository`, `InventoryService.reserve`, `PaymentService`.

**Best practices applied:**
- **Commands vs replies.** Commands go to the participant's topic; replies come back on the orchestrator's topic.
- **Saga state in the DB**, never in memory, so it survives restarts.
- **Kafka key = orderId.** All messages of one order land on the same partition and stay ordered.
- **`SELECT ... FOR UPDATE`** on the saga row, so two replies for one order can't interleave.
- **202 Accepted** from the API. The result isn't known yet; the client polls `GET /orders/{id}`.
- **Atomic stock check:** `UPDATE ... SET available = available - ? WHERE available >= ?` in one statement, with no read-then-write race.

**Verify:** scenario 1 from section 4 ends `APPROVED`.

### Step 3 — Reliable messaging (week 2)

**Task:** stop calling `kafkaTemplate.send()` from business code. Add the outbox and idempotent consumers.

**Files:** `OutboxWriter`, `OutboxRelay`, `IdempotencyGuard`, `V0_1__messaging_tables.sql` (shared by all services).

**Best practices applied:**
- **Transactional Outbox** fixes the dual-write problem: the message is inserted in the same transaction as the business change.
- **`FOR UPDATE SKIP LOCKED`** in the relay allows several instances of a service to publish in parallel.
- The relay gives **at-least-once** delivery, so every consumer is **idempotent at two levels**:
  1. *Message level* — `processed_messages` table, written in the same transaction (`ON CONFLICT DO NOTHING`).
  2. *Business level* — a re-sent command has a *new* message id, so services also check state:
     Inventory checks for an existing reservation, Payment for an existing payment, and the orchestrator
     ignores replies that don't match the saga's current step (`expect(...)`).

**Verify:** drill 6.3 (duplicate message) and drill 6.5 (crash).

### Step 4 — Failures and compensation (week 3)

**Task:** handle `StockReserveFailed` and `PaymentFailed` → `ReleaseStock` → `StockReleased`.
Add dead-letter topics and the stuck-saga detector.

**Files:** orchestrator transitions, `InventoryService.release`, `MessagingConfig.kafkaErrorHandler`, `StuckSagaDetector`.

**Best practices applied:**
- **Compensations are idempotent and never fail for business reasons.** Reservations are rows with a status,
  so release happens once and replying "released" when there was nothing to release is fine.
- **DLQ:** 3 retries, then `<topic>.dlq`. One poison message can't block its partition.
  Invalid messages (bad JSON, unknown type) skip retries entirely.
- **Timeouts for long-running processes:** the detector re-sends the current command for sagas idle > 60 s,
  and after 3 retries marks them `FAILED` with an ERROR log (in production this would page someone).

**Verify:** scenarios 2 and 3 from section 4, drill 6.4.

### Step 5 — Distributed tracing (week 4)

**Task:** every order produces one connected trace across all services, Kafka hops and the bank call.

**Files:** Dockerfiles (agent), `docker-compose.yml` (`OTEL_*` env), `infra/otel-collector.yaml`,
`TraceContext`, `@WithSpan` + `Span.current().setAttribute(...)` in handlers.

**Best practices applied:**
- **Auto-instrumentation first** (Java agent: Spring MVC, Kafka, JDBC, HTTP client), manual spans only for business steps
  (`saga.start`, `saga.handle-reply`, `bank.charge`...).
- **Business attributes** (`order.id`, `saga.step`) make traces searchable by what support people actually ask about.
- **Apps → Collector → backend.** Services never know about Jaeger; you can switch to Tempo/Datadog in the Collector.
- **The outbox breaks tracing, and we fix it.** The relay publishes later on a scheduler thread, so the original trace
  context is gone. `OutboxWriter` stores the current `traceparent` in the outbox row; `OutboxRelay` restores it
  before `send()`, so the agent injects the right context into Kafka headers. Most tutorials miss this.
- **No trace spam.** Scheduled polling runs under an unsampled context (`TraceContext.untraced`), otherwise Jaeger fills
  with thousands of one-span traces of `SELECT ... FROM outbox`.
- **trace_id in every log line** (`%X{trace_id}` from the agent's MDC). Copy it from a log into Jaeger's search box.

**Verify:** scenario 3 is a single trace; drill 6.6.

### Step 6 — Resilience on the synchronous call (week 5)

**Task:** protect `Payment → Fake Bank`.

**Files:** `BankClient`, `resilience4j` section in `payment-service/application.yml`, `fake-bank/BankController`.

**Best practices applied:**
- **Explicit timeouts** (1 s connect, 2 s read). The JDK default is infinite.
- **Retry only transient errors** (timeouts, 5xx), never 4xx; **exponential backoff with jitter**; max 3 attempts.
- **Idempotency-Key = orderId** makes retries safe: the bank returns the original result instead of charging twice.
- **Circuit breaker** opens at ≥ 50 % failures or ≥ 80 % slow calls; then calls fail instantly and the saga compensates.
  A 402 decline is a business answer, so it's *ignored* by the breaker.
- **No DB transaction during the remote call** (`PaymentService` uses short `TransactionTemplate` blocks around it).

**Verify:** drills 6.1 and 6.2.

---

## 6. Incident drills

For each drill: inject the fault, **diagnose using only Jaeger, logs and Kafka UI**, then write a postmortem (template below).

A handy loop for creating several orders:

```bash
for i in $(seq 1 10); do curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","productId":"p-1","quantity":1,"amount":30}'; echo; done
```

### 6.1 Slow bank → timeouts, retries, compensation
```bash
curl -X POST "localhost:8083/chaos?delayMs=3000"
```
Create one order. **Expect in Jaeger:** three `bank.charge` spans (~2 s each, timed out) with growing gaps
(backoff), then `PaymentFailed` → `ReleaseStock` → order `REJECTED`.
**Then check:** `curl localhost:8083/charges/<orderId>` → the bank shows **APPROVED**!
The bank finished the charge after our client gave up: money taken, order rejected.
This is the classic *ambiguous outcome* problem, see extension task 1. Great interview story.
Reset: `curl -X POST "localhost:8083/chaos?delayMs=0"`

### 6.2 Dead bank → circuit breaker opens
```bash
curl -X POST "localhost:8083/chaos?failureRate=1.0"
```
Run the order loop. The first orders show retries with 503s; after ~5 calls the breaker opens and the rest fail
immediately with *"circuit breaker open"*, with **no bank span at all**. Check `localhost:8082/actuator/health`.
Reset to `failureRate=0`, wait 15 s, create orders again: half-open → closed.

### 6.3 Duplicate message
```bash
docker compose exec postgres psql -U minishop -d inventory_db -c \
  "UPDATE outbox SET published_at = NULL WHERE seq = (SELECT max(seq) FROM outbox);"
```
The last reply is re-published with the same message id. **Expect:** order-service logs
`Duplicate message ... skipped`, nothing changes.

### 6.4 Poison messages → DLQ → stuck saga
```bash
curl -X POST "localhost:8081/chaos?failureRate=1.0"
```
Create one order. **Expect:** inventory retries 3 times, then the message appears in `inventory.commands.dlq`
(Kafka UI). The order stays `PENDING`. After ~60 s the detector logs `saga stuck at RESERVE_STOCK, command re-sent`;
after 3 retries: `MANUAL INTERVENTION NEEDED ... FAILED`.
Variant: set `failureRate=0` again *before* the detector fires. The re-sent command succeeds and the order is approved.

### 6.5 Crash in the middle of a saga
```bash
docker compose stop payment-service
# create an order -> stays PENDING at CHARGE_PAYMENT
docker compose start payment-service
```
**Expect:** the command waited in Kafka; the order completes after restart. If the detector re-sent the command
meanwhile, the duplicate is absorbed by business-level idempotency. Look for it in the logs.

### 6.6 Broken trace propagation
In `OutboxRelay.publish` replace `TraceContext.fromTraceparent(row.traceparent()).makeCurrent()` with
`Context.root().makeCurrent()`, rebuild (`mvn package -DskipTests && docker compose up --build -d`).
**Expect:** each saga step becomes a separate trace. Revert and explain why in your postmortem.

### 6.7 Load test (optional)
Install [k6](https://k6.io) and send 50–100 orders/second. Find the bottleneck in traces: DB pool? outbox poll
interval? consumer concurrency? Try `minishop.outbox.poll-interval-ms` and `spring.kafka.listener.concurrency`.

### Postmortem template
```
## <Incident name>
Symptom:        what a user/monitor would see
Detection:      which trace / log / dashboard showed it
Root cause:     ...
Fix / mitigation: ...
Prevention:     what we'd add (alert, test, pattern)
```

---

## 7. Extension tasks (make it yours)

1. **Ambiguous payment outcome.** After a timeout, store the payment as `UNKNOWN` instead of `FAILED`,
   ask the bank `GET /charges/{key}` (with retries) and only then reply. Or add a `RefundPayment` compensation.
2. **Integration tests with Testcontainers** (Postgres + Kafka) and Awaitility: happy path, compensation,
   duplicate delivery, bank outage. This is the single most valuable addition for a reviewer.
3. **Metrics:** Micrometer + Prometheus + Grafana. RED metrics per service plus counters for saga outcomes
   (`saga.completed`, `saga.rolled_back`, `saga.failed`) and an alert on `saga.failed > 0`.
4. **Tail sampling** in the Collector: keep 100 % of error and slow traces, 10 % of the rest.
5. **Debezium CDC** instead of the polling relay.
6. **Kubernetes** (kind/minikube): liveness/readiness probes (`/actuator/health/liveness`, `/readiness`),
   2 replicas of each service. That proves `SKIP LOCKED` and consumer groups work under scale-out.
7. **Housekeeping jobs:** delete published outbox rows and old `processed_messages` after N days.

---

## 8. Design decisions (interview talking points)

- **Why not 2PC?** Kafka and most modern stores don't support XA; 2PC blocks participants while the coordinator is down.
- **Orchestration vs choreography:** the flow is explicit in one class, easy to trace and to add steps to. The trade-off is
  a central component that must not become a "god service" — it only coordinates, business rules stay in participants.
- **Why a shared `common` module?** It holds infrastructure (outbox, idempotency) and message contracts. The contracts
  create compile-time coupling between services; in a bigger system you'd use a schema registry (Avro/Protobuf)
  and generate classes per service instead.
- **Why JdbcTemplate, not JPA?** The important SQL (`FOR UPDATE`, `SKIP LOCKED`, `ON CONFLICT`, atomic updates)
  is visible instead of hidden behind an ORM. JPA would work too.
- **At-least-once + idempotency** instead of chasing "exactly-once": simpler and works across DB and Kafka.
- **Isolation anomalies:** other requests can see a `PENDING` order with reserved stock. `PENDING` is a *semantic lock*;
  the UI must treat it as "not final".

---

## 9. Troubleshooting

- **No traces in Jaeger:** `docker compose logs otel-collector`; check `OTEL_EXPORTER_OTLP_ENDPOINT` uses port **4318** (the agent's default protocol is http/protobuf).
- **App can't reach Kafka from the IDE:** use `localhost:29092`, not `9092`.
- **Resilience4j fails to start complaining about exponential + randomized wait:** remove `enable-randomized-wait` and
  `randomized-wait-factor`, or configure `IntervalFunction.ofExponentialRandomBackoff(...)` in a `RetryConfigCustomizer` bean.
- **Reset everything:** `docker compose down -v` (drops the databases and Kafka data).
