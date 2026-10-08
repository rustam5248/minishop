package com.minishop.common.messaging;

import com.minishop.common.tracing.TraceContext;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Transactional Outbox, publish side (the "polling publisher" variant).
 *
 * Guarantees AT-LEAST-ONCE delivery: if we crash after Kafka accepted a message but before
 * published_at is committed, the message is sent again on the next run. That is why every
 * consumer must be idempotent (see IdempotencyGuard).
 *
 * FOR UPDATE SKIP LOCKED lets several instances of the same service run the relay in parallel
 * without publishing the same row concurrently.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final RowMapper<OutboxRow> ROW_MAPPER = (rs, rowNum) -> new OutboxRow(
            rs.getObject("id", UUID.class),
            rs.getString("topic"),
            rs.getString("message_key"),
            rs.getString("message_type"),
            rs.getString("payload"),
            rs.getString("traceparent"));

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${minishop.outbox.poll-interval-ms:500}")
    public void publishPending() {
        try {
            // Polling every 500 ms would flood Jaeger with useless root traces; see TraceContext.
            TraceContext.runUntraced(() -> tx.executeWithoutResult(status -> {
                List<OutboxRow> rows = jdbc.query("""
                        SELECT id, topic, message_key, message_type, payload::text AS payload, traceparent
                        FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY seq
                        LIMIT 100
                        FOR UPDATE SKIP LOCKED
                        """, ROW_MAPPER);
                for (OutboxRow row : rows) {
                    publish(row);
                    jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", row.id());
                }
            }));
        } catch (RuntimeException e) {
            // The transaction rolled back; unpublished rows stay and are retried on the next run.
            log.warn("Outbox relay run failed, will retry: {}", e.getMessage());
        }
    }

    private void publish(OutboxRow row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
        record.headers().add(MessageHeaderNames.MESSAGE_ID, row.id().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaderNames.MESSAGE_TYPE, row.type().getBytes(StandardCharsets.UTF_8));

        // THE KEY TRACING TRICK: restore the trace context that was active when the business
        // transaction wrote this row. The OTel agent's Kafka instrumentation then creates the
        // producer span as a child of the original request and injects "traceparent" into the
        // Kafka headers, so the consumer continues the SAME trace. Remove this line and every
        // saga step shows up in Jaeger as a separate, disconnected trace.
        try (Scope ignored = TraceContext.fromTraceparent(row.traceparent()).makeCurrent()) {
            kafka.send(record).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing outbox message " + row.id(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish outbox message " + row.id(), e);
        }
        log.debug("Published {} {} to {}", row.type(), row.id(), row.topic());
    }

    private record OutboxRow(UUID id, String topic, String key, String type, String payload, String traceparent) {
    }
}
