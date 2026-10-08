package com.minishop.common.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Idempotent Consumer pattern.
 *
 * Kafka + outbox give at-least-once delivery, so the same message WILL sometimes arrive twice.
 * We record each processed message id in the same transaction as the business change:
 * if the business change commits, the id is recorded; if it rolls back, so does the id.
 */
@Component
public class IdempotencyGuard {

    private final JdbcTemplate jdbc;

    public IdempotencyGuard(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Returns true the first time a message is seen by this consumer, false for duplicates.
     * ON CONFLICT DO NOTHING avoids an exception, which would otherwise abort the Postgres transaction.
     */
    public boolean firstDelivery(String consumer, String messageId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("firstDelivery must run in the same transaction as the business change");
        }
        int inserted = jdbc.update("""
                INSERT INTO processed_messages (consumer, message_id) VALUES (?, ?)
                ON CONFLICT DO NOTHING
                """, consumer, messageId);
        return inserted == 1;
    }

    /** Cheap read-only pre-check, e.g. before an expensive remote call. Not a replacement for firstDelivery. */
    public boolean alreadyProcessed(String consumer, String messageId) {
        Boolean exists = jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM processed_messages WHERE consumer = ? AND message_id = ?)",
                Boolean.class, consumer, messageId);
        return Boolean.TRUE.equals(exists);
    }
}
