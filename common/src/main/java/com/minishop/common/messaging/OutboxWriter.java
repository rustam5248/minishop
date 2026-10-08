package com.minishop.common.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minishop.common.messaging.Messages.SagaMessage;
import com.minishop.common.tracing.TraceContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/**
 * Transactional Outbox, write side.
 *
 * Never call kafkaTemplate.send() from business code: if the app crashes between the DB commit
 * and the send, the database and Kafka disagree forever (the "dual-write problem").
 * Instead we insert the message into the outbox table inside the SAME transaction as the business
 * change. Either both are committed or neither is. OutboxRelay publishes the row later.
 */
@Component
public class OutboxWriter {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public OutboxWriter(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public void save(String topic, SagaMessage message) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("OutboxWriter.save must be called inside a DB transaction");
        }
        jdbc.update("""
                        INSERT INTO outbox (id, topic, message_key, message_type, payload, traceparent)
                        VALUES (?, ?, ?, ?, ?::jsonb, ?)
                        """,
                UUID.randomUUID(),
                topic,
                message.orderId().toString(),          // key = orderId -> per-order ordering in Kafka
                message.getClass().getSimpleName(),
                toJson(message),
                TraceContext.currentTraceparent());     // remember the trace, the relay runs later
    }

    private String toJson(Object message) {
        try {
            return mapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize " + message, e);
        }
    }
}
