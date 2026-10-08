package com.minishop.order.saga;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class SagaRepository {

    private static final RowMapper<SagaInstance> MAPPER = (rs, rowNum) -> new SagaInstance(
            rs.getObject("id", UUID.class),
            rs.getObject("order_id", UUID.class),
            SagaStep.valueOf(rs.getString("current_step")),
            SagaStatus.valueOf(rs.getString("status")),
            rs.getInt("attempts"),
            rs.getString("failure_reason"));

    private final JdbcTemplate jdbc;

    public SagaRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void start(UUID orderId) {
        jdbc.update("""
                INSERT INTO saga_instance (id, order_id, current_step, status)
                VALUES (?, ?, 'RESERVE_STOCK', 'STARTED')
                """, UUID.randomUUID(), orderId);
    }

    /** Row lock: two replies for the same order are processed one after another, never interleaved. */
    public Optional<SagaInstance> findByOrderIdForUpdate(UUID orderId) {
        return jdbc.query("SELECT * FROM saga_instance WHERE order_id = ? FOR UPDATE", MAPPER, orderId)
                .stream().findFirst();
    }

    public List<UUID> findStaleIds(Instant cutoff, int limit) {
        return jdbc.query("""
                SELECT id FROM saga_instance
                WHERE status IN ('STARTED', 'COMPENSATING') AND updated_at < ?
                ORDER BY updated_at
                LIMIT ?
                """, (rs, rowNum) -> rs.getObject("id", UUID.class), Timestamp.from(cutoff), limit);
    }

    /** Re-checks staleness under a lock: a reply may have arrived since findStaleIds ran. */
    public Optional<SagaInstance> findStaleByIdForUpdate(UUID id, Instant cutoff) {
        return jdbc.query("""
                SELECT * FROM saga_instance
                WHERE id = ? AND status IN ('STARTED', 'COMPENSATING') AND updated_at < ?
                FOR UPDATE
                """, MAPPER, id, Timestamp.from(cutoff)).stream().findFirst();
    }

    public void moveTo(UUID id, SagaStep step, SagaStatus status, String failureReason) {
        jdbc.update("""
                UPDATE saga_instance
                SET current_step = ?, status = ?, failure_reason = COALESCE(?, failure_reason),
                    attempts = 0, updated_at = now()
                WHERE id = ?
                """, step.name(), status.name(), failureReason, id);
    }

    public void recordRetry(UUID id) {
        jdbc.update("UPDATE saga_instance SET attempts = attempts + 1, updated_at = now() WHERE id = ?", id);
    }

    public void markFailed(UUID id, String reason) {
        jdbc.update("""
                UPDATE saga_instance SET status = 'FAILED', failure_reason = ?, updated_at = now()
                WHERE id = ?
                """, reason, id);
    }
}
