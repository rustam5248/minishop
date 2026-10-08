package com.minishop.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minishop.common.messaging.IdempotencyGuard;
import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.InvalidMessageException;
import com.minishop.common.messaging.Messages.ReleaseStock;
import com.minishop.common.messaging.Messages.ReserveStock;
import com.minishop.common.messaging.Messages.SagaMessage;
import com.minishop.common.messaging.Messages.StockReleased;
import com.minishop.common.messaging.Messages.StockReserveFailed;
import com.minishop.common.messaging.Messages.StockReserved;
import com.minishop.common.messaging.OutboxWriter;
import com.minishop.common.messaging.Topics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryService {

    static final String CONSUMER = "inventory-service.commands";
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final JdbcTemplate jdbc;
    private final OutboxWriter outbox;
    private final IdempotencyGuard idempotency;
    private final ObjectMapper mapper;
    private final ChaosSettings chaos;

    public InventoryService(JdbcTemplate jdbc, OutboxWriter outbox, IdempotencyGuard idempotency,
                            ObjectMapper mapper, ChaosSettings chaos) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.mapper = mapper;
        this.chaos = chaos;
    }

    @Transactional
    @WithSpan("inventory.handle-command")
    public void handle(InboundMessage message) {
        chaos.maybeFail();
        if (!idempotency.firstDelivery(CONSUMER, message.id())) {
            log.info("Duplicate message {} ({}) skipped", message.id(), message.type());
            return;
        }
        SagaMessage command = message.payload(mapper);
        Span.current().setAttribute("order.id", command.orderId().toString());
        Span.current().setAttribute("inventory.command", message.type());

        switch (command) {
            case ReserveStock c -> reserve(c);
            case ReleaseStock c -> release(c);
            default -> throw new InvalidMessageException("Unexpected command " + message.type());
        }
    }

    private void reserve(ReserveStock c) {
        // Business-level idempotency: the orchestrator may re-send ReserveStock with a NEW message id
        // (stuck-saga retry). The processed_messages table would not catch that; this check does.
        Optional<String> existing = jdbc.query("SELECT status FROM reservations WHERE order_id = ?",
                (rs, rowNum) -> rs.getString("status"), c.orderId()).stream().findFirst();
        if (existing.isPresent()) {
            if ("RESERVED".equals(existing.get())) {
                outbox.save(Topics.ORDER_SAGA_REPLIES, new StockReserved(c.orderId()));
            } else {
                log.warn("ReserveStock for order {} ignored: reservation already {}", c.orderId(), existing.get());
            }
            return;
        }

        // Atomic check-and-decrement: no race between "check stock" and "update stock".
        int updated = jdbc.update(
                "UPDATE products SET available = available - ? WHERE id = ? AND available >= ?",
                c.quantity(), c.productId(), c.quantity());
        if (updated == 0) {
            outbox.save(Topics.ORDER_SAGA_REPLIES,
                    new StockReserveFailed(c.orderId(), "Insufficient stock for product " + c.productId()));
            log.info("Order {}: insufficient stock for {}", c.orderId(), c.productId());
            return;
        }
        jdbc.update("""
                INSERT INTO reservations (id, order_id, product_id, quantity, status)
                VALUES (?, ?, ?, ?, 'RESERVED')
                """, UUID.randomUUID(), c.orderId(), c.productId(), c.quantity());
        outbox.save(Topics.ORDER_SAGA_REPLIES, new StockReserved(c.orderId()));
        log.info("Order {}: reserved {} x {}", c.orderId(), c.quantity(), c.productId());
    }

    /** Compensation. Must be idempotent and must never "fail" for business reasons. */
    private void release(ReleaseStock c) {
        List<Map<String, Object>> released = jdbc.queryForList("""
                UPDATE reservations SET status = 'RELEASED', updated_at = now()
                WHERE order_id = ? AND status = 'RESERVED'
                RETURNING product_id, quantity
                """, c.orderId());
        for (Map<String, Object> row : released) {
            jdbc.update("UPDATE products SET available = available + ? WHERE id = ?",
                    row.get("quantity"), row.get("product_id"));
        }
        // Reply even if nothing was reserved: "released" is true either way.
        outbox.save(Topics.ORDER_SAGA_REPLIES, new StockReleased(c.orderId()));
        log.info("Order {}: stock released ({} reservation(s))", c.orderId(), released.size());
    }
}
