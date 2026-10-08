package com.minishop.order.domain;

import com.minishop.order.api.CreateOrderRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderRepository {

    private static final RowMapper<Order> MAPPER = (rs, rowNum) -> new Order(
            rs.getObject("id", UUID.class),
            rs.getString("customer_id"),
            rs.getString("product_id"),
            rs.getInt("quantity"),
            rs.getBigDecimal("amount"),
            rs.getString("status"),
            rs.getString("reject_reason"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant());

    private final JdbcTemplate jdbc;

    public OrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertPending(UUID id, CreateOrderRequest r) {
        jdbc.update("""
                INSERT INTO orders (id, customer_id, product_id, quantity, amount, status)
                VALUES (?, ?, ?, ?, ?, 'PENDING')
                """, id, r.customerId(), r.productId(), r.quantity(), r.amount());
    }

    public Optional<Order> findById(UUID id) {
        return jdbc.query("SELECT * FROM orders WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public void markApproved(UUID id) {
        jdbc.update("UPDATE orders SET status = 'APPROVED', updated_at = now() WHERE id = ?", id);
    }

    public void markRejected(UUID id, String reason) {
        jdbc.update("UPDATE orders SET status = 'REJECTED', reject_reason = ?, updated_at = now() WHERE id = ?",
                reason, id);
    }
}
