package com.minishop.common.messaging;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/**
 * The saga's message contracts.
 *
 * Commands tell a participant to do something; replies report the outcome back to the orchestrator.
 * Best practice: only ADD fields to these records, never rename or remove them, so old and new
 * versions of services can run side by side during a rolling deployment.
 */
public final class Messages {

    public interface SagaMessage {
        UUID orderId();
    }

    // ---- Commands (orchestrator -> participants) ----
    public record ReserveStock(UUID orderId, String productId, int quantity) implements SagaMessage {
    }

    public record ReleaseStock(UUID orderId) implements SagaMessage {
    }

    public record ChargePayment(UUID orderId, String customerId, BigDecimal amount) implements SagaMessage {
    }

    // ---- Replies (participants -> orchestrator) ----
    public record StockReserved(UUID orderId) implements SagaMessage {
    }

    public record StockReserveFailed(UUID orderId, String reason) implements SagaMessage {
    }

    public record StockReleased(UUID orderId) implements SagaMessage {
    }

    public record PaymentCharged(UUID orderId, String bankTransactionId) implements SagaMessage {
    }

    public record PaymentFailed(UUID orderId, String reason) implements SagaMessage {
    }

    private static final Map<String, Class<? extends SagaMessage>> TYPES = Map.of(
            "ReserveStock", ReserveStock.class,
            "ReleaseStock", ReleaseStock.class,
            "ChargePayment", ChargePayment.class,
            "StockReserved", StockReserved.class,
            "StockReserveFailed", StockReserveFailed.class,
            "StockReleased", StockReleased.class,
            "PaymentCharged", PaymentCharged.class,
            "PaymentFailed", PaymentFailed.class);

    public static Class<? extends SagaMessage> classFor(String type) {
        Class<? extends SagaMessage> messageClass = TYPES.get(type);
        if (messageClass == null) {
            throw new InvalidMessageException("Unknown message type: " + type);
        }
        return messageClass;
    }

    private Messages() {
    }
}
