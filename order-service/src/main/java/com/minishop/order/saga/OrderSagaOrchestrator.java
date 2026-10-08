package com.minishop.order.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minishop.common.messaging.IdempotencyGuard;
import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.InvalidMessageException;
import com.minishop.common.messaging.Messages.ChargePayment;
import com.minishop.common.messaging.Messages.PaymentCharged;
import com.minishop.common.messaging.Messages.PaymentFailed;
import com.minishop.common.messaging.Messages.ReleaseStock;
import com.minishop.common.messaging.Messages.ReserveStock;
import com.minishop.common.messaging.Messages.SagaMessage;
import com.minishop.common.messaging.Messages.StockReleased;
import com.minishop.common.messaging.Messages.StockReserveFailed;
import com.minishop.common.messaging.Messages.StockReserved;
import com.minishop.common.messaging.OutboxWriter;
import com.minishop.common.messaging.Topics;
import com.minishop.order.api.CreateOrderRequest;
import com.minishop.order.domain.Order;
import com.minishop.order.domain.OrderRepository;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Orchestration-based saga: this class is the single place where the whole order flow is defined.
 *
 *   RESERVE_STOCK --StockReserved--> CHARGE_PAYMENT --PaymentCharged--> DONE/COMPLETED (APPROVED)
 *        |                                |
 *   StockReserveFailed               PaymentFailed
 *        v                                v
 *   DONE/ROLLED_BACK (REJECTED)      RELEASE_STOCK/COMPENSATING --StockReleased--> DONE/ROLLED_BACK (REJECTED)
 *
 * Every transition = ONE local transaction: update saga + order + write the next command to the outbox.
 */
@Service
public class OrderSagaOrchestrator {

    static final String CONSUMER = "order-service.saga";
    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);

    private final OrderRepository orders;
    private final SagaRepository sagas;
    private final OutboxWriter outbox;
    private final IdempotencyGuard idempotency;
    private final ObjectMapper mapper;
    private final int maxRetries;

    public OrderSagaOrchestrator(OrderRepository orders,
                                 SagaRepository sagas,
                                 OutboxWriter outbox,
                                 IdempotencyGuard idempotency,
                                 ObjectMapper mapper,
                                 @Value("${minishop.saga.max-retries:3}") int maxRetries) {
        this.orders = orders;
        this.sagas = sagas;
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.mapper = mapper;
        this.maxRetries = maxRetries;
    }

    @Transactional
    @WithSpan("saga.start")
    public UUID startSaga(CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        Span.current().setAttribute("order.id", orderId.toString());

        orders.insertPending(orderId, request);
        sagas.start(orderId);
        outbox.save(Topics.INVENTORY_COMMANDS,
                new ReserveStock(orderId, request.productId(), request.quantity()));

        log.info("Order {} created, saga started -> RESERVE_STOCK", orderId);
        return orderId;
    }

    @Transactional
    @WithSpan("saga.handle-reply")
    public void handleReply(InboundMessage message) {
        // Layer 1 of idempotency: exact duplicate delivery of the same message.
        if (!idempotency.firstDelivery(CONSUMER, message.id())) {
            log.info("Duplicate message {} ({}) skipped", message.id(), message.type());
            return;
        }
        SagaMessage reply = message.payload(mapper);
        SagaInstance saga = sagas.findByOrderIdForUpdate(reply.orderId())
                .orElseThrow(() -> new InvalidMessageException("No saga for order " + reply.orderId()));

        Span span = Span.current();
        span.setAttribute("order.id", saga.orderId().toString());
        span.setAttribute("saga.step", saga.step().name());
        span.setAttribute("saga.reply", message.type());

        switch (reply) {
            case StockReserved r -> onStockReserved(saga);
            case StockReserveFailed r -> onStockReserveFailed(saga, r.reason());
            case PaymentCharged r -> onPaymentCharged(saga);
            case PaymentFailed r -> onPaymentFailed(saga, r.reason());
            case StockReleased r -> onStockReleased(saga);
            default -> throw new InvalidMessageException("Unexpected reply type " + message.type());
        }
    }

    private void onStockReserved(SagaInstance saga) {
        if (!expect(saga, SagaStep.RESERVE_STOCK, SagaStatus.STARTED, "StockReserved")) {
            return;
        }
        Order order = orders.findById(saga.orderId()).orElseThrow();
        sagas.moveTo(saga.id(), SagaStep.CHARGE_PAYMENT, SagaStatus.STARTED, null);
        outbox.save(Topics.PAYMENT_COMMANDS, new ChargePayment(order.id(), order.customerId(), order.amount()));
        log.info("Order {}: stock reserved -> CHARGE_PAYMENT", order.id());
    }

    private void onStockReserveFailed(SagaInstance saga, String reason) {
        if (!expect(saga, SagaStep.RESERVE_STOCK, SagaStatus.STARTED, "StockReserveFailed")) {
            return;
        }
        // Nothing was done yet, so nothing to compensate.
        orders.markRejected(saga.orderId(), reason);
        sagas.moveTo(saga.id(), SagaStep.DONE, SagaStatus.ROLLED_BACK, reason);
        log.info("Order {} REJECTED: {}", saga.orderId(), reason);
    }

    private void onPaymentCharged(SagaInstance saga) {
        if (!expect(saga, SagaStep.CHARGE_PAYMENT, SagaStatus.STARTED, "PaymentCharged")) {
            return;
        }
        orders.markApproved(saga.orderId());
        sagas.moveTo(saga.id(), SagaStep.DONE, SagaStatus.COMPLETED, null);
        log.info("Order {} APPROVED", saga.orderId());
    }

    private void onPaymentFailed(SagaInstance saga, String reason) {
        if (!expect(saga, SagaStep.CHARGE_PAYMENT, SagaStatus.STARTED, "PaymentFailed")) {
            return;
        }
        sagas.moveTo(saga.id(), SagaStep.RELEASE_STOCK, SagaStatus.COMPENSATING, reason);
        outbox.save(Topics.INVENTORY_COMMANDS, new ReleaseStock(saga.orderId()));
        log.warn("Order {}: payment failed ({}) -> COMPENSATING, releasing stock", saga.orderId(), reason);
    }

    private void onStockReleased(SagaInstance saga) {
        if (!expect(saga, SagaStep.RELEASE_STOCK, SagaStatus.COMPENSATING, "StockReleased")) {
            return;
        }
        orders.markRejected(saga.orderId(), saga.failureReason());
        sagas.moveTo(saga.id(), SagaStep.DONE, SagaStatus.ROLLED_BACK, null);
        log.info("Order {} REJECTED after compensation: {}", saga.orderId(), saga.failureReason());
    }

    /**
     * Layer 2 of idempotency: a late or re-sent reply (new message id) that no longer fits the
     * saga's state is ignored instead of corrupting it.
     */
    private boolean expect(SagaInstance saga, SagaStep step, SagaStatus status, String reply) {
        if (saga.isAt(step, status)) {
            return true;
        }
        log.warn("Ignoring {} for order {}: saga is at {}/{}, expected {}/{}",
                reply, saga.orderId(), saga.step(), saga.status(), step, status);
        return false;
    }

    /**
     * Called by StuckSagaDetector. Re-sends the current step's command (safe because participants
     * are idempotent). After maxRetries the saga is marked FAILED for manual handling.
     */
    @Transactional
    @WithSpan("saga.retry-stuck")
    public void retryStuckSaga(UUID sagaId, Instant cutoff) {
        Optional<SagaInstance> stale = sagas.findStaleByIdForUpdate(sagaId, cutoff);
        if (stale.isEmpty()) {
            return; // it progressed in the meantime
        }
        SagaInstance saga = stale.get();
        Span.current().setAttribute("order.id", saga.orderId().toString());
        Span.current().setAttribute("saga.step", saga.step().name());

        if (saga.attempts() >= maxRetries) {
            String reason = "Stuck at " + saga.step() + " after " + saga.attempts() + " retries";
            sagas.markFailed(saga.id(), reason);
            log.error("MANUAL INTERVENTION NEEDED: saga for order {} FAILED: {}", saga.orderId(), reason);
            return;
        }

        Order order = orders.findById(saga.orderId()).orElseThrow();
        switch (saga.step()) {
            case RESERVE_STOCK -> outbox.save(Topics.INVENTORY_COMMANDS,
                    new ReserveStock(order.id(), order.productId(), order.quantity()));
            case CHARGE_PAYMENT -> outbox.save(Topics.PAYMENT_COMMANDS,
                    new ChargePayment(order.id(), order.customerId(), order.amount()));
            case RELEASE_STOCK -> outbox.save(Topics.INVENTORY_COMMANDS, new ReleaseStock(order.id()));
            case DONE -> {
                return;
            }
        }
        sagas.recordRetry(saga.id());
        log.warn("Order {}: saga stuck at {}, command re-sent (retry {}/{})",
                order.id(), saga.step(), saga.attempts() + 1, maxRetries);
    }
}
