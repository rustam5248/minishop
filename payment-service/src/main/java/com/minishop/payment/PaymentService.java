package com.minishop.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minishop.common.messaging.IdempotencyGuard;
import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.InvalidMessageException;
import com.minishop.common.messaging.Messages.ChargePayment;
import com.minishop.common.messaging.Messages.PaymentCharged;
import com.minishop.common.messaging.Messages.PaymentFailed;
import com.minishop.common.messaging.Messages.SagaMessage;
import com.minishop.common.messaging.OutboxWriter;
import com.minishop.common.messaging.Topics;
import com.minishop.payment.bank.BankClient;
import com.minishop.payment.bank.PaymentDeclinedException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

/**
 * Unlike the other handlers, this one is NOT one big @Transactional method.
 * Best practice: never keep a DB transaction (and connection) open during a slow remote call.
 * So: check -> call the bank (no transaction) -> short transaction to record the result.
 *
 * If we crash after the bank charged but before our transaction commits, Kafka redelivers the
 * command, we call the bank again with the same Idempotency-Key and get the same answer.
 */
@Service
public class PaymentService {

    static final String CONSUMER = "payment-service.commands";
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final JdbcTemplate jdbc;
    private final BankClient bank;
    private final OutboxWriter outbox;
    private final IdempotencyGuard idempotency;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;

    public PaymentService(JdbcTemplate jdbc, BankClient bank, OutboxWriter outbox,
                          IdempotencyGuard idempotency, ObjectMapper mapper, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.bank = bank;
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.mapper = mapper;
        this.tx = tx;
    }

    @WithSpan("payment.handle-command")
    public void handle(InboundMessage message) {
        SagaMessage payload = message.payload(mapper);
        if (!(payload instanceof ChargePayment command)) {
            throw new InvalidMessageException("Unexpected command " + message.type());
        }
        Span.current().setAttribute("order.id", command.orderId().toString());

        if (idempotency.alreadyProcessed(CONSUMER, message.id())) {
            log.info("Duplicate message {} skipped", message.id());
            return;
        }

        // Business-level idempotency: re-sent command (new message id) for an order we already handled.
        Optional<PaymentRecord> existing = findByOrderId(command.orderId());
        if (existing.isPresent()) {
            log.info("Order {} already has payment {}, re-sending the stored outcome",
                    command.orderId(), existing.get().status());
            tx.executeWithoutResult(status -> {
                if (idempotency.firstDelivery(CONSUMER, message.id())) {
                    outbox.save(Topics.ORDER_SAGA_REPLIES, existing.get().toReply());
                }
            });
            return;
        }

        ChargeOutcome outcome = chargeAtBank(command);   // remote call, outside any transaction

        tx.executeWithoutResult(status -> {
            if (!idempotency.firstDelivery(CONSUMER, message.id())) {
                return; // a concurrent duplicate won the race
            }
            jdbc.update("""
                            INSERT INTO payments (id, order_id, customer_id, amount, status, bank_transaction_id, failure_reason)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                    UUID.randomUUID(), command.orderId(), command.customerId(), command.amount(),
                    outcome.charged() ? "CHARGED" : "FAILED", outcome.transactionId(), outcome.failureReason());
            outbox.save(Topics.ORDER_SAGA_REPLIES, outcome.toReply(command.orderId()));
        });
        log.info("Order {}: payment {}", command.orderId(),
                outcome.charged() ? "CHARGED" : "FAILED (" + outcome.failureReason() + ")");
    }

    private ChargeOutcome chargeAtBank(ChargePayment command) {
        try {
            return ChargeOutcome.success(bank.charge(command.orderId(), command.amount()));
        } catch (PaymentDeclinedException e) {
            return ChargeOutcome.failure("Declined by bank: " + e.getMessage());
        } catch (CallNotPermittedException e) {
            return ChargeOutcome.failure("Bank unavailable (circuit breaker open)");
        } catch (RuntimeException e) {
            // KNOWN LIMITATION (see README, extension task 1): after a TIMEOUT we don't actually know
            // whether the bank charged the card. Treating it as "failed" can leave money charged
            // for a rejected order. Real systems mark it UNKNOWN and reconcile with the bank.
            return ChargeOutcome.failure("Bank call failed: " + e.getClass().getSimpleName());
        }
    }

    private Optional<PaymentRecord> findByOrderId(UUID orderId) {
        return jdbc.query("SELECT order_id, status, bank_transaction_id, failure_reason FROM payments WHERE order_id = ?",
                (rs, rowNum) -> new PaymentRecord(
                        rs.getObject("order_id", UUID.class),
                        rs.getString("status"),
                        rs.getString("bank_transaction_id"),
                        rs.getString("failure_reason")),
                orderId).stream().findFirst();
    }

    private record ChargeOutcome(boolean charged, String transactionId, String failureReason) {

        static ChargeOutcome success(String transactionId) {
            return new ChargeOutcome(true, transactionId, null);
        }

        static ChargeOutcome failure(String reason) {
            return new ChargeOutcome(false, null, reason);
        }

        SagaMessage toReply(UUID orderId) {
            return charged ? new PaymentCharged(orderId, transactionId) : new PaymentFailed(orderId, failureReason);
        }
    }

    private record PaymentRecord(UUID orderId, String status, String transactionId, String failureReason) {

        SagaMessage toReply() {
            return "CHARGED".equals(status)
                    ? new PaymentCharged(orderId, transactionId)
                    : new PaymentFailed(orderId, failureReason);
        }
    }
}
