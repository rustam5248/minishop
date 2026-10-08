package com.minishop.payment.bank;

/** The bank answered "no" (HTTP 402). Not retried, not counted as a bank failure. */
public class PaymentDeclinedException extends RuntimeException {

    public PaymentDeclinedException(String message) {
        super(message);
    }
}
