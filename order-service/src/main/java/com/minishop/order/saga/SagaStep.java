package com.minishop.order.saga;

/**
 * Step order matters: the step that is hardest to undo (charging money) goes LAST.
 * It is the "pivot transaction": once it succeeds, the saga can only move forward.
 */
public enum SagaStep {
    RESERVE_STOCK,
    CHARGE_PAYMENT,
    RELEASE_STOCK,   // compensation of RESERVE_STOCK
    DONE
}
