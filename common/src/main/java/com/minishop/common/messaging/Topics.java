package com.minishop.common.messaging;

/**
 * Command topics are owned by the participant that executes the command.
 * The reply topic is owned by the orchestrator.
 */
public final class Topics {

    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String ORDER_SAGA_REPLIES = "order.saga-replies";

    public static final String DLQ_SUFFIX = ".dlq";

    private Topics() {
    }
}
