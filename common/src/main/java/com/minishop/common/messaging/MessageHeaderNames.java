package com.minishop.common.messaging;

public final class MessageHeaderNames {

    /** Unique per message. Consumers use it for de-duplication. */
    public static final String MESSAGE_ID = "message-id";

    /** Simple class name of the payload, e.g. "ReserveStock". */
    public static final String MESSAGE_TYPE = "message-type";

    private MessageHeaderNames() {
    }
}
