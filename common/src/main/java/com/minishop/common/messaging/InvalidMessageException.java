package com.minishop.common.messaging;

/**
 * A message that can never be processed successfully (bad JSON, unknown type, missing headers).
 * It is NOT retried: the Kafka error handler sends it straight to the dead-letter topic.
 */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
