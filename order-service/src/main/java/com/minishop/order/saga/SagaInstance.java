package com.minishop.order.saga;

import java.util.UUID;

public record SagaInstance(UUID id,
                           UUID orderId,
                           SagaStep step,
                           SagaStatus status,
                           int attempts,
                           String failureReason) {

    public boolean isAt(SagaStep expectedStep, SagaStatus expectedStatus) {
        return step == expectedStep && status == expectedStatus;
    }
}
