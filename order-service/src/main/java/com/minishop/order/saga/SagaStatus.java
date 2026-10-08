package com.minishop.order.saga;

public enum SagaStatus {
    STARTED,        // moving forward
    COMPENSATING,   // undoing completed steps
    COMPLETED,      // order approved
    ROLLED_BACK,    // order rejected, everything undone cleanly
    FAILED          // gave up: needs a human (see StuckSagaDetector)
}
