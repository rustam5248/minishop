package com.minishop.order.saga;

import com.minishop.common.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Sagas can get stuck: a participant is down, a message went to a DLQ, a reply was lost.
 * Production systems always need a timeout mechanism for long-running processes.
 */
@Component
public class StuckSagaDetector {

    private static final Logger log = LoggerFactory.getLogger(StuckSagaDetector.class);

    private final SagaRepository sagas;
    private final OrderSagaOrchestrator orchestrator;
    private final long stuckAfterSeconds;

    public StuckSagaDetector(SagaRepository sagas,
                             OrderSagaOrchestrator orchestrator,
                             @Value("${minishop.saga.stuck-after-seconds:60}") long stuckAfterSeconds) {
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.stuckAfterSeconds = stuckAfterSeconds;
    }

    @Scheduled(fixedDelayString = "${minishop.saga.stuck-check-interval-ms:15000}")
    public void resendStuckSagas() {
        Instant cutoff = Instant.now().minusSeconds(stuckAfterSeconds);
        List<UUID> stale = TraceContext.untraced(() -> sagas.findStaleIds(cutoff, 50));
        for (UUID sagaId : stale) {
            try {
                orchestrator.retryStuckSaga(sagaId, cutoff); // traced: shows up in Jaeger as "saga.retry-stuck"
            } catch (RuntimeException e) {
                log.error("Failed to retry stuck saga {}", sagaId, e);
            }
        }
    }
}
