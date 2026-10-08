package com.minishop.order.saga;

import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Thin adapter: Kafka -> orchestrator. Kept separate so the @Transactional boundary is
 * on the orchestrator. If handleReply throws, the transaction rolls back, the offset is not
 * committed and the DefaultErrorHandler retries (then sends to order.saga-replies.dlq).
 */
@Component
public class SagaReplyListener {

    private final OrderSagaOrchestrator orchestrator;

    public SagaReplyListener(OrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = Topics.ORDER_SAGA_REPLIES)
    public void onMessage(ConsumerRecord<String, String> record) {
        orchestrator.handleReply(InboundMessage.from(record));
    }
}
