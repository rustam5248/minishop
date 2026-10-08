package com.minishop.payment;

import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCommandListener {

    private final PaymentService payments;

    public PaymentCommandListener(PaymentService payments) {
        this.payments = payments;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
    public void onMessage(ConsumerRecord<String, String> record) {
        payments.handle(InboundMessage.from(record));
    }
}
