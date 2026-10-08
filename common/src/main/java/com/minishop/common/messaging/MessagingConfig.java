package com.minishop.common.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableScheduling
public class MessagingConfig {

    /** Topics are declared in code so the setup is reproducible (no manual kafka-topics.sh). */
    @Bean
    public KafkaAdmin.NewTopics minishopTopics() {
        return new KafkaAdmin.NewTopics(
                topic(Topics.INVENTORY_COMMANDS),
                topic(Topics.PAYMENT_COMMANDS),
                topic(Topics.ORDER_SAGA_REPLIES),
                topic(Topics.INVENTORY_COMMANDS + Topics.DLQ_SUFFIX),
                topic(Topics.PAYMENT_COMMANDS + Topics.DLQ_SUFFIX),
                topic(Topics.ORDER_SAGA_REPLIES + Topics.DLQ_SUFFIX));
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    /**
     * Retry a failing message 3 times (1s apart), then move it to "<topic>.dlq" so one
     * poison message cannot block its partition forever. Invalid messages skip the retries.
     * Spring Boot automatically plugs this bean into the @KafkaListener container factory.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + Topics.DLQ_SUFFIX, -1));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
        handler.addNotRetryableExceptions(InvalidMessageException.class);
        return handler;
    }
}
