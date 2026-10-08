package com.minishop.common.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minishop.common.messaging.Messages.SagaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/** A Kafka record reduced to what our handlers need. */
public record InboundMessage(String id, String type, String key, String json) {

    public static InboundMessage from(ConsumerRecord<String, String> record) {
        return new InboundMessage(
                header(record, MessageHeaderNames.MESSAGE_ID),
                header(record, MessageHeaderNames.MESSAGE_TYPE),
                record.key(),
                record.value());
    }

    public SagaMessage payload(ObjectMapper mapper) {
        try {
            return mapper.readValue(json, Messages.classFor(type));
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Cannot parse " + type + " message " + id, e);
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new InvalidMessageException("Missing header '" + name + "' at offset " + record.offset());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
