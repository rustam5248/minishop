package com.minishop.inventory;

import com.minishop.common.messaging.InboundMessage;
import com.minishop.common.messaging.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCommandListener {

    private final InventoryService inventory;

    public InventoryCommandListener(InventoryService inventory) {
        this.inventory = inventory;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onMessage(ConsumerRecord<String, String> record) {
        inventory.handle(InboundMessage.from(record));
    }
}
