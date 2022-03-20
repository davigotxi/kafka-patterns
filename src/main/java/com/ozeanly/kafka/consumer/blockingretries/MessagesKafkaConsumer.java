package com.ozeanly.kafka.consumer.blockingretries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;


public class MessagesKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MessagesKafkaConsumer.class);

    private List<Consumer<String>> messageProcessors = Collections.emptyList();

    /**
     * Sets a list of message processors that will execute in order for each
     * received message
     * @param messageProcessors
     */
    public void setMessageProcessors(List<Consumer<String>> messageProcessors) {
        this.messageProcessors = messageProcessors;
    }

    /**
     * Configures a {@link KafkaListener} that will be executed for the topic
     * @param message
     * @param topic
     * @param partition
     * @param offset
     */
    @KafkaListener(topics = "${sample.app.kafka.messages.topic.name:test-messages}",
            containerFactory = "messagesKafkaListenerContainerFactory")
    public void messagesListener(@Payload String message,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                                 @Header(KafkaHeaders.OFFSET) Long offset) {
        LOG.debug("received message=[{}], from topic={} partition={} and offset={}", message, topic, partition, offset);
        messageProcessors.forEach(processor -> processor.accept(message));
    }
}
