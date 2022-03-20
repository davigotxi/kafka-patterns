package com.ozeanly.kafka.consumer.nonblockingretries;

import com.ozeanly.kafka.exception.ConsumerRecoverableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class implements a non-blocking retry / dlt functionality strategy with
 * delayed retry topics (see https://eng.uber.com/reliable-reprocessing/ and
 * https://docs.spring.io/spring-kafka/reference/html/#retry-topic)
 * <p>
 * The default configurations are as follow:
 * <ul>
 *     <li>4 attempts, meaning 1 main topic, 3 retry topics and a DLT topic</li>
 *     <li>Exponential back-off strategy</li>
 *     <ul>
 *         <li>After 5m ~ Delay = 300000</li>
 *         <li>After 15m ~ Delay = 300000 * 3 = 900000</li>
 *         <li>After 45m ~ Delay = 900000 * 3 = 270000</li>
 *     </ul>
 *     <li>The names for the different topics are segment,
 *     segment-retry-0, segment-retry-1, segment-retry-2, segment-dlt</li>
 *     <li>Only errors that result on a SegmentRecoverableException are retried,
 *     the rest are sent directly to the DLT topic</li>
 * </ul>
 */
public class MessagesKafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MessagesKafkaConsumer.class);
    private static final Logger DLT_LOG = LoggerFactory.getLogger("DLT");

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
     * Configures a {@link RetryableTopic} with a {@link KafkaListener}, this will be executed
     * for both the main segment topic and the retry topics
     *
     * @param message - the message received
     * @param topic - the topic where the message has been received from
     * @param partition - the partition where the message has been received from
     * @param offset - the offset of the message
     */
    @RetryableTopic(
            attempts = "${sample.app.kafka.topic.attempts:4}",
            backoff = @Backoff(
                    delayExpression = "${sample.app.kafka.topic.backoff.delayms:300000}",
                    multiplierExpression = "${sample.app.kafka.topic.backoff.multiplier:3}"),
            autoCreateTopics = "false",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            kafkaTemplate = "messagesKafkaTemplate",
            include = ConsumerRecoverableException.class)
    @KafkaListener(topics = "${sample.app.kafka.messages.topic.name:test-messages}",
            containerFactory = "messagesKafkaListenerContainerFactory")
    public void messagesListener(@Payload String message,
                                 @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                 @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                                 @Header(KafkaHeaders.OFFSET) Long offset) {
        LOG.debug("received message=[{}], from topic={} partition={} and offset={}", message, topic, partition, offset);
        messageProcessors.forEach(processor -> processor.accept(message));
    }

    /**
     * Basic DLT handler that  logs the message into a separate logger
     * that can be configured to be printed out to a separate file
     *
     * @param message
     * @param exStackTrace
     * @param partition
     * @param offset
     */
    @DltHandler
    public void dlt(@Payload String message,
                    @Header(KafkaHeaders.EXCEPTION_STACKTRACE) String exStackTrace,
                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                    @Header(KafkaHeaders.OFFSET) Long offset
    ) {
        DLT_LOG.info("{},{},message=[{}], exception=[{}]", partition, offset, message, exStackTrace);
    }
}
