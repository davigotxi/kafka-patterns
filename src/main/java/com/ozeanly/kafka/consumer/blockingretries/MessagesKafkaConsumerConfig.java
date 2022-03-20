package com.ozeanly.kafka.consumer.blockingretries;

import com.ozeanly.kafka.KafkaClusterConfig;
import com.ozeanly.kafka.consumer.processor.LoggingMessageProcessor;
import com.ozeanly.kafka.exception.ConsumerNonRecoverableException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@EnableKafka
@Configuration
public class MessagesKafkaConsumerConfig extends KafkaClusterConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MessagesKafkaConsumerConfig.class);

    private String groupId;
    private Integer consumerConcurrency;
    private Integer retryMaxAttempts;

    @Value("${sample.app.kafka.topic.consumerGroupId:test-group}")
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Value("${sample.app.kafka.topic.consumer.concurrency:6}")
    public void setConsumerConcurrency(Integer consumerConcurrency) {
        this.consumerConcurrency = consumerConcurrency;
    }

    @Value("${sample.app.kafka.topic.consumer.retryMaxAttempts:3}")
    public void setRetryMaxAttempts(Integer retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> messagesKafkaListenerContainerFactory() {
        return kafkaListenerContainerFactory(new StringDeserializer());
    }

    private <V> ConcurrentKafkaListenerContainerFactory<String, V> kafkaListenerContainerFactory(Deserializer<V> valueDeserializer) {
        ConcurrentKafkaListenerContainerFactory<String, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(valueDeserializer));
        factory.setConcurrency(consumerConcurrency);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(retryingErrorHandler());
        return factory;
    }


    private CommonErrorHandler retryingErrorHandler() {
        var errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, retryMaxAttempts));
        errorHandler.addNotRetryableExceptions(ConsumerNonRecoverableException.class);
        return errorHandler;
    }


    private <V> ConsumerFactory<String, V> consumerFactory(Deserializer<V> valueDeserializer) {
        var props = commonConfigurations();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public MessagesKafkaConsumer messagesKafkaConsumer(LoggingMessageProcessor loggingMessageProcessor) {
        var kafkaConsumer = new MessagesKafkaConsumer();
        kafkaConsumer.setMessageProcessors(List.of(loggingMessageProcessor));
        return kafkaConsumer;
    }
}