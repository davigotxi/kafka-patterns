package com.ozeanly.kafka.consumer.blockingretries;

import com.ozeanly.kafka.KafkaClusterConfig;
import com.ozeanly.kafka.consumer.processor.LoggingMessageProcessor;
import com.ozeanly.kafka.exception.ConsumerNonRecoverableException;
import com.ozeanly.kafka.exception.ConsumerRecoverableException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;



@SpringBootTest(classes =
        MessagesKafkaConsumerContainersTestIT.MessagesKafkaConsumerConfigCustomHelperApp.class)
@TestPropertySource(properties = {
        "spring.main.allow-bean-definition-overriding=true",
})
@DirtiesContext
@Testcontainers
public  class MessagesKafkaConsumerContainersTestIT {

    public static final String TEST_MESSAGES_TOPIC = "test-messages";
    public static final String TEST_MESSAGE = "fooBar";

    public static final String EXCEPTION_MSG = "This is a exception thrown for test purposes, IGNORE ME";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.4"));

    @DynamicPropertySource
    static void addDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrap-servers", kafka::getBootstrapServers);
    }


    @MockBean
    LoggingMessageProcessor loggingMessageProcessor;

    @Autowired
    MessagesKafkaConsumer messagesKafkaConsumer;

    KafkaTemplate<String, String> kafkaTemplate;

    CountDownLatch successfullyProcessedLatch;

    @Captor
    ArgumentCaptor<String> messageArgumentCaptor;


    @BeforeEach
    void setUp() throws Exception {
        successfullyProcessedLatch = new CountDownLatch(1);

        messagesKafkaConsumer.setMessageProcessors(List.of(
                loggingMessageProcessor,
                message -> successfullyProcessedLatch.countDown()));

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(),
                new StringSerializer()
        ));
    }

    @Test
    void canSendAndReceiveWithoutErrors() throws Exception {
        //given
        var msg = TEST_MESSAGE;

        //when
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        boolean msgSent = successfullyProcessedLatch.await(5, TimeUnit.SECONDS); //wait to receive message

        //then
        verify(loggingMessageProcessor).accept(messageArgumentCaptor.capture());
        var messageReceived = messageArgumentCaptor.getValue();
        assertAll(
                () -> assertTrue(msgSent),
                () -> assertEquals(msg, messageReceived)
        );
    }


    @Test
    void failedMessageIsReattemptedOnceAfterOneRecoverableFailure() throws Exception {
        //given
        var msg = TEST_MESSAGE;
        doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))            // 1st retry
                .doNothing().when(loggingMessageProcessor).accept(any(String.class));

        //when
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        boolean msgSent = successfullyProcessedLatch.await(5, TimeUnit.SECONDS); //wait to receive message

        //then
        verify(loggingMessageProcessor, times(2)).accept(any(String.class));
        assertTrue(msgSent);
    }


    @Test
    void failedMessageIsReattemptedThreeTimeAfterTwoRecoverableFailures() throws Exception {
        //given
        var msg = TEST_MESSAGE;
        doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))            // 1st retry
                .doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))   // 2nd retry
                .doNothing().when(loggingMessageProcessor).accept(any(String.class));

        //when
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        boolean msgSent = successfullyProcessedLatch.await(5, TimeUnit.SECONDS); //wait to receive message

        //then
        verify(loggingMessageProcessor, times(3)).accept(any(String.class));
        assertTrue(msgSent);
    }


    @Test
    void failedMessageIsDiscardedAfterMaxRecoverableFailures() throws Exception {
        //given
        var msg = TEST_MESSAGE;
        doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))            // 1st retry
                .doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))   // 2nd retry
                .doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))   // 3rd retry
                .doThrow(new ConsumerRecoverableException(EXCEPTION_MSG))   // errorHandler should kick-in
                .doNothing().when(loggingMessageProcessor).accept(any(String.class));

        //when
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        boolean msgSent = successfullyProcessedLatch.await(5, TimeUnit.SECONDS); //wait to receive message

        //then
        verify(loggingMessageProcessor, times(4)).accept(any(String.class));
        assertFalse(msgSent);
    }


    @Test
    void failedMessagesAreDiscardedAfterNonRecoverableFailures() throws Exception {
        //given
        var msg = TEST_MESSAGE;
        doThrow(new ConsumerNonRecoverableException(EXCEPTION_MSG))            // discarded
                .doThrow(new ConsumerNonRecoverableException(EXCEPTION_MSG))   // discarded
                .doNothing().when(loggingMessageProcessor).accept(any(String.class));

        //when
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        kafkaTemplate.send(TEST_MESSAGES_TOPIC, msg).get(5, TimeUnit.SECONDS); //wait to send message
        boolean msgSent = successfullyProcessedLatch.await(5, TimeUnit.SECONDS); //wait to receive message

        //then
        verify(loggingMessageProcessor, times(2)).accept(any(String.class));
        assertFalse(msgSent);
    }


    @SpringBootApplication
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    @ComponentScan(basePackages = {"com.ozeanly.kafka.consumer.blockingretries"})
    public static class MessagesKafkaConsumerConfigCustomHelperApp extends KafkaClusterConfig {
        @Bean
        public KafkaAdmin kafkaAdmin() {
            final Map<String, Object> configs = commonConfigurations();
            return new KafkaAdmin(configs);
        }

        @Bean
        public NewTopic messagesTopic() {
            return TopicBuilder.name(TEST_MESSAGES_TOPIC).partitions(1).replicas(1).build();
        }
    }
}