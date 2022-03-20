package com.ozeanly.kafka.consumer.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
public class LoggingMessageProcessor implements Consumer<String> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingMessageProcessor.class);

    @Override
    public void accept(String message) {
        LOG.info("message received -> {}", message);
    }
}
