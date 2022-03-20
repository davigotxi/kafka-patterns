package com.ozeanly.kafka.exception;

public class ConsumerRecoverableException extends RuntimeException {

    public ConsumerRecoverableException(String message) {
        super(message);
    }

    public ConsumerRecoverableException(Throwable cause) {
        super(cause);
    }
}
