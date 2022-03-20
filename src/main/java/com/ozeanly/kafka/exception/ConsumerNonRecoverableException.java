package com.ozeanly.kafka.exception;

public class ConsumerNonRecoverableException extends RuntimeException {

    public ConsumerNonRecoverableException(String message) {
        super(message);
    }

    public ConsumerNonRecoverableException(Throwable cause) {
        super(cause);
    }
}
