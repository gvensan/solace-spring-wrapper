package com.solace.wrapper.exception;

/**
 * Custom exception for Solace consumer related errors.
 */
public class SolaceConsumerException extends RuntimeException {

    public SolaceConsumerException(String message) {
        super(message);
    }

    public SolaceConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
