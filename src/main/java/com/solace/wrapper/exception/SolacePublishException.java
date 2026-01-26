package com.solace.wrapper.exception;

/**
 * Custom exception for Solace publishing related errors.
 */
public class SolacePublishException extends RuntimeException {

    public SolacePublishException(String message) {
        super(message);
    }

    public SolacePublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
