package com.solace.wrapper.exception;

/**
 * Thrown when a Solace request-reply replier cannot be created or started.
 */
public class SolaceReplierException extends RuntimeException {

    public SolaceReplierException(String message) {
        super(message);
    }

    public SolaceReplierException(String message, Throwable cause) {
        super(message, cause);
    }
}
