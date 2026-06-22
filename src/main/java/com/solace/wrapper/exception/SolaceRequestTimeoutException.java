package com.solace.wrapper.exception;

/**
 * Thrown when a Solace request-reply request does not receive a reply within the configured timeout.
 */
public class SolaceRequestTimeoutException extends SolaceRequestException {

    public SolaceRequestTimeoutException(String message) {
        super(message);
    }

    public SolaceRequestTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
