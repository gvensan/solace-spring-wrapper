package com.solace.wrapper.exception;

/**
 * Thrown when a Solace request-reply request fails (broker rejection, serialization error,
 * connectivity, etc.). {@link SolaceRequestTimeoutException} is a specialization for reply timeouts.
 */
public class SolaceRequestException extends RuntimeException {

    public SolaceRequestException(String message) {
        super(message);
    }

    public SolaceRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
