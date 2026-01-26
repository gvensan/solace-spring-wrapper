package com.solace.wrapper.exception;

/**
 * Custom exception for Solace connection related errors.
 */
public class SolaceConnectionException extends RuntimeException {

    public SolaceConnectionException(String message) {
        super(message);
    }

    public SolaceConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
