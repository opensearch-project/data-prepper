package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.exception;

public class InvalidBufferTypeException extends RuntimeException {
    public InvalidBufferTypeException(String message) {
        super(message);
    }
}
