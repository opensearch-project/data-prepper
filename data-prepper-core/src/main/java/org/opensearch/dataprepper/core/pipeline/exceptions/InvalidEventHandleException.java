package org.opensearch.dataprepper.core.pipeline.exceptions;

public class InvalidEventHandleException extends RuntimeException {
    public InvalidEventHandleException(final String message) {
        super(message);
    }
}
