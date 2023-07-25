package org.opensearch.dataprepper.exceptions;

public class RequestCancelledException extends RuntimeException {
    public RequestCancelledException(final String message) {
        super(message);
    }
}
