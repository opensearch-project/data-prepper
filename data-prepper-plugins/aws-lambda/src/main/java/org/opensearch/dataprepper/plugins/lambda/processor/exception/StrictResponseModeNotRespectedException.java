package org.opensearch.dataprepper.plugins.lambda.processor.exception;

public class StrictResponseModeNotRespectedException extends RuntimeException {
    public StrictResponseModeNotRespectedException(final String message) {
        super(message);
    }
}
