package org.opensearch.dataprepper.plugins.kinesis.source.exceptions;

public class KinesisConsumerNotFoundException extends RuntimeException {
    public KinesisConsumerNotFoundException(final String errorMessage) {
        super(errorMessage);
    }
}
