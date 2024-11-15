package org.opensearch.dataprepper.plugins.lambda.common.exception;

public class S3RetriesExhaustedException extends RuntimeException {

    public S3RetriesExhaustedException(final String errorMessage) {
        super(errorMessage);
    }
}
