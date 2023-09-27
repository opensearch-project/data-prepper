package org.opensearch.dataprepper.plugins.source.s3.exception;

public class S3RetriesExhaustedException extends RuntimeException {

    public S3RetriesExhaustedException(final String errorMessage) {
        super(errorMessage);
    }
}
