package org.opensearch.dataprepper.plugins.source.s3.exception;

/**
 * This exception is thrown when SQS retries are exhausted
 *
 * @since 2.1
 */
public class SqsRetriesExhaustedException extends RuntimeException {

    public SqsRetriesExhaustedException(final String errorMessage) {
        super(errorMessage);
    }
}
