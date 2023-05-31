/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.exception;

/**
 * This exception is thrown when SQS retries are exhausted
 */
public class SqsRetriesExhaustedException extends RuntimeException {

    public SqsRetriesExhaustedException(final String errorMessage) {
        super(errorMessage);
    }
}
