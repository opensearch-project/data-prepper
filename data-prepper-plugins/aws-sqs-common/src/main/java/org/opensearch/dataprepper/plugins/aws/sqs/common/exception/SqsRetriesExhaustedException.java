/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common.exception;

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
