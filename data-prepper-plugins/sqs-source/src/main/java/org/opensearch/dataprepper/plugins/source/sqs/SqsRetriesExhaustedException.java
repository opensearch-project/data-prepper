/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

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