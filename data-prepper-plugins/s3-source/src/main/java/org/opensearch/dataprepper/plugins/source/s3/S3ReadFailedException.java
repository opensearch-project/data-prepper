/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.s3;

/**
 * Exception thrown when there is a failure reading from S3 objects.
 * This is used to distinguish actual S3 read failures from other processing failures.
 */
public class S3ReadFailedException extends RuntimeException {
    public S3ReadFailedException(final Throwable cause) {
        super(cause);
    }
}
