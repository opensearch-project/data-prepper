/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

public class S3ObjectTooLargeException extends RuntimeException {

    public S3ObjectTooLargeException(final String errorMessage) {
        super(errorMessage);
    }
}
