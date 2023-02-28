/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

public class InvalidS3URIException extends RuntimeException {

    public InvalidS3URIException(final String errorMessage) {
        super(errorMessage);
    }
}
