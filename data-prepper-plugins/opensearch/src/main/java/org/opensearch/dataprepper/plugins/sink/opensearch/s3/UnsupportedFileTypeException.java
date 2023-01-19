/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

public class UnsupportedFileTypeException extends RuntimeException {

    public UnsupportedFileTypeException(final String errorMessage) {
        super(errorMessage);
    }
}
