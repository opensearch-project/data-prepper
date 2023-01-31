/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

public class OpenSearchSinkException extends RuntimeException {

    public OpenSearchSinkException(final String errorMessage) {
        super(errorMessage);
    }
}
