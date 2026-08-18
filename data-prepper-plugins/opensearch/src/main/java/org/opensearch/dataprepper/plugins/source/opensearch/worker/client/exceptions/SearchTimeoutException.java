/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions;

public class SearchTimeoutException extends RuntimeException {
    public SearchTimeoutException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public SearchTimeoutException(final String message) {
        super(message);
    }
}
