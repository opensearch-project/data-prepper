/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions;

public class SearchContextLimitException extends RuntimeException {
    public SearchContextLimitException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public SearchContextLimitException(final String message) { super (message); }
}
