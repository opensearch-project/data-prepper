/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.exceptions;

public class RequestCancelledException extends RuntimeException {
    public RequestCancelledException(final String message) {
        super(message);
    }
}
