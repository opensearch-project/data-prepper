/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.exceptions;

public class BadRequestException extends RuntimeException {
    public BadRequestException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
