/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.exceptions;

public class BufferWriteException extends RuntimeException {
    public BufferWriteException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
