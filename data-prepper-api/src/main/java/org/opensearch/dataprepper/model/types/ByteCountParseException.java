/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.types;

public class ByteCountParseException extends RuntimeException {
    public ByteCountParseException(final String message) {
        super(message);
    }
}
