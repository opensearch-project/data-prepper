/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.types;

public class ByteCountInvalidInputException extends RuntimeException {
    public ByteCountInvalidInputException(final String message) {
        super(message);
    }
}
