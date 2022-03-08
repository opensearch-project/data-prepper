/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

/**
 * @since 1.3
 * Exception thrown by {@link ParseTreeCoercionService} methods.
 */
public class ParseTreeCoercionException extends Exception {
    public ParseTreeCoercionException(final String message) {
        super(message);
    }
}
