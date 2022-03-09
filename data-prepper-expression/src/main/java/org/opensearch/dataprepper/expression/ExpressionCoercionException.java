/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

/**
 * @since 1.3
 * Exception thrown by {@link ParseTreeCoercionService} methods to indicate type coercion failure.
 */
public class ExpressionCoercionException extends Exception {
    public ExpressionCoercionException(final String message) {
        super(message);
    }
}
