/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

/**
 * @since 1.3
 * Exception thrown by {@link ParseTreeCoercionService} methods to indicate type coercion failure.
 */
class ExpressionCoercionException extends RuntimeException {
    public ExpressionCoercionException(final String message) {
        super(message);
    }
}
