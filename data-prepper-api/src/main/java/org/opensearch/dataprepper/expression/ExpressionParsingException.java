/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

/**
 * @since 2.11
 * Wrapper exception for any exception thrown while evaluating a statement
 */
public class ExpressionParsingException extends ExpressionEvaluationException {
    public ExpressionParsingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

