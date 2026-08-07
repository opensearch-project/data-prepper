/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

