/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

public class ExpressionArgumentsException extends ExpressionEvaluationException{
    
    public ExpressionArgumentsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ExpressionArgumentsException(final String message) {
        super(message, null);
    }
}