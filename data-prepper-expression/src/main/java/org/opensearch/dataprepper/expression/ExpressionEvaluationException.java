package org.opensearch.dataprepper.expression;

/**
 * @since 1.3
 * Wrapper exception for any exception thrown while evaluating a statement
 */
public class ExpressionEvaluationException extends Exception {
    public ExpressionEvaluationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
