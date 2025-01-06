/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

/**
 * @since 1.3
 * ExpressionEvaluator interface to abstract the parse and evaluate implementations.
 */
public interface ExpressionEvaluator {

    /**
     * @since 1.3
     * Parse and evaluate the statement string, resolving external references with the provided context. Return type is
     *  ambiguous until statement evaluation is complete
     *
     * @param statement string to be parsed and evaluated
     * @param context event used to resolve external references in the statement
     * @return result of statement evaluation
     */
    Object evaluate(final String statement, final Event context);

    default Boolean evaluateConditional(final String statement, final Event context) {
        Object result;
        try {
            result = evaluate(statement, context);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            throw new ClassCastException("Unexpected expression return value of " + result);
        } catch (ExpressionParsingException e) {
            throw e;
        } catch (ExpressionEvaluationException e) {
            return false;
        }
    }

    Boolean isValidExpressionStatement(final String statement);

    Boolean isValidFormatExpression(final String format);

    List<String> extractDynamicKeysFromFormatExpression(final String format);

    List<String> extractDynamicExpressionsFromFormatExpression(final String format);
}
