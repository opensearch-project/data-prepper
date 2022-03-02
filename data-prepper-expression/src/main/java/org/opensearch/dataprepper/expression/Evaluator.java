/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

/**
 * @since 1.3
 * Placeholder interface for evaluators used by a {@link ExpressionEvaluator} implementation
 * @param <ParsedData> parsed data type
 * @param <Context> Context data type
 */
interface Evaluator<ParsedData, Context> {
    Object evaluate(final ParsedData parsedData, final Context context) throws ClassCastException;
}
