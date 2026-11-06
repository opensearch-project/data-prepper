/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudWatchLogsSinkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsSinkUtils.class);

    /**
     * Whether a configured value is a Data Prepper format expression carrying a {@code ${...}} field
     * reference or expression, as opposed to a literal. Dynamic values are resolved per request rather than
     * applied once at client build.
     */
    public static boolean isDynamicExpression(final String value, final ExpressionEvaluator expressionEvaluator) {
        if (value == null) {
            return false;
        }
        return !expressionEvaluator.extractDynamicKeysFromFormatExpression(value).isEmpty()
                || !expressionEvaluator.extractDynamicExpressionsFromFormatExpression(value).isEmpty();
    }
}
