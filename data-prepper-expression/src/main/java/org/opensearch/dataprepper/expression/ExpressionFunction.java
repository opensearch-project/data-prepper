/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;
import java.util.function.Function;

interface ExpressionFunction {
    /**
     * @return function name
     * @since 2.3
     */
    String getFunctionName();

    /**
     * evaluates the function and returns the result
     * @param args list of arguments to the function
     * @return the result of function evaluation
     * @since 2.3
     */
    Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType);
}
