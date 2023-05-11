/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import javax.inject.Named;
import javax.inject.Inject;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import java.util.function.Function;

@Named
public class ExpressionFunctionProvider {
    private Map<String, ExpressionFunction> expressionFunctionsMap;

    @Inject
    public ExpressionFunctionProvider(final List<ExpressionFunction> expressionFunctions) {
        expressionFunctionsMap = expressionFunctions.stream().collect(Collectors.toMap(e -> e.getFunctionName(), e -> e));
    }

    public Object provideFunction(final String functionName, final List<Object> argList, Event event, Function<Object, Object> convertLiteralType) {
        if (!expressionFunctionsMap.containsKey(functionName)) {
            throw new RuntimeException("Unknown function in the expression");
        }
        return expressionFunctionsMap.get(functionName).evaluate(argList, event, convertLiteralType);
    }
    
}
