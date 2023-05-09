/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import javax.inject.Named;
import javax.inject.Inject;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

@Named
public class ExpressionFunctionProvider {
    private Map<String, ExpressionFunction> expressionFunctionsMap;

    @Inject
    public ExpressionFunctionProvider(final List<ExpressionFunction> expressionFunctions) {
        expressionFunctionsMap = expressionFunctions.stream().collect(Collectors.toMap(e -> e.getFunctionName(), e -> e));
    }

    public Object provideFunction(final String functionName, final List<Object> argList) {
        if (!expressionFunctionsMap.containsKey(functionName)) {
            return null;
        }
        return expressionFunctionsMap.get(functionName).evaluate(argList);
    }
    
}
