/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import javax.inject.Named;
import java.util.List;
import java.util.function.Function;

@Named
public class SubListExpressionFunction implements ExpressionFunction {
    private static final int NUMBER_OF_ARGS = 3;
    static final String SUB_LIST_FUNCTION_NAME = "subList";

    @Override
    public String getFunctionName() {
        return SUB_LIST_FUNCTION_NAME;
    }

    @Override
    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() != NUMBER_OF_ARGS) {
            throw new IllegalArgumentException("subList() takes exactly three arguments");
        }

        // Validate and extract JSON pointer argument
        Object arg0 = args.get(0);
        if (!(arg0 instanceof String)) {
            throw new IllegalArgumentException("subList() first argument must be a JSON pointer String");
        }
        String jsonPointer = ((String) arg0).trim();
        if (jsonPointer.isEmpty()) {
            throw new IllegalArgumentException("subList() JSON pointer cannot be empty");
        }
        if (jsonPointer.charAt(0) != '/') {
            throw new IllegalArgumentException("subList() expects the first argument to be a JSON pointer (starting with '/')");
        }

        // Retrieve the list from the event
        Object listObject = event.get(jsonPointer, Object.class);
        if (listObject == null) {
            return null;
        }
        if (!(listObject instanceof List)) {
            throw new IllegalArgumentException(jsonPointer + " is not a List type");
        }
        List<?> list = (List<?>) listObject;

        int startIndex;
        int endIndex;
        try {
            startIndex = Integer.parseInt(args.get(1).toString());
            endIndex = Integer.parseInt(args.get(2).toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("subList() start and end index arguments must be integers", e);
        }

        if (startIndex < 0 || startIndex > list.size()) {
            throw new IllegalArgumentException("subList() start index out of bounds");
        }
        if (endIndex == -1) {
            endIndex = list.size();
        }
        if (endIndex < startIndex || endIndex > list.size()) {
            throw new IllegalArgumentException("subList() end index out of bounds");
        }

        return list.subList(startIndex, endIndex);
    }
}