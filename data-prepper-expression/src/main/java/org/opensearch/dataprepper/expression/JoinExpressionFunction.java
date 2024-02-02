/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;

import java.util.HashMap;
import java.util.List;
import javax.inject.Named;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named
public class JoinExpressionFunction implements  ExpressionFunction {

    private static final String FUNCTION_NAME = "join";

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    @Override
    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.isEmpty() || args.size() > 2) {
            throw new IllegalArgumentException(FUNCTION_NAME + "() takes one or two arguments");
        }

        final List<String> argStrings;
        try {
            argStrings = args.stream()
                    .map(arg -> ((String)arg).trim())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Arguments in " + FUNCTION_NAME + "() function should be of Json Pointer type or String type");
        }

        final String delimiter;
        if (argStrings.size() == 2) {
            final String trimmedDelimiter = argStrings.get(1).substring(1, argStrings.get(1).length() - 1);

            // remove slashes used to escape comma
            delimiter = trimmedDelimiter.replace("\\\\,", ",");
        } else {
            delimiter = ",";
        }

        final String sourceKey = argStrings.get(0);
        try {
            final List<Object> sourceList = event.get(sourceKey, List.class);
            return joinList(sourceList, delimiter);
        } catch (Exception e) {
            try {
                final Map<String, Object> sourceMap = event.get(sourceKey, Map.class);
                return joinListsInMap(sourceMap, delimiter);
            } catch (Exception ex) {
                throw new RuntimeException("Unable to perform join function on " + sourceKey, ex);
            }
        }
    }

    private String joinList(final List<Object> sourceList, final String delimiter) {
        final List<String> stringList = sourceList.stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        return String.join(delimiter, stringList);
    }

    private Map<String, Object> joinListsInMap(final Map<String, Object> sourceMap, final String delimiter) {
        final Map<String, Object> resultMap = new HashMap<>();
        for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
            try {
                final String joinedEntryValue = joinList((List<Object>)entry.getValue(), delimiter);
                resultMap.put(entry.getKey(), joinedEntryValue);
            } catch (Exception e) {
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }
        return resultMap;
    }
}
