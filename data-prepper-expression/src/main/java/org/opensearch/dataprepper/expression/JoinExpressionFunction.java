/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;

import javax.inject.Named;
import java.util.HashMap;
import java.util.List;
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

        final String delimiter;
        final EventKey sourceKey;

        if (args.size() == 2) {
            final Object delimiterArg = args.get(0);
            final Object sourceKeyArg = args.get(1);

            if (delimiterArg instanceof String) {
                // remove slashes used to escape comma
                delimiter = ((String) delimiterArg).replace("\\\\,", ",");
            } else {
                throw new RuntimeException("Unexpected argument type for delimiter: " + delimiterArg.getClass() +
                        ". Expected String.");
            }

            if (sourceKeyArg instanceof EventKey) {
                sourceKey = (EventKey) sourceKeyArg;
            } else {
                throw new RuntimeException("Unexpected argument type for source key: " + sourceKeyArg.getClass() +
                        ". Expected EventKey.");
            }
        } else {
            delimiter = ",";
            final Object sourceKeyArg = args.get(0);
            if (sourceKeyArg instanceof EventKey) {
                sourceKey = (EventKey) sourceKeyArg;
            } else {
                throw new RuntimeException("Unexpected argument type for source key: " + sourceKeyArg.getClass() +
                        ". Expected EventKey.");
            }
        }

        try {
            if (event.isValueAList(sourceKey.getKey())) {
                final List<Object> sourceList = event.get(sourceKey, List.class);
                return joinList(sourceList, delimiter);
            } else {
                final Map<String, Object> sourceMap = event.get(sourceKey, Map.class);
                return joinListsInMap(sourceMap, delimiter);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Unable to perform join function on " + sourceKey.getKey(), ex);
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
