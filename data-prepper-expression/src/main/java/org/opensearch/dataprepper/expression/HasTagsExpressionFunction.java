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

import javax.inject.Named;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named
public class HasTagsExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "hasTags";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.isEmpty()) {
            throw new RuntimeException("hasTags() takes at least one argument");
        }
        if (!args.stream().allMatch(a -> a instanceof String)) {
            throw new RuntimeException("Unexpected argument type. hasTags() takes only String type arguments");
        }
        final List<String> tags = args.stream()
                                    .map(a -> (String) a)
                                    .collect(Collectors.toList());
        if (tags.stream().anyMatch(String::isEmpty)) {
            throw new RuntimeException("Unexpected argument value. hasTags() requires non-empty strings.");
        }
        return event.getMetadata().hasTags(tags);
    }
}


