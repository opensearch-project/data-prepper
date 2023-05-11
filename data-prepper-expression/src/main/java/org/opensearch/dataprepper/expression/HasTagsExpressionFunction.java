/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import java.util.stream.Collectors;
import java.util.List;
import javax.inject.Named;
import java.util.function.Function;

@Named
public class HasTagsExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "hasTags";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() == 0) {
            throw new RuntimeException("hasTags() takes at least one argument");
        }
        if(!args.stream().allMatch(a -> a instanceof String)) {
            throw new RuntimeException("hasTags() takes only String type arguments");
        }
        final List<String> tags = args.stream()
                                    .map(a -> (String) a)
                                    .collect(Collectors.toList());
        return event.getMetadata().hasTags(tags);
    }
}


