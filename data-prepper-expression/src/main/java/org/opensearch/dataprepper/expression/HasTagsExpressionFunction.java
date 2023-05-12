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
        if(!args.stream().allMatch(a -> ((a instanceof String) && (((String)a).length() > 0) && (((String)a).charAt(0) == '"')))) {
            throw new RuntimeException("hasTags() takes only non-empty string literal type arguments");
        }
        final List<String> tags = args.stream()
                                    .map(a -> {
                                        String str = (String) a;
                                        return str.substring(1, str.length()-1);
                                     })
                                    .collect(Collectors.toList());
        return event.getMetadata().hasTags(tags);
    }
}


