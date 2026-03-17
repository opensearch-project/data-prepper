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

import java.util.List;
import java.util.function.Function;

abstract class AbstractSubstringExpressionFunction implements ExpressionFunction {
    private static final int NUMBER_OF_ARGS = 2;

    @Override
    public Object evaluate(final List<Object> args, final Event event, final Function<Object, Object> convertLiteralType) {
        if (args.size() != NUMBER_OF_ARGS) {
            throw new RuntimeException(getFunctionName() + "() takes exactly two arguments");
        }

        final String[] strArgs = new String[NUMBER_OF_ARGS];
        for (int i = 0; i < NUMBER_OF_ARGS; i++) {
            final Object arg = args.get(i);
            if (arg instanceof EventKey) {
                final Object obj = event.get((EventKey) arg, Object.class);
                if (obj == null) {
                    strArgs[i] = null;
                } else if (!(obj instanceof String)) {
                    throw new RuntimeException(String.format("%s() takes only string type arguments. \"%s\" is not of type string", getFunctionName(), obj));
                } else {
                    strArgs[i] = (String) obj;
                }
            } else if (arg instanceof String) {
                strArgs[i] = (String) arg;
            } else {
                throw new RuntimeException("Unexpected argument type: " + arg.getClass());
            }
        }

        final String source = strArgs[0];
        final String delimiter = strArgs[1];

        if (source == null) {
            return null;
        }
        if (delimiter == null || delimiter.isEmpty()) {
            return source;
        }
        return extractSubstring(source, delimiter);
    }

    protected abstract String extractSubstring(final String source, final String delimiter);
}
