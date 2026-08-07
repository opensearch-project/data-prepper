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
import java.util.List;
import java.util.function.Function;

@Named
public class ContainsExpressionFunction implements ExpressionFunction {
    private static final int NUMBER_OF_ARGS = 2;

    public String getFunctionName() {
        return "contains";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() != NUMBER_OF_ARGS) {
            throw new RuntimeException("contains() takes exactly two arguments");
        }
        String[] strArgs = new String[NUMBER_OF_ARGS];
        for (int i = 0; i < NUMBER_OF_ARGS; i++) {
            Object arg = args.get(i);
            if (arg instanceof EventKey) {
                EventKey eventKey = (EventKey) arg;
                Object obj = event.get(eventKey, Object.class);
                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof String)) {
                    throw new RuntimeException(String.format("containsSubstring() takes only string type arguments. \"%s\" is not of type string", obj));
                }
                strArgs[i] = (String) obj;
            } else if (arg instanceof String) {
                strArgs[i] = (String) arg;
            } else {
                throw new RuntimeException("Unexpected argument type: " + arg.getClass());
            }
        }
        return strArgs[0].contains(strArgs[1]);
    }
}
