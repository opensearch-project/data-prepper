/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;
import javax.inject.Named;
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
            if (!(arg instanceof String)) {
                throw new RuntimeException(String.format("containsSubstring() takes only string type arguments. \"%s\" is not of type string", arg));
            }
            String str = (String) arg;
            if (str.charAt(0) == '"') {
                strArgs[i] = str.substring(1, str.length()-1);
            } else if (str.charAt(0) == '/') {
                Object obj = event.get(str, Object.class);
                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof String)) {
                    throw new RuntimeException(String.format("containsSubstring() takes only string type arguments. \"%s\" is not of type string", obj));
                }
                strArgs[i] = (String)obj;
            } else {
                throw new RuntimeException(String.format("Arguments to contains() must be a literal string or a Json Pointer. \"%s\" is not string literal or json pointer", str));
            }
        }
        return strArgs[0].contains(strArgs[1]);
    }
}



