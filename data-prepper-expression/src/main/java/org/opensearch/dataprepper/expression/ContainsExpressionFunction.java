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
    public String getFunctionName() {
        return "contains";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() != 2) {
            throw new RuntimeException("contains() takes exactly two arguments");
        }
        String[] strArgs = new String[2];
        for (int i = 0; i < 2; i++) {
            Object arg = args.get(i);
            if (!(arg instanceof String)) {
                throw new RuntimeException("contains() takes only string type arguments");
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
                    throw new RuntimeException("contains() takes only string type arguments");
                }
                strArgs[i] = (String)obj;
            } else {
                throw new RuntimeException("Arguments to contains() must be a literal string or a Json Pointer");
            }
        }
        return strArgs[0].contains(strArgs[1]);
    }
}



