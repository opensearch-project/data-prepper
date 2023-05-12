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
public class LengthExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "length";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() != 1) {
            throw new RuntimeException("length() takes only one argument");
        }
        Object arg = args.get(0);
        if (!(arg instanceof String)) {
            throw new RuntimeException("length() takes only String type arguments");
        }
        String argStr = ((String)arg).trim();
        if (argStr.length() == 0) {
            return 0;
        }
        if (argStr.charAt(0) == '\"') {
            throw new RuntimeException("Literal strings not supported as arguments to length()");
        } else {
            // argStr must be JsonPointer
            final Object value = event.get(argStr, Object.class);
            if (value == null) {
                return null;
            } 
            
            if (!(value instanceof String)) {
                throw new RuntimeException(argStr + " is not String type");
            }
            return Integer.valueOf(((String)value).length());
        }
    }
}

