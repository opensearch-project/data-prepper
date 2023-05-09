/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;
import javax.inject.Named;

@Named
public class LengthExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "length";
    }

    public Object evaluate(final List<Object> args, Event event) {
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
            if (argStr.charAt(argStr.length()-1) != '\"') {
                throw new RuntimeException("Invalid string passed to length() "+argStr);
            }
            return Integer.valueOf(argStr.length()-2);
        } else {
            return Integer.valueOf(argStr.length());
        }
    }
}

