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
public class GetMetadataExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "getMetadata";
    }

    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (args.size() != 1) {
            throw new RuntimeException("getMetadata() takes only one argument");
        }
        Object arg = args.get(0);
        if (!(arg instanceof String)) {
            throw new RuntimeException("getMetadata() takes only String type arguments");
        }
        String argStr = ((String)arg).trim();
        if (argStr.length() == 0) {
            return null;
        }
        if (argStr.charAt(0) != '\"' || argStr.length() < 2) {
            throw new RuntimeException("Literal string expected as argument to getMetadata()");
        } 
        argStr = argStr.substring(1, argStr.length()-1).trim();
        if (argStr.isEmpty()) {
            return null;
        }
        Object value = event.getMetadata().getAttribute(argStr);
        if (value == null) {
            return null;
        }
        return convertLiteralType.apply(value);
    }
}


