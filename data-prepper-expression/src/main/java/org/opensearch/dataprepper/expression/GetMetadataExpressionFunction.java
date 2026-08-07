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
            throw new RuntimeException("Unexpected argument type: " + arg.getClass() + ". getMetadata() takes only String type arguments");
        }
        String argStr = ((String)arg).trim();
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


