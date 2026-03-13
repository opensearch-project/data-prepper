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
public class LengthExpressionFunction implements ExpressionFunction {
    public String getFunctionName() {
        return "length";
    }

    public Object evaluate(final List<Object> args, final Event event, final Function<Object, Object> convertLiteralType) {
        if (args.size() != 1) {
            throw new RuntimeException("length() takes only one argument");
        }
        final Object arg = args.get(0);
        if (arg instanceof String) {
            return getLength((String) arg);
        } else if (arg instanceof EventKey) {
            final EventKey eventKey = (EventKey) arg;
            final Object value = event.get(eventKey, Object.class);
            if (value == null) {
                return null;
            }
            if (!(value instanceof String)) {
                throw new RuntimeException(eventKey.getKey() + " is not String type");
            }
            return getLength((String) value);
        } else {
            throw new RuntimeException("Unexpected argument type: " + arg.getClass());
        }
    }

    private static Integer getLength(final String value) {
        return value.length();
    }
}
