/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import javax.inject.Named;
import java.util.function.Function;
import java.util.List;

@Named
public class GetEventTypeExpressionFunction implements ExpressionFunction {

    @Override
    public String getFunctionName() {
        return "getEventType";
    }

    @Override
    public Object evaluate(final List<Object> args, Event event, Function<Object, Object> convertLiteralType) {
        if (!args.isEmpty()) {
            throw new RuntimeException("getEventType() does not take any arguments");
        }
        return event.getMetadata().getEventType();
    }
}