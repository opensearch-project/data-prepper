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
public class ToJsonStringExpressionFunction implements ExpressionFunction {
    public static final String FUNCTION_NAME = "toJsonString";

    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    public Object evaluate(final List<Object> arguments, final Event event, final Function<Object, Object> convertLiteralType) {
        if (arguments.size() != 0) {
            throw new RuntimeException(FUNCTION_NAME + " takes no arguments");
        }

        return event.toJsonString();
    }
}

