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
import java.util.UUID;
import java.util.function.Function;

/**
 * Expression function that generates a random UUID (version 4) string.
 * Usage: {@code generateUuid()}
 */
@Named
public class GenerateUuidExpressionFunction implements ExpressionFunction {

    static final String FUNCTION_NAME = "generateUuid";

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    @Override
    public Object evaluate(final List<Object> args, final Event event, final Function<Object, Object> convertLiteralType) {
        if (!args.isEmpty()) {
            throw new RuntimeException(FUNCTION_NAME + "() does not take any arguments");
        }
        return UUID.randomUUID().toString();
    }
}
