/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import javax.inject.Named;

@Named
public class SubstringBeforeExpressionFunction extends AbstractSubstringExpressionFunction {
    static final String FUNCTION_NAME = "substringBefore";

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    @Override
    protected String extractSubstring(final String source, final String delimiter) {
        final int index = source.indexOf(delimiter);
        if (index == -1) {
            return source;
        }
        return source.substring(0, index);
    }
}
