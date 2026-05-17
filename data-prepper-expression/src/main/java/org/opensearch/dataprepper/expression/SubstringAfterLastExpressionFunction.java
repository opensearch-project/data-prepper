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
public class SubstringAfterLastExpressionFunction extends AbstractSubstringExpressionFunction {
    static final String FUNCTION_NAME = "substringAfterLast";

    @Override
    public String getFunctionName() {
        return FUNCTION_NAME;
    }

    @Override
    protected String extractSubstring(final String source, final String delimiter) {
        final int index = source.lastIndexOf(delimiter);
        if (index == -1) {
            return source;
        }
        return source.substring(index + delimiter.length());
    }
}
