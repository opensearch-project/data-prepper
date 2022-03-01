/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import javax.inject.Named;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkArgument;

@Named
class RegexNotEqualOperator implements Operator<Boolean> {
    @Override
    public Integer getSymbol() {
        return DataPrepperExpressionParser.NOT_MATCH_REGEX_PATTERN;
    }

    @Override
    public Boolean evaluate(final Object... args) {
        checkArgument(args.length == 2, "Operands length needs to be 2.");
        checkArgument(args[0] instanceof String, "Left operand needs to be String.");
        checkArgument(args[1] instanceof String, "Right Operand needs to be String.");
        try {
            return !((String) args[0]).matches((String) args[1]);
        } catch (final PatternSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
