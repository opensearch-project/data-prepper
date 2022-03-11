package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;

public class GenericNotOperator extends BinaryOperator<Boolean> {
    private final BinaryOperator<Boolean> innerOperator;

    public GenericNotOperator(
            final int symbol,
            final int shouldEvaluateRuleIndex,
            final BinaryOperator<Boolean> innerOperator
    ) {
        super(symbol, shouldEvaluateRuleIndex);
        this.innerOperator = innerOperator;
    }

    @Override
    public Boolean checkedEvaluate(final Object lhs, final Object rhs) {
        return !innerOperator.checkedEvaluate(lhs, rhs);
    }
}
