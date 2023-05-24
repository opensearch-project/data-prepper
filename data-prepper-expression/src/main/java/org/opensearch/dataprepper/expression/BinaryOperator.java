package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static com.google.common.base.Preconditions.checkArgument;

abstract class BinaryOperator<T> implements Operator<T> {
    private final int symbol;
    private final int shouldEvaluateRuleIndex;
    protected final String displayName;

    protected BinaryOperator(final int symbol, final int shouldEvaluateRuleIndex) {
        this.symbol = symbol;
        this.shouldEvaluateRuleIndex = shouldEvaluateRuleIndex;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
    }

    @Override
    public boolean shouldEvaluate(final RuleContext ctx) {
        return ctx.getRuleIndex() == shouldEvaluateRuleIndex;
    }

    @Override
    public int getSymbol() {
        return symbol;
    }


    @Override
    public T evaluate(final Object ... args) {
        checkArgument(args.length == getNumberOfOperands(null), displayName + " requires operands length to be "+ getNumberOfOperands(null)+".");
        return checkedEvaluate(args[0], args[1]);
    }

    abstract T checkedEvaluate(final Object lhs, final Object rhs);
}
