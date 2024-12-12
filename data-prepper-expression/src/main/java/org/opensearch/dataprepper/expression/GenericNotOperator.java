package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

class GenericNotOperator implements Operator<Boolean> {
    private final int symbol;
    private final int shouldEvaluateRuleIndex;
    protected final String displayName;
    private final Operator<Boolean> innerOperator;

    public GenericNotOperator(final int symbol, final int shouldEvaluateRuleIndex, final Operator<Boolean> innerOperator) {
        this.symbol = symbol;
        this.shouldEvaluateRuleIndex = shouldEvaluateRuleIndex;
        displayName = DataPrepperExpressionParser.VOCABULARY.getDisplayName(symbol);
        this.innerOperator = innerOperator;
    }

    @Override
    public boolean isBooleanOperator() {
        return true;
    }

    @Override
    public int getNumberOfOperands(final RuleContext ctx) {
        return innerOperator.getNumberOfOperands(ctx);
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
    public Boolean evaluate(final Object ... args) {
        return !innerOperator.evaluate(args);
    }
}
