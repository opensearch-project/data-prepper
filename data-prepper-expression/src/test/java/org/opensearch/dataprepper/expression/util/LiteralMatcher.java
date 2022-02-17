/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class LiteralMatcher extends DiagnosingMatcher<ParseTree> {
    private final Matcher<Integer> childCountMatcher = is(1);
    private final Matcher<ParseTree> literalMatcher = isLiteral();

    private static final List<Class<? extends ParseTree>> VALID_LITERAL_RULE_ORDER = Arrays.asList(
            DataPrepperExpressionParser.ExpressionContext.class,
            DataPrepperExpressionParser.ConditionalExpressionContext.class,
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class,
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class,
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class,
            DataPrepperExpressionParser.SetOperatorExpressionContext.class,
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class,
            DataPrepperExpressionParser.PrimaryContext.class,
            DataPrepperExpressionParser.LiteralContext.class
    );

    public static DiagnosingMatcher<ParseTree> isLiteral() {
        return hasContext(DataPrepperExpressionParser.LiteralContext.class, isTerminalNode());
    }

    public static DiagnosingMatcher<ParseTree> isUnaryTree() {
        return new LiteralMatcher();
    }

    private boolean isValidRuleOrder(final ParseTree current, final ParseTree next) {
        final int index = VALID_LITERAL_RULE_ORDER.indexOf(current.getClass());
        if (index < 0 || index >= VALID_LITERAL_RULE_ORDER.size() - 1) {
            return false;
        }
        else {
            return VALID_LITERAL_RULE_ORDER.get(index + 1).isInstance(next);
        }
    }

    private boolean matchesParseTree(final ParseTree item, final Description mismatchDescription) {
        if (item instanceof DataPrepperExpressionParser.LiteralContext) {
            return literalMatcher.matches(item);
        }
        else if (childCountMatcher.matches(item.getChildCount())) {
            final ParseTree child = item.getChild(0);
            return isValidRuleOrder(item, child) && matchesParseTree(child, mismatchDescription);
        }
        else {
            mismatchDescription.appendText("Unexpected terminal node " + item.getText());
            if (item.getParent() != null) {
                mismatchDescription.appendText(", child of parent node " + item.getParent().getText());
            }
            return false;
        }
    }

    @Override
    protected boolean matches(final Object item, final Description mismatchDescription) {
        if (item instanceof ParseTree) {
            return matchesParseTree((ParseTree) item, mismatchDescription);
        }
        else {
            return false;
        }
    }

    @Override
    public void describeTo(final Description description) {

    }
}
