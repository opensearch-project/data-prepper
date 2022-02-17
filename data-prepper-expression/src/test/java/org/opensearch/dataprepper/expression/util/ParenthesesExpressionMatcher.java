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

public class ParenthesesExpressionMatcher extends SimpleExpressionMatcher {
    private static final Matcher<Integer> childCountMatcher = is(3);

    private static final List<Class<? extends ParseTree>> VALID_PARENTHESES_RULE_ORDER = Arrays.asList(
            DataPrepperExpressionParser.ExpressionContext.class,
            DataPrepperExpressionParser.ConditionalExpressionContext.class,
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class,
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class,
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class,
            DataPrepperExpressionParser.SetOperatorExpressionContext.class,
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class,
            DataPrepperExpressionParser.ParenthesesExpressionContext.class
    );

    public static DiagnosingMatcher<ParseTree> isParenthesesExpression(final DiagnosingMatcher<? extends ParseTree> childMatcher) {
        return new ParenthesesExpressionMatcher(VALID_PARENTHESES_RULE_ORDER, childMatcher);
    }

    private final DiagnosingMatcher<? extends ParseTree> childrenMatcher;

    protected ParenthesesExpressionMatcher(
            final List<Class<? extends ParseTree>> validRuleOrder,
            final DiagnosingMatcher<? extends ParseTree> childMatcher
    ) {
        super(validRuleOrder);
        this.childrenMatcher = hasContext(
                DataPrepperExpressionParser.ParenthesesExpressionContext.class,
                isTerminalNode(),
                childMatcher,
                isTerminalNode()
        );
    }

    @Override
    protected boolean baseCase(final ParseTree item, final Description mismatchDescription) {
        if (!childCountMatcher.matches(item.getChildCount())) {
            mismatchDescription.appendText("\n\t\t expected " + item.getText() + " to have 1 child node");
            return false;
        }
        else if (!childrenMatcher.matches(item)) {
            childrenMatcher.describeTo(mismatchDescription);
            return false;
        }
        else {
            return true;
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Expected ParenthesesExpressionContext");
    }
}
