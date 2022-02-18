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

import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class LiteralMatcher extends SimpleExpressionMatcher {
    private final Matcher<ParseTree> literalMatcher = hasContext(DataPrepperExpressionParser.LiteralContext.class, isTerminalNode());

    //region valid rule order
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
    //endregion

    protected LiteralMatcher(final List<Class<? extends ParseTree>> validRuleOrder) {
        super(validRuleOrder);
    }

    /**
     * Creates a matcher to check if a nodes is a unary tree with all nodes in a valid order that ends in a terminal node
     * @return DiagnosingMatcher
     *
     * @see TerminalNodeMatcher#isTerminalNode()
     */
    public static DiagnosingMatcher<ParseTree> isUnaryTree() {
        return new LiteralMatcher(VALID_LITERAL_RULE_ORDER);
    }

    protected boolean baseCase(final ParseTree item, final Description mismatchDescription) {
        if (literalMatcher.matches(item)) {
            return true;
        }
        else {
            literalMatcher.describeTo(mismatchDescription);
            mismatchDescription.appendText("\n\t\texpected LiteralContext but found " + item);
            return false;
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Expected LiteralContext");
    }
}
