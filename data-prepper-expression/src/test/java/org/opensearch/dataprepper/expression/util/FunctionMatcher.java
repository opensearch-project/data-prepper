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

import static org.opensearch.dataprepper.expression.util.ContextMatcher.describeContextTo;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class FunctionMatcher extends SimpleExpressionMatcher {
    private static final Matcher<ParseTree> TERMINAL_NODE_MATCHER = isTerminalNode();

    /**
     * Creates a matcher to check if a nodes is a unary tree with all nodes in a valid order that ends in a json pointer
     * @return DiagnosingMatcher
     *
     * @see TerminalNodeMatcher#isTerminalNode()
     */
    public static DiagnosingMatcher<ParseTree> isFunctionUnaryTree() {
        return new FunctionMatcher(VALID_JSON_POINTER_RULE_ORDER);
    }

    //region valid rule order
    private static final RuleClassOrderedList VALID_JSON_POINTER_RULE_ORDER = new RuleClassOrderedList(
            DataPrepperExpressionParser.ExpressionContext.class,
            DataPrepperExpressionParser.ConditionalExpressionContext.class,
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class,
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class,
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class,
            DataPrepperExpressionParser.SetOperatorExpressionContext.class,
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class,
            DataPrepperExpressionParser.PrimaryContext.class,
            DataPrepperExpressionParser.FunctionContext.class
    );
    //endregion

    private FunctionMatcher(final RuleClassOrderedList validRuleOrder) {
        super(validRuleOrder);
    }

    @Override
    protected boolean baseCase(final ParseTree item, final Description mismatchDescription) {
        if (!SINGLE_CHILD_MATCHER.matches(item.getChildCount())) {
            mismatchDescription.appendText("\n\t\t expected " + item.getText() + " to have 1 child node");
            describeContextTo(item, mismatchDescription);
            return false;
        }
        else if (!TERMINAL_NODE_MATCHER.matches(item.getChild(0))) {
            mismatchDescription.appendText("\n\t\t expected " + item.getText() + " child to be of type TerminalNode");
            describeContextTo(item, mismatchDescription);
            return false;
        }
        else {
            return true;
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Expected FunctionContext");
    }
}
