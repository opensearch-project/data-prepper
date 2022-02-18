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
import static org.opensearch.dataprepper.expression.util.ContextMatcher.describeContextTo;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class JsonPointerMatcher extends SimpleExpressionMatcher {
    private static final Matcher<Integer> childCountMatcher = is(1);
    private static final Matcher<ParseTree> terminalNodeMatcher = isTerminalNode();

    /**
     * Creates a matcher to check if a nodes is a unary tree with all nodes in a valid order that ends in a json pointer
     * @return DiagnosingMatcher
     *
     * @see TerminalNodeMatcher#isTerminalNode()
     */
    public static DiagnosingMatcher<ParseTree> isJsonPointerUnaryTree() {
        return new JsonPointerMatcher(VALID_JSON_POINTER_RULE_ORDER);
    }

    //region valid rule order
    private static final List<Class<? extends ParseTree>> VALID_JSON_POINTER_RULE_ORDER = Arrays.asList(
            DataPrepperExpressionParser.ExpressionContext.class,
            DataPrepperExpressionParser.ConditionalExpressionContext.class,
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class,
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class,
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class,
            DataPrepperExpressionParser.SetOperatorExpressionContext.class,
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class,
            DataPrepperExpressionParser.PrimaryContext.class,
            DataPrepperExpressionParser.JsonPointerContext.class
    );
    //endregion

    private JsonPointerMatcher(final List<Class<? extends ParseTree>> validRuleOrder) {
        super(validRuleOrder);
    }

    @Override
    protected boolean baseCase(final ParseTree item, final Description mismatchDescription) {
        if (!childCountMatcher.matches(item.getChildCount())) {
            mismatchDescription.appendText("\n\t\t expected " + item.getText() + " to have 1 child node");
            describeContextTo(item, mismatchDescription);
            return false;
        }
        else if (!terminalNodeMatcher.matches(item.getChild(0))) {
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
        description.appendText("Expected JsonPointerContext");
    }
}
