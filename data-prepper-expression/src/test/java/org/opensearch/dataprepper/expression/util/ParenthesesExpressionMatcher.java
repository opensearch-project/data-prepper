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

import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

/**
 * @since 1.3
 * <p>Matcher that asserts a unary tree ending in a Parentheses Expression Context</p>
 * <p>
 *     <b>Valid tree order</b><br>
 *     <pre>
 *     ExpressionContext<br>
 *     ├─ ConditionalExpression<br>
 *        ├─ EqualityOperatorExpression<br>
 *           ├─ RegexOperatorExpressionContext<br>
 *              ├─ RelationalOperatorExpressionContext<br>
 *                 ├─ SetOperatorExpressionContext<br>
 *                    ├─ UnaryOperatorExpressionContext<br>
 *                       ├─ ParenthesesExpressionContext<br>
 *                          ├─ TerminalNode<br>
 *                          ├─ <i>&lt;childMatcher&gt;</i><br>
 *                          ├─ TerminalNode<br>
 *     </pre>
 *     Note, a valid ParseTree may start at any level within the valid tree order
 * </p>
 */
public class ParenthesesExpressionMatcher extends SimpleExpressionMatcher {
    private static final Matcher<Integer> THREE_CHILDREN_MATCHER = is(3);

    private static final RuleClassOrderedList VALID_PARENTHESES_RULE_ORDER = new RuleClassOrderedList(
            DataPrepperExpressionParser.ExpressionContext.class,
            DataPrepperExpressionParser.ConditionalExpressionContext.class,
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class,
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class,
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class,
            DataPrepperExpressionParser.SetOperatorExpressionContext.class,
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class,
            DataPrepperExpressionParser.ParenthesesExpressionContext.class
    );

    /**
     * creates a matcher that asserts starting from a given ParseTree down only one child is present and each child is in a valid order
     * until a {@link DataPrepperExpressionParser.ParenthesesExpressionContext} node is found. Then asserts the node has 3 children. Outer
     * children must be terminal nodes and <b>childMatcher</b> matches middle child.
     * @param childMatcher matcher for ParenthesesExpressionContext middle child node
     * @return DiagnosingMatcher
     */
    public static DiagnosingMatcher<ParseTree> isParenthesesExpression(final DiagnosingMatcher<? extends ParseTree> childMatcher) {
        return new ParenthesesExpressionMatcher(VALID_PARENTHESES_RULE_ORDER, childMatcher);
    }

    private final DiagnosingMatcher<? extends ParseTree> childrenMatcher;

    protected ParenthesesExpressionMatcher(
            final RuleClassOrderedList validRuleOrder,
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
        if (!THREE_CHILDREN_MATCHER.matches(item.getChildCount())) {
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
