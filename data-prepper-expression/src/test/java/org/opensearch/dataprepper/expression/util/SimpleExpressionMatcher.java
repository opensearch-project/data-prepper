/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.describeContextTo;

/**
 * Base class for Matcher that asserts a unary tree ending in a node that matches
 * {@link RuleClassOrderedList#isInstanceOfLast(ParseTree)}
 */
abstract class SimpleExpressionMatcher extends DiagnosingMatcher<ParseTree> {
    protected static final Matcher<Integer> SINGLE_CHILD_MATCHER = is(1);

    protected final RuleClassOrderedList validRuleOrder;

    protected SimpleExpressionMatcher(final RuleClassOrderedList validRuleOrder) {
        this.validRuleOrder = validRuleOrder;
    }

    /**
     * Called when {@link SimpleExpressionMatcher#matchesParseTree(ParseTree, Description)} finds a node matching
     * {@link RuleClassOrderedList#isInstanceOfLast(ParseTree)}
     *
     * @param item matching node
     * @param mismatch description to append failed assertion context.
     * @return if this matcher matches
     */
    protected abstract boolean baseCase(final ParseTree item, final Description mismatch);

    /**
     * Wrapper method for {@link SimpleExpressionMatcher#baseCase(ParseTree, Description)} to ensure a mismatch
     * description is added.
     *
     * @param item ParseTree to match
     * @param mismatch description of matcher
     * @return if item matches the expected value
     */
    private boolean matchesBaseCase(final ParseTree item, final Description mismatch) {
        if (baseCase(item, mismatch)) {
            return true;
        }
        else {
            describeContextTo(item, mismatch);
            return false;
        }
    }

    /**
     * <p>
     *     Checks if item has one child and that item's class and item's child's class are sequential in
     *     {@link SimpleExpressionMatcher#validRuleOrder}. Then recursively starts matching item's child.
     * </p>
     * <p>
     *     Valid Rule Order:<br>
     *     A -> B -> C
     * </p>
     * <p>
     *     True if <pre>item.getClass() == <? extends A> && item.getChild(0).getClass() == <? extends B></pre>
     * </p>
     *
     * @param item ParseTree to match
     * @param mismatch description of matcher
     * @return if item matches the expected value
     */
    private boolean matchesChildren(final ParseTree item, final Description mismatch) {
        if (SINGLE_CHILD_MATCHER.matches(item.getChildCount())) {
            final ParseTree child = item.getChild(0);

            if (validRuleOrder.isSequentialRules(item, child)) {
                return matchesParseTree(child, mismatch);
            }
            else {
                mismatch.appendText(item.getClass() + " -> " + child.getClass() + " not valid rule order");
                describeContextTo(item, mismatch);
                return false;
            }
        }
        else {
            SINGLE_CHILD_MATCHER.describeMismatch(item.getChildCount(), mismatch);
            describeContextTo(item, mismatch);
            return false;
        }
    }

    /**
     * Checks if item is the last rule in {@link SimpleExpressionMatcher#validRuleOrder}. If yes, trigger base case,
     * otherwise match the nodes children.
     *
     * @param item ParseTree to match
     * @param mismatch description of matcher
     * @return if item matches the expected value
     */
    private boolean matchesParseTree(final ParseTree item, final Description mismatch) {
        if (validRuleOrder.isInstanceOfLast(item)) {
            return matchesBaseCase(item, mismatch);
        }
        else {
            return matchesChildren(item, mismatch);
        }
    }

    /**
     * Verifies item is a ParseTree, then starts matching logic
     *
     * @see SimpleExpressionMatcher#matchesParseTree(ParseTree, Description)
     *
     * @param item parse tree to match against
     * @param mismatch description of matcher
     * @return if item matches the expected value
     */
    @Override
    protected boolean matches(final Object item, final Description mismatch) {
        if (item instanceof ParseTree) {
            return matchesParseTree((ParseTree) item, mismatch);
        }
        else {
            return false;
        }
    }
}
