/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.describeContextTo;

public abstract class SimpleExpressionMatcher extends DiagnosingMatcher<ParseTree> {
    private static final Matcher<Integer> childCountMatcher = is(1);

    protected final List<Class<? extends ParseTree>> validRuleOrder;
    protected final Class<? extends ParseTree> lastNodeClass;

    protected SimpleExpressionMatcher(final List<Class<? extends ParseTree>> validRuleOrder) {
        this.validRuleOrder = validRuleOrder;
        this.lastNodeClass = validRuleOrder.get(validRuleOrder.size() - 1);
    }

    private int getRuleIndex(final ParseTree item) {
        for (int x = 0; x < validRuleOrder.size(); x++) {
            if (validRuleOrder.get(x).isInstance(item)) {
                return x;
            }
        }
        return -1;
    }

    private boolean isValidRuleOrder(final ParseTree current, final ParseTree next, final Description mismatchDescription) {
        final int index = getRuleIndex(current);
        if (index < 0 || index >= validRuleOrder.size() - 1) {
            mismatchDescription.appendText(current.getClass() + " is not a valid context ");
            return false;
        }
        else {
            if (validRuleOrder.get(index + 1).isInstance(next)) {
                return true;
            }
            else {
                mismatchDescription.appendText(current.getClass() + " -> " + next.getClass() + " not valid rule order");
                describeContextTo(current, mismatchDescription);
                return false;
            }
        }
    }

    protected abstract boolean baseCase(final ParseTree item, final Description mismatchDescription);

    private boolean matchesParseTree(final ParseTree item, final Description mismatch) {
        if (lastNodeClass.isInstance(item)) {
            if (baseCase(item, mismatch)) {
                return true;
            }
            else {
                describeContextTo(item, mismatch);
                return false;
            }
        }
        else if (childCountMatcher.matches(item.getChildCount())) {
            final ParseTree child = item.getChild(0);
            if (!isValidRuleOrder(item, child, mismatch)) {
                describeTo(mismatch);
                return false;
            }
            return isValidRuleOrder(item, child, mismatch) && matchesParseTree(child, mismatch);
        }
        else {
            mismatch.appendText("Unexpected terminal node " + item.getText());
            if (item.getParent() != null) {
                mismatch.appendText(", child of parent node " + item.getParent().getText());
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
}
