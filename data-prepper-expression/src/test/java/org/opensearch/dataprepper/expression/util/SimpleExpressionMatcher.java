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

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public abstract class SimpleExpressionMatcher extends DiagnosingMatcher<ParseTree> {
    private static final Matcher<Integer> childCountMatcher = is(1);

    protected final List<Class<? extends ParseTree>> validRuleOrder;

    protected SimpleExpressionMatcher(final List<Class<? extends ParseTree>> validRuleOrder) {
        this.validRuleOrder = validRuleOrder;
    }

    private boolean isValidRuleOrder(final ParseTree current, final ParseTree next) {
        final int index = validRuleOrder.indexOf(current.getClass());
        if (index < 0 || index >= validRuleOrder.size() - 1) {
            return false;
        }
        else {
            return validRuleOrder.get(index + 1).isInstance(next);
        }
    }

    protected abstract boolean baseCase(final ParseTree item, final Description mismatchDescription);

    private boolean matchesParseTree(final ParseTree item, final Description mismatchDescription) {
        if (item instanceof DataPrepperExpressionParser.ParenthesesExpressionContext) {
            return baseCase(item, mismatchDescription);
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
