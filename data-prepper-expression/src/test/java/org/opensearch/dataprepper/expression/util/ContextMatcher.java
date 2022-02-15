/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.core.IsEqual.equalTo;

public class ContextMatcher extends DiagnosingMatcher<ParseTree> {
    @SafeVarargs
    public static DiagnosingMatcher<ParseTree> hasContext(
            final Class<? extends ParseTree> parserRuleContextType,
            final DiagnosingMatcher<ParseTree>... childrenMatchers
    ) {
        return new ContextMatcher(parserRuleContextType, childrenMatchers);
    }

    private final DiagnosingMatcher<? extends ParseTree>[] childrenMatchers;
    final Matcher<? extends ParseTree> isParserRuleContextType;
    private final Matcher<Integer> listSizeMatcher;
    @Nullable
    private Matcher<?> failedAssertion;

    @SafeVarargs
    public ContextMatcher(
            final Class<? extends ParseTree> parserRuleContextType,
            final DiagnosingMatcher<? extends ParseTree> ... childrenMatchers
    ) {
        this.childrenMatchers = childrenMatchers;
        isParserRuleContextType = isA(parserRuleContextType);
        listSizeMatcher = equalTo(childrenMatchers.length);
    }

    private boolean matchChildren(final ParseTree ctx, final Description mismatch) {
        if (listSizeMatcher.matches(ctx.getChildCount())) {
            for (int i = 0; i < childrenMatchers.length; i++) {
                final ParseTree child = ctx.getChild(i);
                final DiagnosingMatcher<? extends ParseTree> matcher = childrenMatchers[i];

                if (!matcher.matches(child)) {
                    mismatch.appendDescriptionOf(matcher)
                            .appendText(" ");
                    matcher.describeMismatch(child, mismatch);
                    failedAssertion = matcher;
                    return false;
                }
            }

            return true;
        }
        else {
            mismatch.appendDescriptionOf(listSizeMatcher)
                    .appendText(" ");
            listSizeMatcher.describeMismatch(ctx.getChildCount(), mismatch);
            failedAssertion = listSizeMatcher;
            return false;
        }
    }

    public boolean matches(final Object item, final Description mismatch) {
        if (isParserRuleContextType.matches(item)) {
            final ParseTree ctx = (ParseTree) item;
            return matchChildren(ctx, mismatch);
        }
        else {
            mismatch.appendDescriptionOf(isParserRuleContextType)
                    .appendText("\n\t\tfinally ");
            isParserRuleContextType.describeMismatch(item, mismatch);
            failedAssertion = isParserRuleContextType;
            return false;
        }
    }

    @Override
    public void describeTo(final Description description) {
        if (failedAssertion != null)
            failedAssertion.describeTo(description);
    }
}
