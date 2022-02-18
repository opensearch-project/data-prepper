/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.CoreMatchers.not;

public class ParseRuleContextExceptionMatcher extends TypeSafeMatcher<ParserRuleContext> {

    /**
     * Creates a matcher to assert errors are present in a ParserRuleContext node
     * @return Matcher
     */
    public static Matcher<ParserRuleContext> isNotValid() {
        return not(new ParseRuleContextExceptionMatcher());
    }

    private Exception exception;

    @Override
    protected boolean matchesSafely(final ParserRuleContext item) {
        exception = item.exception;
        return exception == null;
    }

    @Override
    public void describeTo(final Description description) {
        if (exception == null) {
            description.appendText("is valid");
        }
        else {
            description.appendText("invalid, exception found: " + exception);
            if (exception instanceof InputMismatchException) {
                final InputMismatchException mismatch = (InputMismatchException)exception;
                RuleContext ctx = mismatch.getCtx();

                if (mismatch.getOffendingToken() != null) {
                    description.appendText("\noffending token type = " + mismatch.getOffendingToken().getType());
                }

                while (ctx != null) {
                    description.appendText("\nin rule type " + ctx.getClass());
                    ctx = ctx.getParent();
                }
            }
        }
    }
}
