/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class ParseRuleContextExceptionMatcher extends TypeSafeMatcher<ParserRuleContext> {

    /**
     * Creates a matcher to assert errors are present in a ParserRuleContext node
     * @return Matcher
     */
    public static Matcher<ParserRuleContext> isNotValid() {
        return new ParseRuleContextExceptionMatcher();
    }

    private RecognitionException exception;

    @Override
    protected boolean matchesSafely(final ParserRuleContext item) {
        exception = item.exception;
        return exception != null;
    }

    @Override
    public void describeTo(final Description description) {
        if (exception == null) {
            description.appendText("is valid");
        }
        else {
            description.appendText("invalid, exception found: " + exception);
            RuleContext ruleContext = exception.getCtx();

            if (exception.getOffendingToken() != null) {
                description.appendText("\noffending token type = " + exception.getOffendingToken().getType());
            }

            while (ruleContext != null) {
                description.appendText("\nin rule type " + ruleContext.getClass());
                ruleContext = ruleContext.getParent();
            }
        }
    }
}
