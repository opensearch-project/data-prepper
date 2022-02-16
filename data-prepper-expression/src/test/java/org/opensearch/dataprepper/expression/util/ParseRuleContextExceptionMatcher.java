/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ParserRuleContext;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.CoreMatchers.not;

public class ParseRuleContextExceptionMatcher extends TypeSafeMatcher<ParserRuleContext> {
    public static Matcher<ParserRuleContext> hasNoException() {
        return new ParseRuleContextExceptionMatcher();
    }

    public static Matcher<ParserRuleContext> hasException() {
        return not(new ParseRuleContextExceptionMatcher());
    }

    @Override
    protected boolean matchesSafely(final ParserRuleContext item) {
        return item.exception == null;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Has no exceptions");
    }
}
