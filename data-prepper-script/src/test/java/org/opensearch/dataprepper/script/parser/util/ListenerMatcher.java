/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.not;

/**
 * @since 1.3
 * Hamcrest matcher for assertions on {@link TestListener}
 */
public class ListenerMatcher extends TypeSafeMatcher<TestListener> {
    /**
     * @since 1.3
     * Creates a matcher that matches when the examined {@link TestListener} has parsing errors
     * @return {@link Matcher}
     */
    public static Matcher<TestListener> hasError() {
        return new ListenerMatcher();
    }

    /**
     * @since 1.3
     * Creates a matcher that matches when the examined {@link TestListener} has no parsing errors.
     *
     * @see ListenerMatcher#hasError()
     * @return {@link Matcher}
     */
    public static Matcher<TestListener> isValid() {
        return not(hasError());
    }

    @Override
    protected boolean matchesSafely(final TestListener item) {
        return !item.getErrorNodeList().isEmpty() ||
                !item.getExceptionList().isEmpty();
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("TestListener has received on error node events");
    }
}
