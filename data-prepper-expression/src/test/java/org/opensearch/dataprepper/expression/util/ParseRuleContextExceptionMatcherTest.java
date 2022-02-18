/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.util.ParseRuleContextExceptionMatcher.isNotValid;
import static org.opensearch.dataprepper.expression.util.ParseRuleContextExceptionMatcher.isValid;

class ParseRuleContextExceptionMatcherTest {

    @Test
    void testMatchesSafelyNoExceptions() {
        final ParserRuleContext parserRuleContext = mock(ParserRuleContext.class);
        final Matcher<ParserRuleContext> isValid = isValid();
        final Matcher<ParserRuleContext> isNotValid = isNotValid();

        assertTrue(isValid.matches(parserRuleContext));
        assertFalse(isNotValid.matches(parserRuleContext));
    }

    @Test
    void testMatchesSafelyWithExceptions() {
        final ParserRuleContext parserRuleContext = mock(ParserRuleContext.class);
        final Matcher<ParserRuleContext> isValid = isValid();
        final Matcher<ParserRuleContext> isNotValid = isNotValid();

        parserRuleContext.exception = mock(RecognitionException.class);

        assertFalse(isValid.matches(parserRuleContext));
        assertTrue(isNotValid.matches(parserRuleContext));
    }
}