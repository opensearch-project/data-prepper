/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class ErrorListenerTest {
    private ErrorListener errorListener;

    @BeforeEach
    void beforeEach() {
        errorListener = new ErrorListener();
    }

    @Test
    void testSyntaxError() {
        errorListener.syntaxError(null, null, 0, 0, null, null);
        assertThat(errorListener.isWarningFound(), is(false));
        assertThat(errorListener.isErrorFound(), is(true));
    }

    @Test
    void testReportAmbiguity() {
        errorListener.reportAmbiguity(null, null, 0, 0, false, null, null);
        assertThat(errorListener.isWarningFound(), is(true));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testReportAttemptingFullContext() {
        errorListener.reportAttemptingFullContext(null, null, 0, 0, null, null);
        assertThat(errorListener.isWarningFound(), is(true));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testReportContextSensitivity() {
        errorListener.reportContextSensitivity(null, null, 0, 0, 0, null);
        assertThat(errorListener.isWarningFound(), is(true));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testVisitErrorNode() {
        errorListener.visitErrorNode(null);
        assertThat(errorListener.isWarningFound(), is(false));
        assertThat(errorListener.isErrorFound(), is(true));
    }

    @Test
    void testEnterEveryRule() {
        final ParserRuleContext context = mock(ParserRuleContext.class);
        context.exception = mock(RecognitionException.class);
        errorListener.enterEveryRule(context);
        assertThat(errorListener.isWarningFound(), is(false));
        assertThat(errorListener.isErrorFound(), is(true));
    }

    @Test
    void testMultipleFunctionsCalled() {
        final ParserRuleContext context = mock(ParserRuleContext.class);
        context.exception = mock(RecognitionException.class);
        errorListener.enterEveryRule(context);

        errorListener.syntaxError(null, null, 0, 0, null, null);
        errorListener.reportAmbiguity(null, null, 0, 0, false, null, null);
        errorListener.reportAttemptingFullContext(null, null, 0, 0, null, null);
        errorListener.reportContextSensitivity(null, null, 0, 0, 0, null);
        errorListener.visitErrorNode(null);

        assertThat(errorListener.isWarningFound(), is(true));
        assertThat(errorListener.isErrorFound(), is(true));
    }
}
