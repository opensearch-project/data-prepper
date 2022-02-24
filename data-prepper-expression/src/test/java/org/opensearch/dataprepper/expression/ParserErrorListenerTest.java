/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class ParserErrorListenerTest {
    @Mock
    private DataPrepperExpressionParser parser;

    @InjectMocks
    private ParserErrorListener errorListener;



    @Test
    void testSyntaxError() {
        errorListener.syntaxError(null, null, 0, 0, null, null);
        assertThat(errorListener.isErrorFound(), is(true));
        assertThat(errorListener.getExceptions().size(), is(1));
    }

    @Test
    void testReportAmbiguity() {
        errorListener.reportAmbiguity(null, null, 0, 0, false, null, null);
        assertThat(errorListener.isErrorFound(), is(false));
        assertThat(errorListener.getExceptions().size(), is(0));
    }

    @Test
    void testReportAttemptingFullContext() {
        errorListener.reportAttemptingFullContext(null, null, 0, 0, null, null);
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testReportContextSensitivity() {
        errorListener.reportContextSensitivity(null, null, 0, 0, 0, null);
        assertThat(errorListener.isErrorFound(), is(false));
        assertThat(errorListener.getExceptions().size(), is(0));
    }

    @Test
    void testMultipleFunctionsCalled() {
        errorListener.syntaxError(null, null, 0, 0, null, null);
        errorListener.reportAmbiguity(null, null, 0, 0, false, null, null);
        errorListener.reportAttemptingFullContext(null, null, 0, 0, null, null);
        errorListener.reportContextSensitivity(null, null, 0, 0, 0, null);

        assertThat(errorListener.isErrorFound(), is(true));
        assertThat(errorListener.getExceptions().size(), is(1));
    }

    @Test
    void testMultipleErrors() {
        errorListener.syntaxError(null, null, 0, 0, null, null);
        errorListener.syntaxError(null, null, 0, 0, null, null);
        errorListener.syntaxError(null, null, 0, 0, null, null);

        assertThat(errorListener.isErrorFound(), is(true));
        assertThat(errorListener.getExceptions().size(), is(3));
    }

    @Test
    void testResetErrors() {
        errorListener.syntaxError(null, null, 0, 0, null, null);
        errorListener.resetErrors();

        assertThat(errorListener.isErrorFound(), is(false));
        assertThat(errorListener.getExceptions().size(), is(0));
    }

}