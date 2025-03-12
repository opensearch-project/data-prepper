/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GenericExpressionEvaluator_ConditionalTest {

    @Mock
    private Parser<ParseTree> parser;
    @Mock
    private Evaluator<ParseTree, Event> evaluator;
    @InjectMocks
    private GenericExpressionEvaluator statementEvaluator;

    @Test
    void testGivenValidParametersThenEvaluatorResultReturned() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);
        final Event event = mock(Event.class);
        final Boolean expected = true;

        doReturn(parseTree).when(parser).parse(eq(statement));
        doReturn(expected).when(evaluator).evaluate(eq(parseTree), eq(event));

        final Boolean actual = statementEvaluator.evaluateConditional(statement, event);

        assertThat(actual, is(expected));
        verify(parser).parse(eq(statement));
        verify(evaluator).evaluate(eq(parseTree), eq(event));
    }

    @Test
    void testGivenUnexpectedEvaluatorResultTypeThenExceptionThrown() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);
        final Event event = mock(Event.class);
        final Object result = mock(Object.class);

        doReturn(parseTree).when(parser).parse(eq(statement));
        doReturn(result).when(evaluator).evaluate(eq(parseTree), eq(event));

        assertThrows(ClassCastException.class, () -> statementEvaluator.evaluateConditional(statement, event));

        verify(parser).parse(eq(statement));
        verify(evaluator).evaluate(eq(parseTree), eq(event));
    }

    @Test
    void testGivenParserThrowsExceptionThenExceptionThrown() {
        final String statement = UUID.randomUUID().toString();

        doThrow(new RuntimeException()).when(parser).parse(eq(statement));

        assertThrows(ExpressionParsingException.class, () -> statementEvaluator.evaluateConditional(statement, null));

        verify(parser).parse(eq(statement));
        verify(evaluator, times(0)).evaluate(any(), any());
    }

    @Test
    void testGivenEvaluatorThrowsExceptionThenExceptionThrown() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);
        final Event event = mock(Event.class);

        doReturn(parseTree).when(parser).parse(eq(statement));
        doThrow(new RuntimeException()).when(evaluator).evaluate(eq(parseTree), eq(event));

        assertThat(statementEvaluator.evaluateConditional(statement, event), equalTo(false));

        verify(parser).parse(eq(statement));
        verify(evaluator).evaluate(eq(parseTree), eq(event));
    }
}
