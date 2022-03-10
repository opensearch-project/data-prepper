/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseTreeEvaluatorTest {

    @Mock
    private Event event;

    @Mock
    private OperatorProvider operatorProvider;

    @Mock
    private ParseTree parseTree;

    @Mock
    private ParseTreeWalker parseTreeWalker;

    @Mock
    private ParseTreeCoercionService coercionService;

    private ParseTreeEvaluator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new ParseTreeEvaluator(operatorProvider, parseTreeWalker, coercionService);
    }

    @Test
    void testEvaluateSuccess() throws ExpressionCoercionException {
        try (final MockedConstruction<ParseTreeEvaluatorListener> ignored =
                     mockConstruction(ParseTreeEvaluatorListener.class, (mock, context) -> when(mock.getResult()).thenReturn(true))) {
            when(coercionService.coerce(any(), any())).thenAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                final Object res = args[0];
                final Class<Boolean> clazz = (Class<Boolean>) args[1];
                return clazz.cast(res);
            });
            assertThat(objectUnderTest.evaluate(parseTree, event), is(true));
        }
    }

    @Test
    void testEvaluateFailureInWalk() {
        try (final MockedConstruction<ParseTreeEvaluatorListener> ignored =
                     mockConstruction(ParseTreeEvaluatorListener.class)) {
            doThrow(new RuntimeException()).when(parseTreeWalker).walk(
                    any(ParseTreeEvaluatorListener.class), any(ParseTree.class));
            assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
        }
    }

    @Test
    void testEvaluateFailureInGetResult() {
        try (final MockedConstruction<ParseTreeEvaluatorListener> ignored =
                     mockConstruction(ParseTreeEvaluatorListener.class,
                             (mock, context) -> when(mock.getResult()).thenThrow(new RuntimeException()))) {
            assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
        }
    }

    @Test
    void testEvaluateFailureInCoerce() throws ExpressionCoercionException {
        try (final MockedConstruction<ParseTreeEvaluatorListener> ignored =
                     mockConstruction(ParseTreeEvaluatorListener.class, (mock, context) -> when(mock.getResult()).thenReturn(true))) {
            when(coercionService.coerce(any(), any())).thenThrow(new ExpressionCoercionException("test message"));
            assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
        }
    }
}