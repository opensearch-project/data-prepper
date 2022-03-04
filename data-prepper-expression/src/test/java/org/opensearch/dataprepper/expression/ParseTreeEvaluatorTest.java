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
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseTreeEvaluatorTest {

    @Mock
    private Event event;

    @Mock
    private ParseTreeEvaluatorListener listener;

    @Mock
    private ParseTree parseTree;

    @Mock
    private ParseTreeWalker parseTreeWalker;

    @Mock
    private CoercionService coercionService;

    private ParseTreeEvaluator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new ParseTreeEvaluator(listener, parseTreeWalker, coercionService);
    }

    @Test
    void testEvaluateSuccess() throws CoercionException {
        when(coercionService.coerce(any(), any())).thenReturn(true);

        assertThat(objectUnderTest.evaluate(parseTree, event), is(true));
    }

    @Test
    void testEvaluateFailureInWalk() {
        doThrow(new RuntimeException()).when(parseTreeWalker).walk(listener, parseTree);

        assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
    }

    @Test
    void testEvaluateFailureInGetResult() {
        when(listener.getResult()).thenThrow(new RuntimeException());

        assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
    }

    @Test
    void testEvaluateFailureInCoerce() throws CoercionException {
        when(coercionService.coerce(any(), any())).thenThrow(new CoercionException("test message"));

        assertThrows(ExpressionEvaluationException.class, () -> objectUnderTest.evaluate(parseTree, event));
    }
}