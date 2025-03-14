/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotOperatorTest {
    final NotOperator objectUnderTest = new NotOperator();

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(1));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_unaryOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
        assertThat(objectUnderTest.isBooleanOperator(), is(true));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(true), is(false));
        assertThat(objectUnderTest.evaluate(false), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(true, true));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1));
    }
}
