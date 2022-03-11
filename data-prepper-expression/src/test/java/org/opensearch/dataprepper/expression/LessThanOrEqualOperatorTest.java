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
class LessThanOrEqualOperatorTest {
    final Operator<Boolean> objectUnderTest = new OperatorConfiguration().lessThanOrEqualOperator();

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(), is(2));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_relationalOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.LTE));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(2, 1), is(false));
        assertThat(objectUnderTest.evaluate(1, 2), is(true));
        assertThat(objectUnderTest.evaluate(1, 1), is(true));
        assertThat(objectUnderTest.evaluate(2f, 1), is(false));
        assertThat(objectUnderTest.evaluate(1f, 2), is(true));
        assertThat(objectUnderTest.evaluate(1f, 1), is(true));
        assertThat(objectUnderTest.evaluate(2, 1f), is(false));
        assertThat(objectUnderTest.evaluate(1, 2f), is(true));
        assertThat(objectUnderTest.evaluate(1, 1f), is(true));
        assertThat(objectUnderTest.evaluate(2f, 1f), is(false));
        assertThat(objectUnderTest.evaluate(1f, 2f), is(true));
        assertThat(objectUnderTest.evaluate(1f, 1f), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, 2, 3));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1L, 1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1.0, 1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, 1L));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, 1.0));
    }
}