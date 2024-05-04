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

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TypeOfOperatorTest {
    final GenericTypeOfOperator objectUnderTest = new OperatorConfiguration().typeOfOperator();

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_typeOfOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.TYPEOF));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(2, "integer"), is(true));
        assertThat(objectUnderTest.evaluate(2, "string"), is(false));
        assertThat(objectUnderTest.evaluate("testString", "string"), is(true));
        assertThat(objectUnderTest.evaluate("testString", "double"), is(false));
        assertThat(objectUnderTest.evaluate(true, "boolean"), is(true));
        assertThat(objectUnderTest.evaluate(1, "boolean"), is(false));
        assertThat(objectUnderTest.evaluate(1L, "long"), is(true));
        assertThat(objectUnderTest.evaluate(1.0, "long"), is(false));
        assertThat(objectUnderTest.evaluate(1.0, "double"), is(true));
        assertThat(objectUnderTest.evaluate(1L, "double"), is(false));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, "integer", 2));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, "unknown"));
    }
}

