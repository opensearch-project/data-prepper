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
import org.opensearch.dataprepper.expression.util.TestObject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EqualOperatorTest {
    final GenericEqualOperator objectUnderTest = new OperatorConfiguration().equalOperator();

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_equalityOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
        assertThat(objectUnderTest.isBooleanOperator(), is(true));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.EQUAL));
    }

    @Test
    void testEvalValidArgs() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThat(objectUnderTest.evaluate(testObject1, testObject2), is(true));
        assertThat(objectUnderTest.evaluate(testObject1, testObject3), is(false));
        assertThat(objectUnderTest.evaluate(null, testObject1), is(false));
        assertThat(objectUnderTest.evaluate(testObject1, null), is(false));
        assertThat(objectUnderTest.evaluate(null, null), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(testObject1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(testObject1, testObject2, testObject3));
    }
}
