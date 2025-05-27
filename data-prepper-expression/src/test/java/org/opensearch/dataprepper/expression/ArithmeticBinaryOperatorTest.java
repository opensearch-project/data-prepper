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
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ArithmeticBinaryOperatorTest {
    Operator<Object> objectUnderTest;

    private Operator createAddOperatorUnderTest() {
        return new OperatorConfiguration().addOperator();
    }

    private Operator createSubtractOperatorUnderTest() {
        return new OperatorConfiguration().subtractOperator();
    }

    private Operator createMultiplyOperatorUnderTest() {
        return new OperatorConfiguration().multiplyOperator();
    }

    private Operator createDivideOperatorUnderTest() {
        return new OperatorConfiguration().divideOperator();
    }

    private Operator createModOperatorUnderTest() {
        return new OperatorConfiguration().modOperator();
    }

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        objectUnderTest = createAddOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
        objectUnderTest = createMultiplyOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
        objectUnderTest = createDivideOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
        objectUnderTest = createModOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));

        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_arithmeticExpression);
        objectUnderTest = createSubtractOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_stringExpression);
        objectUnderTest = createSubtractOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(1));
    }

    @Test
    void testShouldEvaluate() {
        objectUnderTest = createAddOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_arithmeticExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));

        objectUnderTest = createSubtractOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_arithmeticExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));

        objectUnderTest = createMultiplyOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_multiplicativeExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));

        objectUnderTest = createDivideOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_multiplicativeExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));

        objectUnderTest = createModOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_multiplicativeExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        objectUnderTest = createAddOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.PLUS));
        objectUnderTest = createSubtractOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.SUBTRACT));
        objectUnderTest = createMultiplyOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.MULTIPLY));
        objectUnderTest = createDivideOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.DIVIDE));
        objectUnderTest = createModOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.MOD));
    }

    @Test
    void testInvalid() {
        objectUnderTest = createAddOperatorUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(2, 1, 2));
        objectUnderTest = createSubtractOperatorUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(2, 1, 2));
        objectUnderTest = createMultiplyOperatorUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(2, 1, 2));
        objectUnderTest = createDivideOperatorUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(2, 1, 2));
        objectUnderTest = createModOperatorUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(2, 1, 2));
    }

    @Test
    void testEvalValidArgsForAdd() {
        objectUnderTest = createAddOperatorUnderTest();
        assertThat(objectUnderTest.evaluate(2, 1), is(3));
        assertThat(objectUnderTest.evaluate(2, 1f), is(3f));
        assertThat(objectUnderTest.evaluate(2, 1L), is(3L));
        assertThat(objectUnderTest.evaluate(2, 1.0), is(3.0));

        assertThat(objectUnderTest.evaluate(2f, 1), is(3f));
        assertThat(objectUnderTest.evaluate(2f, 1f), is(3f));
        assertThat(objectUnderTest.evaluate(2f, 1L), is(3f));
        assertThat(objectUnderTest.evaluate(2f, 1.0), is(3.0));

        assertThat(objectUnderTest.evaluate(2L, 1), is(3L));
        assertThat(objectUnderTest.evaluate(2L, 1f), is(3f));
        assertThat(objectUnderTest.evaluate(2L, 1L), is(3L));
        assertThat(objectUnderTest.evaluate(2L, 1.0), is(3.0));

        assertThat(objectUnderTest.evaluate(2.0, 1), is(3.0));
        assertThat(objectUnderTest.evaluate(2.0, 1f), is(3.0));
        assertThat(objectUnderTest.evaluate(2.0, 1L), is(3.0));
        assertThat(objectUnderTest.evaluate(2.0, 1.0), is(3.0));
    }

    @Test
    void testEvalValidArgsForSubtract() {
        objectUnderTest = createSubtractOperatorUnderTest();
        assertThat(objectUnderTest.evaluate(2, 1), is(1));
        assertThat(objectUnderTest.evaluate(2, 1f), is(1f));
        assertThat(objectUnderTest.evaluate(2, 1L), is(1L));
        assertThat(objectUnderTest.evaluate(2, 1.0), is(1.0));

        assertThat(objectUnderTest.evaluate(2f, 1), is(1f));
        assertThat(objectUnderTest.evaluate(2f, 1f), is(1f));
        assertThat(objectUnderTest.evaluate(2f, 1L), is(1f));
        assertThat(objectUnderTest.evaluate(2f, 1.0), is(1.0));

        assertThat(objectUnderTest.evaluate(2L, 1), is(1L));
        assertThat(objectUnderTest.evaluate(2L, 1f), is(1f));
        assertThat(objectUnderTest.evaluate(2L, 1L), is(1L));
        assertThat(objectUnderTest.evaluate(2L, 1.0), is(1.0));

        assertThat(objectUnderTest.evaluate(2.0, 1), is(1.0));
        assertThat(objectUnderTest.evaluate(2.0, 1f), is(1.0));
        assertThat(objectUnderTest.evaluate(2.0, 1L), is(1.0));
        assertThat(objectUnderTest.evaluate(2.0, 1.0), is(1.0));
    }

    @Test
    void testEvalValidArgsForMultiply() {
        objectUnderTest = createMultiplyOperatorUnderTest();
        assertThat(objectUnderTest.evaluate(2, 1), is(2));
        assertThat(objectUnderTest.evaluate(2, 1f), is(2f));
        assertThat(objectUnderTest.evaluate(2, 1L), is(2L));
        assertThat(objectUnderTest.evaluate(2, 1.0), is(2.0));

        assertThat(objectUnderTest.evaluate(2f, 1), is(2f));
        assertThat(objectUnderTest.evaluate(2f, 1f), is(2f));
        assertThat(objectUnderTest.evaluate(2f, 1L), is(2f));
        assertThat(objectUnderTest.evaluate(2f, 1.0), is(2.0));

        assertThat(objectUnderTest.evaluate(2L, 1), is(2L));
        assertThat(objectUnderTest.evaluate(2L, 1f), is(2f));
        assertThat(objectUnderTest.evaluate(2L, 1L), is(2L));
        assertThat(objectUnderTest.evaluate(2L, 1.0), is(2.0));

        assertThat(objectUnderTest.evaluate(2.0, 1), is(2.0));
        assertThat(objectUnderTest.evaluate(2.0, 1f), is(2.0));
        assertThat(objectUnderTest.evaluate(2.0, 1L), is(2.0));
        assertThat(objectUnderTest.evaluate(2.0, 1.0), is(2.0));
    }

    @Test
    void testEvalValidArgsForDivide() {
        objectUnderTest = createDivideOperatorUnderTest();
        assertThat(objectUnderTest.evaluate(5, 2), is(2.5));
        assertThat(objectUnderTest.evaluate(5, 2f), is(2.5f));
        assertThat(objectUnderTest.evaluate(5, 2L), is(2.5));
        assertThat(objectUnderTest.evaluate(5, 2.0), is(2.5));

        assertThat(objectUnderTest.evaluate(5f, 2), is(2.5f));
        assertThat(objectUnderTest.evaluate(5f, 2f), is(2.5f));
        assertThat(objectUnderTest.evaluate(5f, 2L), is(2.5f));
        assertThat(objectUnderTest.evaluate(5f, 2.0), is(2.5));

        assertThat(objectUnderTest.evaluate(5L, 2), is(2.5));
        assertThat(objectUnderTest.evaluate(5L, 2f), is(2.5f));
        assertThat(objectUnderTest.evaluate(5L, 2L), is(2.5));
        assertThat(objectUnderTest.evaluate(5L, 2.0), is(2.5));

        assertThat(objectUnderTest.evaluate(5.0, 2), is(2.5));
        assertThat(objectUnderTest.evaluate(5.0, 2f), is(2.5));
        assertThat(objectUnderTest.evaluate(5.0, 2L), is(2.5));
        assertThat(objectUnderTest.evaluate(5.0, 2.0), is(2.5));
    }

    @Test
    void testEvalValidArgsForMod() {
        objectUnderTest = createModOperatorUnderTest();
        assertThat(objectUnderTest.evaluate(5, 5), is(0));
        assertThat(objectUnderTest.evaluate(5, 2), is(1));
        assertThat(objectUnderTest.evaluate(6, 3), is(0));
    }
}
