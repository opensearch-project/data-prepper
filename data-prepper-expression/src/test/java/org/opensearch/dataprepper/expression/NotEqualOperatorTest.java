/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.TestObject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotEqualOperatorTest {
    @Mock
    private GenericEqualOperator innerOperator;

    @Mock
    private ParserRuleContext ctx;

    private Operator<Boolean> objectUnderTest;

    @BeforeEach
    void beforeEach() {
        objectUnderTest = new OperatorConfiguration().notEqualOperator(innerOperator);
    }

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(), is(2));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_equalityOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT_EQUAL));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEvalValidArgs(final boolean expected) {
        final Object lhs = mock(Object.class);
        final Object rhs = mock(Object.class);
        doReturn(expected)
                .when(innerOperator)
                .checkedEvaluate(lhs, rhs);
        assertThat(objectUnderTest.evaluate(lhs, rhs), not(is(expected)));
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