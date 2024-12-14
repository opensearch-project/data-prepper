/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GenericNotOperatorTest {
    private static final Random random = new Random();

    @Mock
    private Operator<Boolean> mockOperator;

    private int expectedSymbol;
    private int expectedShouldEvaluateRuleIndex;
    private GenericNotOperator genericNotOperator;

    @Mock
    private ParserRuleContext ctx;

    @BeforeEach
    void beforeEach() {
        expectedSymbol = random.nextInt();
        expectedShouldEvaluateRuleIndex = random.nextInt();
        genericNotOperator = new GenericNotOperator(expectedSymbol, expectedShouldEvaluateRuleIndex, mockOperator);
    }

    @Test
    void testGetNumberOfOperands() {
        final int expected = random.nextInt();
        doReturn(expected)
                .when(mockOperator)
                .getNumberOfOperands(ctx);

        final int actual = genericNotOperator.getNumberOfOperands(ctx);
        assertThat(actual, is(expected));
        verify(mockOperator).getNumberOfOperands(ctx);
    }

    @Test
    void testGivenMatchingRuleThenReturnTrue() {
        final RuleContext context = mock(RuleContext.class);
        doReturn(expectedShouldEvaluateRuleIndex)
                .when(context)
                .getRuleIndex();

        assertThat(genericNotOperator.shouldEvaluate(context), is(true));
        assertThat(genericNotOperator.isBooleanOperator(), is(true));
    }

    @Test
    void testGivenNotMatchingRuleThenReturnTrue() {
        final RuleContext context = mock(RuleContext.class);
        doReturn(expectedShouldEvaluateRuleIndex - 1)
                .when(context)
                .getRuleIndex();

        assertThat(genericNotOperator.shouldEvaluate(context), is(false));
    }

    @Test
    void testGetSymbol() {
        assertThat(genericNotOperator.getSymbol(), is(expectedSymbol));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGivenInnerOperatorThenEvaluateReturnInvertedResults(final boolean mockOperatorResponse) {
        final Object arg = new Object();
        doReturn(mockOperatorResponse)
                .when(mockOperator)
                .evaluate(eq(arg), eq(arg));
        assertThat(genericNotOperator.evaluate(arg, arg), not(is(mockOperatorResponse)));
    }
}
