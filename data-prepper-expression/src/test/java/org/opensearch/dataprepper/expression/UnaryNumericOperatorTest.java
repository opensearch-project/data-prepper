/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.RuleContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class UnaryNumericOperatorTest {
    private static final Random RANDOM = new Random();
    private static final int SYMBOL = RANDOM.nextInt();

    @Mock
    private Map<Class<? extends Number>, Function<Number, ? extends Number>> strategy;

    @Mock
    private RuleContext ctx;

    private UnaryNumericOperator unaryNumericOperator;

    @BeforeEach
    void beforeEach() {
        unaryNumericOperator = new UnaryNumericOperator(SYMBOL, strategy);
    }

    @Test
    void testGivenUnaryOperatorExpressionThenShouldEvaluateReturnTrue() {
        doReturn(DataPrepperExpressionParser.RULE_unaryOperatorExpression)
                .when(ctx)
                .getRuleIndex();
        assertThat(unaryNumericOperator.shouldEvaluate(ctx), is(true));
    }

    @Test
    void testGivenNoArgsThenExceptionThrown() {
        assertThrows(IllegalArgumentException.class, unaryNumericOperator::evaluate);
    }

    @Test
    void testGivenUnexpectedArgTypeThenExceptionThrown() {
        assertThrows(IllegalArgumentException.class, () -> unaryNumericOperator.evaluate(new Object()));
    }

    @ParameterizedTest
    @ValueSource(ints = {
            DataPrepperExpressionParser.RULE_expression,
            DataPrepperExpressionParser.RULE_conditionalExpression,
            DataPrepperExpressionParser.RULE_conditionalOperator,
            DataPrepperExpressionParser.RULE_equalityOperatorExpression,
            DataPrepperExpressionParser.RULE_equalityOperator,
            DataPrepperExpressionParser.RULE_regexOperatorExpression,
            DataPrepperExpressionParser.RULE_regexEqualityOperator,
            DataPrepperExpressionParser.RULE_relationalOperatorExpression,
            DataPrepperExpressionParser.RULE_relationalOperator,
            DataPrepperExpressionParser.RULE_setOperatorExpression,
            DataPrepperExpressionParser.RULE_setOperator,
            DataPrepperExpressionParser.RULE_parenthesesExpression,
            DataPrepperExpressionParser.RULE_regexPattern,
            DataPrepperExpressionParser.RULE_setInitializer,
            DataPrepperExpressionParser.RULE_unaryOperator,
            DataPrepperExpressionParser.RULE_primary,
            DataPrepperExpressionParser.RULE_jsonPointer,
            DataPrepperExpressionParser.RULE_variableIdentifier,
            DataPrepperExpressionParser.RULE_variableName,
            DataPrepperExpressionParser.RULE_literal

    })
    void testGivenNotUnaryOperatorExpressionThenShouldEvaluateReturnFalse(final Integer ruleIndex) {
        doReturn(ruleIndex)
                .when(ctx)
                .getRuleIndex();
        assertThat(unaryNumericOperator.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        assertThat(unaryNumericOperator.getSymbol(), is(SYMBOL));
    }

    @ParameterizedTest
    @MethodSource("argProvider")
    void testGivenSingleArgThenEvaluateCallsStrategyFunction(final Number arg) {
        final Number expected = RANDOM.nextInt();

        final Function<Number, Number> strategyFunction = mock(Function.class);

        doReturn(expected)
                .when(strategyFunction)
                .apply(eq(arg));
        doReturn(strategyFunction)
                .when(strategy)
                .get(arg.getClass());

        final Number result = unaryNumericOperator.evaluate(arg);

        verify(strategyFunction).apply(any());
        assertThat(result, is(expected));
    }

    private static Stream<Arguments> argProvider() {
        return Stream.of(
                Arguments.of(-5),
                Arguments.of(0),
                Arguments.of(10),
                Arguments.of(Integer.MIN_VALUE),
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(-42.95f),
                Arguments.of(0.0f),
                Arguments.of(9999.123f),
                Arguments.of(Float.MIN_VALUE),
                Arguments.of(Float.MAX_VALUE),
                Arguments.of(Float.MIN_NORMAL)
        );
    }
}