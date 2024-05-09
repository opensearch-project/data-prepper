/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    @ParameterizedTest
    @MethodSource("getTypeOfTestData")
    void testEvalValidArgs(Object object, String type, boolean expectedResult) {
        assertThat(objectUnderTest.evaluate(object, type), is(expectedResult));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, "integer", 2));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, "unknown"));
    }

    private static Stream<Arguments> getTypeOfTestData() {
        int testArray[] = {1,2};
        List<Integer> testArrayList = new ArrayList<Integer>(){{add(1);add(2);}};
        return Stream.of(
            Arguments.of(2, "integer", true),
            Arguments.of("testString", "string", true),
            Arguments.of(2L, "long", true),
            Arguments.of(2.0, "double", true),
            Arguments.of(true, "boolean", true),
            Arguments.of(Map.of("k","v"), "map", true),
            Arguments.of(testArray, "array", true),
            Arguments.of(testArrayList, "array", true),
            Arguments.of(2.0, "integer", false),
            Arguments.of(2, "string", false),
            Arguments.of("testString", "long", false),
            Arguments.of("testString", "double", false),
            Arguments.of(2, "boolean", false),
            Arguments.of(2L, "map", false),
            Arguments.of(2, "array", false)
        );
    }
}
