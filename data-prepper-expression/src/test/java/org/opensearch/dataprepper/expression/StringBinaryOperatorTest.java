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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StringBinaryOperatorTest {
    Operator<Object> objectUnderTest;

    private Operator<Object> createConcatOperatorUnderTest() {
        return new OperatorConfiguration().addOperator();
    }

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        objectUnderTest = createConcatOperatorUnderTest();
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
    }

    @Test
    void testShouldEvaluate() {
        objectUnderTest = createConcatOperatorUnderTest();
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_stringExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
    }

    @Test
    void testGetSymbol() {
        objectUnderTest = createConcatOperatorUnderTest();
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.PLUS));
    }

    @Test
    void testEvalValidArgsForConcat() {
        objectUnderTest = createConcatOperatorUnderTest();
        String testString = "testString";
        assertThat(objectUnderTest.evaluate("string1", "string2"), equalTo("string1string2"));
        assertThat(objectUnderTest.evaluate(testString, "string2"), equalTo(testString+"string2"));
        assertThat(objectUnderTest.evaluate(testString, testString), equalTo(testString+testString));
    }

}
