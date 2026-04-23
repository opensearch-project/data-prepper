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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RegexEqualOperatorTest {
    final GenericRegexMatchOperator objectUnderTest = new OperatorConfiguration().regexEqualOperator();

    @Mock
    private ParserRuleContext ctx;

    @Test
    void testGetNumberOfOperands() {
        assertThat(objectUnderTest.getNumberOfOperands(ctx), is(2));
    }

    @Test
    void testShouldEvaluate() {
        when(ctx.getRuleIndex()).thenReturn(DataPrepperExpressionParser.RULE_regexOperatorExpression);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(true));
        when(ctx.getRuleIndex()).thenReturn(-1);
        assertThat(objectUnderTest.shouldEvaluate(ctx), is(false));
        assertThat(objectUnderTest.isBooleanOperator(), is(true));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.MATCH_REGEX_PATTERN));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate("a", "a*"), is(true));
        assertThat(objectUnderTest.evaluate("a", "b*"), is(false));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate("a"));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate("a", "a", "a*"));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, "a*"));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate("a", 1));
    }

    @Test
    void testEvalInValidPattern() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate("a", "*"));
    }

    @Test
    void evaluate_with_null_lhs_returns_false() {
        assertThat(objectUnderTest.evaluate(null, "a*"), equalTo(false));
    }
    
    @Test
    void testEvalAdversarialArgsWithRe2j() {
        System.setProperty("dataprepper.pattern.provider", "re2j");
        try {
            Boolean result = Assertions.assertTimeoutPreemptively(Duration.ofMillis(1000), () -> {
                return objectUnderTest.evaluate("a".repeat(1000) + "X", "(a+)+");
            });
            assertThat(result, is(false));
        } finally {
            System.clearProperty("dataprepper.pattern.provider");
        }
    }
}
