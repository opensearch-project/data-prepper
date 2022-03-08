/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LessThanOperatorTest {
    final Operator<Boolean> objectUnderTest = new OperatorFactory().lessThanOperator();

    @Test
    void testGetRuleIndex() {
        assertThat(objectUnderTest.getRuleIndex(), is(DataPrepperExpressionParser.RULE_relationalOperator));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.LT));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(2, 1), is(false));
        assertThat(objectUnderTest.evaluate(1, 2), is(true));
        assertThat(objectUnderTest.evaluate(1, 1), is(false));
        assertThat(objectUnderTest.evaluate(2f, 1), is(false));
        assertThat(objectUnderTest.evaluate(1f, 2), is(true));
        assertThat(objectUnderTest.evaluate(1f, 1), is(false));
        assertThat(objectUnderTest.evaluate(2, 1f), is(false));
        assertThat(objectUnderTest.evaluate(1, 2f), is(true));
        assertThat(objectUnderTest.evaluate(1, 1f), is(false));
        assertThat(objectUnderTest.evaluate(2f, 1f), is(false));
        assertThat(objectUnderTest.evaluate(1f, 2f), is(true));
        assertThat(objectUnderTest.evaluate(1f, 1f), is(false));
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