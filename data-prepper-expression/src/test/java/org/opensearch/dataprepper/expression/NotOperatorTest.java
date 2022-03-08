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

class NotOperatorTest {
    final NotOperator objectUnderTest = new NotOperator();

    @Test
    void testGetRuleIndex() {
        assertThat(objectUnderTest.getRuleIndex(), is(DataPrepperExpressionParser.RULE_unaryOperator));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(true), is(false));
        assertThat(objectUnderTest.evaluate(false), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(true, true));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1));
    }
}