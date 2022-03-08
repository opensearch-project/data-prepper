/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InSetOperatorTest {
    final GenericInSetOperator objectUnderTest = new OperatorFactory().inSetOperator();

    @Test
    void testGetRuleIndex() {
        assertThat(objectUnderTest.getRuleIndex(), is(DataPrepperExpressionParser.RULE_setOperator));
    }

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.IN_SET));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate(1, Set.of(1)), is(true));
        assertThat(objectUnderTest.evaluate(1, Collections.emptySet()), is(false));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, 2, Set.of(1, 2)));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(1, 1));
    }
}