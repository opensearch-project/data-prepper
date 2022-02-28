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

class RegexNotEqualOperatorTest {
    final RegexNotEqualOperator objectUnderTest = new RegexNotEqualOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT_MATCH_REGEX_PATTERN));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.evaluate("a", "a*"), is(false));
        assertThat(objectUnderTest.evaluate("a", "b*"), is(true));
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
}