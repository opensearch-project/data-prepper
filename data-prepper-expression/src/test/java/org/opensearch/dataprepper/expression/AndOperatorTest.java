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

class AndOperatorTest {
    final AndOperator objectUnderTest = new AndOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.AND));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.eval(true, true), is(true));
        assertThat(objectUnderTest.eval(true, false), is(false));
        assertThat(objectUnderTest.eval(false, true), is(false));
        assertThat(objectUnderTest.eval(false, false), is(false));
    }

    @Test
    void testEvalInValidArgLength() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(true));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(true, true, false));
    }

    @Test
    void testEvalInValidArgType() {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(true, 1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(1, true));
    }
}