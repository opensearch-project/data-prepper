/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrOperatorTest {
    final OrOperator objectUnderTest = new OrOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is("or"));
    }

    @Test
    void testEvalValidArgs() {
        assertThat(objectUnderTest.eval(true, true), is(true));
        assertThat(objectUnderTest.eval(true, false), is(true));
        assertThat(objectUnderTest.eval(false, true), is(true));
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