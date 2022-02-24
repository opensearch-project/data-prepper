/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.TestObject;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NotEqualOperatorTest {
    final NotEqualOperator objectUnderTest = new NotEqualOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT_EQUAL));
    }

    @Test
    void testEvalValidArgs() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThat(objectUnderTest.eval(testObject1, testObject2), is(false));
        assertThat(objectUnderTest.eval(testObject1, testObject3), is(true));
    }

    @Test
    void testEvalInValidArgLength() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(testObject1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.eval(testObject1, testObject2, testObject3));
    }
}