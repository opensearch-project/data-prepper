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
    final GenericEqualOperator objectUnderTest = new OperatorFactory().notEqualOperator();

    @Test
    void testGetSymbol() {
        assertThat(objectUnderTest.getSymbol(), is(DataPrepperExpressionParser.NOT_EQUAL));
    }

    @Test
    void testEvalValidArgs() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThat(objectUnderTest.evaluate(testObject1, testObject2), is(false));
        assertThat(objectUnderTest.evaluate(testObject1, testObject3), is(true));
        assertThat(objectUnderTest.evaluate(null, testObject1), is(true));
        assertThat(objectUnderTest.evaluate(testObject1, null), is(true));
        assertThat(objectUnderTest.evaluate(null, null), is(false));
    }

    @Test
    void testEvalInValidArgLength() {
        final TestObject testObject1 = new TestObject("1");
        final TestObject testObject2 = new TestObject("1");
        final TestObject testObject3 = new TestObject("2");
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(testObject1));
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.evaluate(testObject1, testObject2, testObject3));
    }
}