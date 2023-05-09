/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;

import java.util.List;

class LengthExpressionFunctionTest {
    private LengthExpressionFunction lengthExpressionFunction;
    private Event testEvent;

    public LengthExpressionFunction createObjectUnderTest() {
        testEvent = mock(Event.class);
        return new LengthExpressionFunction();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 5, 10, 20, 50})
    void testWithOneStringArgumentWithOutQuotes(int stringLength) {
        lengthExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(stringLength);
        assertThat(lengthExpressionFunction.evaluate(List.of(testString), testEvent), equalTo(testString.length()));
    }

    @Test
    void testWithOneStringArgumentWithQuotes() {
        lengthExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(5);
        assertThat(lengthExpressionFunction.evaluate(List.of("\""+testString + "\""), testEvent), equalTo(testString.length()));
    }

    @Test
    void testWithTwoArgs() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of("arg1", "arg2"), testEvent));
    }
    
    @Test
    void testWithNonStringArgument() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of(10), testEvent));
    }
    
    @Test
    void testWithInvalidStringArgument() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of("\"arg1"), testEvent));
    }
}
