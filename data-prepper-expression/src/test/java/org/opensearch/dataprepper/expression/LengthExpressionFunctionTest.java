/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

class LengthExpressionFunctionTest {
    private LengthExpressionFunction lengthExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    public LengthExpressionFunction createObjectUnderTest() {
        testFunction = mock(Function.class);
        return new LengthExpressionFunction();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 5, 10, 20, 50})
    void testWithOneStringArgumentWithOutQuotes(int stringLength) {
        lengthExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(stringLength);
        testEvent = createTestEvent(Map.of("key", testString));
        assertThat(lengthExpressionFunction.evaluate(List.of("/key"), testEvent, testFunction), equalTo(testString.length()));
    }

    @Test
    void testWithOneStringArgumentWithQuotes() {
        lengthExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(5);
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of("\"" + testString + "\""), testEvent, testFunction));
    }

    @Test
    void testWithTwoArgs() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of("arg1", "arg2"), testEvent, testFunction));
    }
    
    @Test
    void testWithNonStringArgument() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of(10), testEvent, testFunction));
    }
    
    @Test
    void testWithInvalidArgument() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", 10));
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of("/key"), testEvent, testFunction));
    }

    @Test
    void testWithUnknownKeyArgument() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", "value"));
        assertThat(lengthExpressionFunction.evaluate(List.of("/unknownKey"), testEvent, testFunction), equalTo(null));
    }

    @Test
    void testWithZeroLengthString() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", 10));
        assertThat(lengthExpressionFunction.evaluate(List.of(""), testEvent, testFunction), equalTo(0));
    }
}
