/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ContainsExpressionFunctionTest {
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private ContainsExpressionFunction containsExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;
    private String testKey;
    private String testKey2;
    private String testKey3;
    private String testValue;
    private String testValue2;
    private static final int testValueLength = 10;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    @BeforeEach
    public void setUp() {
        testKey = RandomStringUtils.randomAlphabetic(5);
        testKey2 = RandomStringUtils.randomAlphabetic(5);
        testKey3 = RandomStringUtils.randomAlphabetic(5);
        testValue = RandomStringUtils.randomAlphabetic(testValueLength);
        testValue2 = testValue.substring(2, 6);
        testEvent = createTestEvent(Map.of(testKey, testValue, testKey2, testValue2, testKey3, 1234));
        testFunction = mock(Function.class);
    }

    public ContainsExpressionFunction createObjectUnderTest() {
        return new ContainsExpressionFunction();
    }

    @Test
    void evaluate_with_two_eventKeys_when_first_argument_contains_second() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey1 = eventKeyFactory.createEventKey("/" + testKey);
        EventKey eventKey2 = eventKeyFactory.createEventKey("/" + testKey2);
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey1, eventKey2), testEvent, testFunction), equalTo(true));
    }

    @Test
    void evaluate_with_two_eventKeys_when_first_argument_does_not_contain_second() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey1 = eventKeyFactory.createEventKey("/" + testKey);
        EventKey eventKey2 = eventKeyFactory.createEventKey("/" + testKey2);
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey2, eventKey1), testEvent, testFunction), equalTo(false));
    }

    @ParameterizedTest
    @CsvSource({
            "abcde,abcde",
            "abcde,abcd",
            "abcde,a"
    })
    void evaluate_with_two_literal_strings_returns_when_first_argument_contains_second(final String arg1, final String arg2) {
        containsExpressionFunction = createObjectUnderTest();
        assertThat(containsExpressionFunction.evaluate(List.of(arg1, arg2), testEvent, testFunction), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
            "abcde,xyz",
            "abc,abcd"
    })
    void evaluate_with_two_literal_strings_returns_when_first_argument_does_not_contain_second() {
        containsExpressionFunction = createObjectUnderTest();
        assertThat(containsExpressionFunction.evaluate(List.of("abcde", "xyz"), testEvent, testFunction), equalTo(false));
    }

    @Test
    void testContainsEventKeyAndLiteralString() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey = eventKeyFactory.createEventKey("/" + testKey);
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey, testValue2), testEvent, testFunction), equalTo(true));
    }

    @Test
    void testContainsLiteralStringAndEventKey() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey = eventKeyFactory.createEventKey("/" + testKey2);
        assertThat(containsExpressionFunction.evaluate(List.of(testValue, eventKey), testEvent, testFunction), equalTo(true));
    }

    @Test
    void testContainsReturnsFalseWhenNotContained() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey = eventKeyFactory.createEventKey("/" + testKey);
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey, "xyz"), testEvent, testFunction), equalTo(false));
    }

    @Test
    void testContainsReturnsFalseWhenEventKeyResolvesToNull() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey = eventKeyFactory.createEventKey("/unknownKey");
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey, "value"), testEvent, testFunction), equalTo(false));
    }

    @Test
    void testContainsReturnsFalseWhenSecondEventKeyResolvesToNull() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey1 = eventKeyFactory.createEventKey("/" + testKey);
        EventKey eventKey2 = eventKeyFactory.createEventKey("/unknownKey");
        assertThat(containsExpressionFunction.evaluate(List.of(eventKey1, eventKey2), testEvent, testFunction), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
    void testContainsMultipleSubstrings(int endOffset) {
        containsExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(10);
        assertThat(containsExpressionFunction.evaluate(List.of(testString, testString.substring(0, endOffset)), testEvent, testFunction), equalTo(true));
    }

    @Test
    void testThrowsWhenWrongNumberOfArgs() {
        containsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> containsExpressionFunction.evaluate(List.of("abcd"), testEvent, testFunction));
    }

    @Test
    void testThrowsWhenEventKeyResolvesToNonString() {
        containsExpressionFunction = createObjectUnderTest();
        EventKey eventKey = eventKeyFactory.createEventKey("/" + testKey3);
        assertThrows(RuntimeException.class, () -> containsExpressionFunction.evaluate(List.of(eventKey, "value"), testEvent, testFunction));
    }

    @Test
    void testThrowsWhenUnexpectedArgumentType() {
        containsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> containsExpressionFunction.evaluate(List.of("abcd", 1234), testEvent, testFunction));
    }
}
