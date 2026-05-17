/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.SubstringBeforeLastExpressionFunction.FUNCTION_NAME;

class SubstringBeforeLastExpressionFunctionTest {

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private Event testEvent;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private ExpressionFunction createObjectUnderTest() {
        return new SubstringBeforeLastExpressionFunction();
    }

    @ParameterizedTest
    @MethodSource("validSubstringBeforeLastProvider")
    void substringBeforeLast_returns_expected_result_when_evaluated(
            final String value, final String delimiter, final String expectedResult) {
        final String key = "test_key";
        testEvent = createTestEvent(Map.of(key, value));

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getFunctionName(), equalTo(FUNCTION_NAME));

        final EventKey eventKey = eventKeyFactory.createEventKey("/" + key);
        final Object result = objectUnderTest.evaluate(
                List.of(eventKey, delimiter), testEvent, mock(Function.class));

        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void substringBeforeLast_with_two_literals_returns_expected_result() {
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final Object result = objectUnderTest.evaluate(
                List.of("key=a=b", "="), createTestEvent(Map.of()), mock(Function.class));
        assertThat(result, equalTo("key=a"));
    }

    @Test
    void substringBeforeLast_with_a_key_as_the_delimiter_returns_expected_result() {
        final String key = "test_key";
        final String value = "/app/src/main.py";
        testEvent = createTestEvent(Map.of(key, value, "delimiter", "/"));

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final EventKey sourceKey = eventKeyFactory.createEventKey("/" + key);
        final EventKey delimKey = eventKeyFactory.createEventKey("/delimiter");
        final Object result = objectUnderTest.evaluate(
                List.of(sourceKey, delimKey), testEvent, mock(Function.class));
        assertThat(result, equalTo("/app/src"));
    }

    @Test
    void substringBeforeLast_returns_null_when_source_key_does_not_exist_in_Event() {
        testEvent = createTestEvent(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final EventKey eventKey = eventKeyFactory.createEventKey("/test_key");
        final Object result = objectUnderTest.evaluate(
                List.of(eventKey, "delim"), testEvent, mock(Function.class));
        assertThat(result, nullValue());
    }

    @Test
    void substringBeforeLast_returns_source_when_delimiter_key_does_not_exist_in_Event() {
        final String key = "test_key";
        final String value = "hello";
        testEvent = createTestEvent(Map.of(key, value));
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final EventKey sourceKey = eventKeyFactory.createEventKey("/" + key);
        final EventKey delimKey = eventKeyFactory.createEventKey("/unknown");
        final Object result = objectUnderTest.evaluate(
                List.of(sourceKey, delimKey), testEvent, mock(Function.class));
        assertThat(result, equalTo(value));
    }

    @Test
    void substringBeforeLast_returns_null_when_both_keys_do_not_exist_in_Event() {
        testEvent = createTestEvent(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final EventKey key1 = eventKeyFactory.createEventKey("/unknown1");
        final EventKey key2 = eventKeyFactory.createEventKey("/unknown2");
        final Object result = objectUnderTest.evaluate(
                List.of(key1, key2), testEvent, mock(Function.class));
        assertThat(result, nullValue());
    }

    @Test
    void substringBeforeLast_without_2_arguments_throws_RuntimeException() {
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(
                List.of("abcd"), createTestEvent(Map.of()), mock(Function.class)));
    }

    @Test
    void substringBeforeLast_with_eventKey_resolving_to_non_string_throws_RuntimeException() {
        testEvent = createTestEvent(Map.of("test_key", 1234));
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        final EventKey eventKey = eventKeyFactory.createEventKey("/test_key");
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(
                List.of(eventKey, "delim"), testEvent, mock(Function.class)));
    }

    @Test
    void substringBeforeLast_with_unexpected_argument_type_throws_RuntimeException() {
        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.evaluate(
                List.of("abcd", 1234), createTestEvent(Map.of()), mock(Function.class)));
    }

    private static Stream<Arguments> validSubstringBeforeLastProvider() {
        return Stream.of(
                Arguments.of("/app/src/main.py", "/", "/app/src"),
                Arguments.of("abc.def.ghi", ".", "abc.def"),
                Arguments.of("hello-world", "-", "hello"),
                Arguments.of("hello-world", "xyz", "hello-world"),
                Arguments.of("hello-world", "", "hello-world")
        );
    }
}
