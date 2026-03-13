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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class LengthExpressionFunctionTest {
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
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
    void testWithEventKeyResolvingToString(int stringLength) {
        lengthExpressionFunction = createObjectUnderTest();
        final String testString = RandomStringUtils.insecure().nextAlphabetic(stringLength);
        testEvent = createTestEvent(Map.of("key", testString));
        EventKey eventKey = eventKeyFactory.createEventKey("/key");
        assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction), equalTo(testString.length()));
    }

    @Test
    void testWithEventKeyResolvingToNull() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", "value"));
        EventKey eventKey = eventKeyFactory.createEventKey("/unknownKey");
        assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction), equalTo(null));
    }

    @Test
    void testWithEventKeyResolvingToNonString() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", 10));
        EventKey eventKey = eventKeyFactory.createEventKey("/key");
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction));
    }

    @Test
    void testWithTwoArgs() {
        lengthExpressionFunction = createObjectUnderTest();
        EventKey eventKey1 = eventKeyFactory.createEventKey("/key1");
        EventKey eventKey2 = eventKeyFactory.createEventKey("/key2");
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of(eventKey1, eventKey2), testEvent, testFunction));
    }

    @Test
    void testWithUnexpectedArgumentType() {
        lengthExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("key", "value"));
        assertThrows(RuntimeException.class, () -> lengthExpressionFunction.evaluate(List.of(10), testEvent, testFunction));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 5, 10, 20, 50})
    void evaluate_with_a_string_argument(final int stringLength) {
        lengthExpressionFunction = createObjectUnderTest();
        final String testString = RandomStringUtils.insecure().nextAlphabetic(stringLength);
        testEvent = createTestEvent(Collections.emptyMap());
        assertThat(lengthExpressionFunction.evaluate(List.of(testString), testEvent, testFunction),
                equalTo(testString.length()));
    }
}
