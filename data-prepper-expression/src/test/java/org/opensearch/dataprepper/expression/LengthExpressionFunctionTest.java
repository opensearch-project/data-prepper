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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    void testGetFunctionName() {
        lengthExpressionFunction = createObjectUnderTest();
        assertThat(lengthExpressionFunction.getFunctionName(), is(equalTo("length")));
    }

    @Nested
    class WithStringType {

        @ParameterizedTest
        @ValueSource(ints = {0, 1, 2, 5, 10, 20, 50})
        void testWithEventKeyResolvingToString(int stringLength) {
            lengthExpressionFunction = createObjectUnderTest();
            final String testString = RandomStringUtils.insecure().nextAlphabetic(stringLength);
            testEvent = createTestEvent(Map.of("key", testString));
            EventKey eventKey = eventKeyFactory.createEventKey("/key");
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction), equalTo(testString.length()));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 1, 2, 5, 10, 20, 50})
        void testWithDirectStringArgument(final int stringLength) {
            lengthExpressionFunction = createObjectUnderTest();
            final String testString = RandomStringUtils.insecure().nextAlphabetic(stringLength);
            testEvent = createTestEvent(Collections.emptyMap());
            assertThat(lengthExpressionFunction.evaluate(List.of(testString), testEvent, testFunction),
                    equalTo(testString.length()));
        }
    }

    @Nested
    class WithMapType {

        @Test
        void testWithEmptyMap() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new HashMap<>());
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(0));
        }

        @Test
        void testWithSingleEntryMap() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(Map.of("a", "1"));
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(1));
        }

        @Test
        void testWithMultipleEntryMap() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(Map.of("a", "1", "b", "2", "c", "3"));
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(3));
        }
    }

    @Nested
    class WithListType {

        @Test
        void testWithEmptyList() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new ArrayList<>());
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(0));
        }

        @Test
        void testWithSingleElementList() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(List.of("a"));
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(1));
        }

        @Test
        void testWithMultipleElementList() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(List.of("a", "b", "c", "d"));
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(4));
        }

        @Test
        void testWithListOfMaps() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(List.of(Map.of("a", "1"), Map.of("b", "2")));
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(2));
        }
    }

    @Nested
    class WithArrayType {

        @Test
        void testWithEmptyStringArray() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new String[]{});
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(0));
        }

        @Test
        void testWithSingleElementArray() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new String[]{"a"});
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(1));
        }

        @Test
        void testWithMultipleElementArray() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new String[]{"a", "b", "c"});
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(3));
        }

        @Test
        void testWithIntArray() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(new int[]{1, 2, 3, 4, 5});
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(5));
        }
    }

    @Nested
    class WithErrorCases {

        @Test
        void testWithEventKeyResolvingToNull() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(null);
            assertThat(lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction), equalTo(null));
        }

        @Test
        void testWithEventKeyResolvingToUnsupportedType() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(10);
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction));
            assertThat(exception.getMessage(), equalTo("/key is not a supported type for length(). Supported: String, Map, List, Array"));
        }

        @Test
        void testWithEventKeyResolvingToBoolean() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(true);
            assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction));
        }

        @Test
        void testWithEventKeyResolvingToDouble() {
            lengthExpressionFunction = createObjectUnderTest();
            final Event mockEvent = mock(Event.class);
            final EventKey eventKey = eventKeyFactory.createEventKey("/key");
            when(mockEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(3.14);
            assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(eventKey), mockEvent, testFunction));
        }

        @Test
        void testWithTwoArgs() {
            lengthExpressionFunction = createObjectUnderTest();
            EventKey eventKey1 = eventKeyFactory.createEventKey("/key1");
            EventKey eventKey2 = eventKeyFactory.createEventKey("/key2");
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(eventKey1, eventKey2), testEvent, testFunction));
            assertThat(exception.getMessage(), equalTo("length() takes only one argument"));
        }

        @Test
        void testWithZeroArgs() {
            lengthExpressionFunction = createObjectUnderTest();
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(), testEvent, testFunction));
            assertThat(exception.getMessage(), equalTo("length() takes only one argument"));
        }

        @Test
        void testWithUnexpectedArgumentType() {
            lengthExpressionFunction = createObjectUnderTest();
            testEvent = createTestEvent(Map.of("key", "value"));
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> lengthExpressionFunction.evaluate(List.of(10), testEvent, testFunction));
            assertThat(exception.getMessage().contains("Unexpected argument type"), equalTo(true));
        }
    }
}
