/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JoinExpressionFunctionTest {

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private JoinExpressionFunction joinExpressionFunction;
    private Event testEvent;

    @Mock
    private Function<Object, Object> testFunction;

    @BeforeEach
    public void setUp() {
        joinExpressionFunction = createObjectUnderTest();
    }

    @ParameterizedTest
    @MethodSource("joinSingleList")
    void testJoinSingleListSuccess(final EventKey sourceKey, final String delimiter, final String inputData, final String expectedOutput) {
        testEvent = createTestEvent(inputData);
        List<Object> args;
        if (delimiter == null) {
            args = List.of(sourceKey);
        } else {
            args = List.of(delimiter, sourceKey);
        }
        Object expressionResult = joinExpressionFunction.evaluate(args, testEvent, testFunction);
        assertThat((String)expressionResult, equalTo(expectedOutput));
    }

    @ParameterizedTest
    @MethodSource("joinListsInMap")
    void testJoinListsInMapSuccess(final EventKey sourceKey, final String delimiter, final String inputData, final Map<String, Object> expectedOutput) {
        testEvent = createTestEvent(inputData);
        List<Object> args;
        if (delimiter == null) {
            args = List.of(sourceKey);
        } else {
            args = List.of(delimiter, sourceKey);
        }
        Object expressionResult = joinExpressionFunction.evaluate(args, testEvent, testFunction);
        assertThat((Map<String, Object>)expressionResult, equalTo(expectedOutput));
    }

    @Test
    void testNoArgumentThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        assertThrows(IllegalArgumentException.class,
                () -> joinExpressionFunction.evaluate(List.of(), testEvent, testFunction));
    }

    @Test
    void testTooManyArgumentsThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        EventKey eventKey = eventKeyFactory.createEventKey("/list");
        assertThrows(IllegalArgumentException.class,
                () -> joinExpressionFunction.evaluate(List.of(eventKey, " ", false), testEvent, testFunction));
    }

    @Test
    void testUnexpectedSourceKeyTypeThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        assertThrows(RuntimeException.class,
                () -> joinExpressionFunction.evaluate(List.of("not-an-event-key"), testEvent, testFunction));
    }

    @Test
    void testUnexpectedSourceKeyTypeWithDelimiterThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        assertThrows(RuntimeException.class,
                () -> joinExpressionFunction.evaluate(List.of(",", "not-an-event-key"), testEvent, testFunction));
    }

    @Test
    void testUnexpectedDelimiterTypeThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        EventKey eventKey = eventKeyFactory.createEventKey("/list");
        assertThrows(RuntimeException.class,
                () -> joinExpressionFunction.evaluate(List.of(1234, eventKey), testEvent, testFunction));
    }

    @Test
    void testSourceFieldNotExistsInEventThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        EventKey eventKey = eventKeyFactory.createEventKey("/missingKey");
        assertThrows(RuntimeException.class,
                () -> joinExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction));
    }

    @Test
    void testSourceFieldNotListOrMapThrowsException() {
        testEvent = createTestEvent(Map.of("key", "value"));
        EventKey eventKey = eventKeyFactory.createEventKey("/key");
        assertThrows(RuntimeException.class,
                () -> joinExpressionFunction.evaluate(List.of(eventKey), testEvent, testFunction));
    }

    private static Stream<Arguments> joinSingleList() {
        final EventKeyFactory factory = TestEventKeyFactory.getTestEventFactory();
        final EventKey listKey = factory.createEventKey("/list");
        final String inputData = "{\"list\":[\"string\", 1, true]}";
        return Stream.of(
                Arguments.of(listKey, null, inputData, "string,1,true"),
                Arguments.of(listKey, "\\\\, ", inputData, "string, 1, true"),
                Arguments.of(listKey, " ", inputData, "string 1 true")
        );
    }

    private static Stream<Arguments> joinListsInMap() {
        final EventKeyFactory factory = TestEventKeyFactory.getTestEventFactory();
        final EventKey listKey = factory.createEventKey("/list");
        String testData1 = "{\"list\":{\"key\": [\"string\", 1, true]}}";
        String testData2 = "{\"list\":{\"key1\": [\"string\", 1, true], \"key2\": [1,2,3], \"key3\": \"value3\"}}";
        return Stream.of(
                Arguments.of(listKey, null, testData1, Map.of("key", "string,1,true")),
                Arguments.of(listKey, "\\\\, ", testData1, Map.of("key", "string, 1, true")),
                Arguments.of(listKey, " ", testData1, Map.of("key", "string 1 true")),
                Arguments.of(listKey, null, testData2, Map.of("key1", "string,1,true", "key2", "1,2,3", "key3", "value3"))
        );
    }

    private JoinExpressionFunction createObjectUnderTest() {
        return new JoinExpressionFunction();
    }

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }
}