/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class SubListExpressionFunctionTest {
    private ExpressionFunction subListExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private ExpressionFunction createObjectUnderTest() {
        return new SubListExpressionFunction();
    }

    @BeforeEach
    void setUp() {
        subListExpressionFunction = createObjectUnderTest();
        testFunction = mock(Function.class);
    }

    @Test
    void getFunctionName_returns_expected_function_name() {
        assertThat(subListExpressionFunction.getFunctionName(), equalTo(SubListExpressionFunction.SUB_LIST_FUNCTION_NAME));
    }

    @Test
    void testSubListValidExtraction() {
        List<String> sampleList = List.of("one", "two", "three", "four", "five");
        testEvent = createTestEvent(Map.of("myList", sampleList));
        Object result = subListExpressionFunction.evaluate(List.of("/myList", 1, 5), testEvent, testFunction);
        assertThat(result, equalTo(sampleList.subList(1, 5)));
    }

    @Test
    void testSubListReturnsNullWhenKeyNotFound() {
        // If the event does not contain the pointer, event.get returns null
        testEvent = createTestEvent(Map.of("otherKey", List.of("A", "B")));
        Object result = subListExpressionFunction.evaluate(List.of("/myList", 0, 1), testEvent, testFunction);
        assertThat(result, equalTo(null));
    }

    @Test
    void testSubListEndIndexAsMinusOne() {
        List<String> sampleList = List.of("a", "b", "c", "d");
        testEvent = createTestEvent(Map.of("listKey", sampleList));
        Object result = subListExpressionFunction.evaluate(List.of("/listKey", 2, -1), testEvent, testFunction);
        assertThat(result, equalTo(sampleList.subList(2, sampleList.size())));
    }

    @Test
    void testSubListWithNonStringJsonPointerThrowsException() {
        testEvent = createTestEvent(Map.of("myList", List.of(1, 2, 3)));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of(123, 0, 1), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() first argument must be a JSON pointer String"));
    }

    @Test
    void testSubListWithEmptyJsonPointerThrowsException() {
        testEvent = createTestEvent(Map.of("myList", List.of("a", "b", "c")));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("", 0, 1), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() JSON pointer cannot be empty"));
    }

    @Test
    void testSubListWithInvalidJsonPointerFormatThrowsException() {
        testEvent = createTestEvent(Map.of("myList", List.of("a", "b", "c")));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("myList", 0, 1), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() expects the first argument to be a JSON pointer (starting with '/')"));
    }

    @Test
    void testSubListWithNonListEventValueThrowsException() {
        testEvent = createTestEvent(Map.of("myList", "not a list"));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/myList", 0, 1), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("/myList is not a List type"));
    }

    @Test
    void testSubListWithInvalidNumberOfArgumentsThrowsException() {
        testEvent = createTestEvent(Map.of("myList", List.of(1, 2, 3)));
        assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/myList", 0), testEvent, testFunction));
    }

    @Test
    void testSubListWithInvalidIndexParsingThrowsException() {
        List<Integer> sampleList = List.of(10, 20, 30);
        testEvent = createTestEvent(Map.of("intList", sampleList));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/intList", "a", "1"), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() start and end index arguments must be integers"));
    }

    @Test
    void testSubListWithNegativeStartIndexThrowsException() {
        List<String> sampleList = List.of("x", "y", "z");
        testEvent = createTestEvent(Map.of("letters", sampleList));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/letters", -1, 2), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() start index out of bounds"));
    }

    @Test
    void testSubListWithStartIndexGreaterThanListSizeThrowsException() {
        List<String> sampleList = List.of("x", "y", "z");
        testEvent = createTestEvent(Map.of("letters", sampleList));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/letters", 4, 4), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() start index out of bounds"));
    }

    @Test
    void testSubListWithEndIndexLessThanStartIndexThrowsException() {
        List<String> sampleList = List.of("a", "b", "c", "d");
        testEvent = createTestEvent(Map.of("myList", sampleList));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/myList", 3, 1), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() end index out of bounds"));
    }

    @Test
    void testSubListWithEndIndexOutOfBoundsThrowsException() {
        List<String> sampleList = List.of("a", "b", "c");
        testEvent = createTestEvent(Map.of("myList", sampleList));
        Exception exception = assertThrows(ExpressionArgumentsException.class,
                () -> subListExpressionFunction.evaluate(List.of("/myList", 1, 5), testEvent, testFunction));
        assertThat(exception.getMessage(), equalTo("subList() end index out of bounds"));
    }
}