package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SubListExpressionFunctionTest {
    private SubListExpressionFunction subListExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }


    public SubListExpressionFunction createObjectUnderTest() {
        testFunction = mock(Function.class);
        return new SubListExpressionFunction();
    }

    @Test
    void testFunctionName() {
        subListExpressionFunction = createObjectUnderTest();
        assertEquals("subList", subListExpressionFunction.getFunctionName());
    }

    @Test
    void testWithValidArguments() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4);
        testEvent = createTestEvent(Map.of("list", testList));
        assertThat(subListExpressionFunction.evaluate(List.of("/list", "\"1\"", "\"3\""), testEvent, testFunction), equalTo(List.of(2, 3)));
    }

    @Test
    void testWithValidArgumentsCase2() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4);
        testEvent = createTestEvent(Map.of("list", testList));
        assertThat(subListExpressionFunction.evaluate(List.of("/list", "\"1\"", "\"3\""), testEvent, testFunction), equalTo(List.of(2, 3)));
    }

    @Test
    void testWithValidArgumentsCase3() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4);
        testEvent = createTestEvent(Map.of("main", Map.of("list", testList)));
        assertThat(subListExpressionFunction.evaluate(List.of("/main/list", "\"1\"", "\"3\""), testEvent, testFunction), equalTo(List.of(2, 3)));
    }

    @Test
    void testWithValidArgumentsCase4() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4);
        testEvent = createTestEvent(Map.of("main", Map.of("list", testList)));
        assertThat(subListExpressionFunction.evaluate(List.of("/main/list", "\"1\"", "\"-1\""), testEvent, testFunction), equalTo(List.of(2, 3, 4)));
    }

    @Test
    void testWithValidArgumentsCase5() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4);
        testEvent = createTestEvent(Map.of("list", testList));
        assertThat(subListExpressionFunction.evaluate(List.of("/list", 1, 3), testEvent, testFunction), equalTo(List.of(2, 3)));
    }

    @Test
    void testWithOutOfBoundArgumentsCase1() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"-1\"", "\"4\""), testEvent, testFunction));
        assertEquals("subList() start index should be between 0 and list length (inclusive)", exception.getMessage());
    }

    @Test
    void testWithOutOfBoundArgumentsCase2() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"10\"", "\"4\""), testEvent, testFunction));
        assertEquals("subList() start index should be between 0 and list length (inclusive)", exception.getMessage());
    }

    @Test
    void testWithOutOfBoundArgumentsCase3() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"4\"", "\"2\""), testEvent, testFunction));
        assertEquals("subList() start index should be less than or equal to end index", exception.getMessage());
    }

    @Test
    void testWithOutOfBoundArgumentsCase4() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"1\"", "\"10\""), testEvent, testFunction));
        assertEquals("subList() end index should be between 0 and list length or -1 for list length (exclusive)", exception.getMessage());
    }

    @Test
    void testWithOutOfBoundArgumentsCase5() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"1\"", "\"-2\""), testEvent, testFunction));
        assertEquals("subList() end index should be between 0 and list length or -1 for list length (exclusive)", exception.getMessage());
    }

    @Test
    void testWithInvalidArguments() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"5\"", "\"2\""), testEvent, testFunction));
        assertEquals("subList() start index should be less than or equal to end index", exception.getMessage());
    }

    @Test
    void testWithInvalidArgumentsCase2() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"five\"", "\"two\""), testEvent, testFunction));
        assertEquals("subList() takes 2nd and 3rd arguments as integers", exception.getMessage());
    }

    @Test
    void testWithInvalidArgumentsCase3() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"0\""), testEvent, testFunction));
        assertEquals("subList() takes 3 arguments", exception.getMessage());
    }

    @Test
    void testWithInvalidArgumentsCase4() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of(1, "\"0\"", "\"2\""), testEvent, testFunction));
        assertEquals("subList() takes 1st argument as string type", exception.getMessage());
    }

    @Test
    void testWithInvalidArgumentsCase5() {
        subListExpressionFunction = createObjectUnderTest();
        testEvent = createTestEvent(Map.of("list", "testList"));
        Exception exception = assertThrows(RuntimeException.class, () -> subListExpressionFunction.evaluate(List.of("/list", "\"0\"", "\"2\""), testEvent, testFunction));
        assertEquals("/list is not of list type", exception.getMessage());
    }

    @Test
    void testWithUnknownKeyArgument() {
        subListExpressionFunction = createObjectUnderTest();
        List<Integer> testList = List.of(1, 2, 3, 4, 5, 6);
        testEvent = createTestEvent(Map.of("list", testList));
        assertThat(subListExpressionFunction.evaluate(List.of("/unknownList", 1, 2), testEvent, testFunction), equalTo(null));
    }
}
