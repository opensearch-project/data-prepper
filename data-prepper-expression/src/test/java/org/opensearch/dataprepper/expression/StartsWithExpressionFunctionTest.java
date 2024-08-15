package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.StartsWithExpressionFunction.STARTS_WITH_FUNCTION_NAME;

public class StartsWithExpressionFunctionTest {

    private Event testEvent;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private ExpressionFunction createObjectUnderTest() {
        return new StartsWithExpressionFunction();
    }

    @ParameterizedTest
    @MethodSource("validStartsWithProvider")
    void startsWith_returns_expected_result_when_evaluated(
            final String value, final String prefix, final boolean expectedResult) {
        final String key = "test_key";
        testEvent = createTestEvent(Map.of(key, value));

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getFunctionName(), equalTo(STARTS_WITH_FUNCTION_NAME));

        final Object result = objectUnderTest.evaluate(List.of("/" + key, "\"" + prefix + "\""), testEvent, mock(Function.class));

        assertThat(result, equalTo(expectedResult));
    }

    @Test
    void startsWith_with_a_key_as_the_prefix_returns_expected_result() {

        final String prefixKey = "prefix";
        final String prefixValue = "te";

        final String key = "test_key";
        final String value = "test";
        testEvent = createTestEvent(Map.of(key, value, prefixKey, prefixValue));

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getFunctionName(), equalTo(STARTS_WITH_FUNCTION_NAME));

        final Object result = objectUnderTest.evaluate(List.of("/" + key, "/" + prefixKey), testEvent, mock(Function.class));

        assertThat(result, equalTo(true));
    }

    @Test
    void startsWith_returns_false_when_key_does_not_exist_in_Event() {
        final String key = "test_key";
        testEvent = createTestEvent(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        final ExpressionFunction startsWithExpressionFunction = createObjectUnderTest();
        final Object result = startsWithExpressionFunction.evaluate(List.of("/" + key, "\"abcd\""), testEvent, mock(Function.class));

        assertThat(result, equalTo(false));
    }

    @Test
    void startsWith_without_2_arguments_throws_RuntimeException() {
        final ExpressionFunction startsWithExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> startsWithExpressionFunction.evaluate(List.of("abcd"), testEvent, mock(Function.class)));
    }

    @ParameterizedTest
    @MethodSource("invalidStartsWithProvider")
    void invalid_startsWith_arguments_throws_RuntimeException(final String firstArg, final Object secondArg, final Object value) {
        final ExpressionFunction startsWithExpressionFunction = createObjectUnderTest();
        final String testKey = "test_key";

        assertThrows(RuntimeException.class, () -> startsWithExpressionFunction.evaluate(List.of(firstArg, secondArg), createTestEvent(Map.of(testKey, value)), mock(Function.class)));
    }

    private static Stream<Arguments> validStartsWithProvider() {
        return Stream.of(
                Arguments.of("{test", "{te", true),
                Arguments.of("{test", "{", true),
                Arguments.of("test", "{", false),
                Arguments.of("MyPrefix", "My", true),
                Arguments.of("MyPrefix", "Prefix", false)
        );
    }

    private static Stream<Arguments> invalidStartsWithProvider() {
        return Stream.of(
                Arguments.of("\"abc\"", "/test_key", 1234),
                Arguments.of("abcd", "/test_key", "value"),
                Arguments.of("\"abcd\"", "/test_key", 1234),
                Arguments.of("\"/test_key\"", 1234, "value")
        );
    }
}
