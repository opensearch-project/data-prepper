package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

class GetEventTypeExpressionFunctionTest {

    private GetEventTypeExpressionFunction createObjectUnderTest() {
        return new GetEventTypeExpressionFunction();
    }

    private Event createTestEvent(final String eventType) {
        return JacksonEvent.builder()
                .withEventType(eventType)
                .withData(Map.of())
                .build();
    }

    @ParameterizedTest
    @ValueSource(strings = {"LOG", "TRACE", "METRIC"})
    void testGetEventTypeReturnsCorrectType(String eventType) {
        GetEventTypeExpressionFunction function = createObjectUnderTest();
        Event testEvent = createTestEvent(eventType);

        Object result = function.evaluate(List.of(), testEvent, Function.identity());

        assertThat(result, equalTo(eventType));
    }

    @Test
    void testGetEventTypeThrowsExceptionWhenArgumentsProvided() {
        GetEventTypeExpressionFunction function = createObjectUnderTest();
        Event testEvent = createTestEvent("LOG");

        assertThrows(RuntimeException.class, () -> function.evaluate(List.of("arg1"), testEvent, Function.identity()));
    }

    @Test
    void testGetEventTypeFunctionRegistered() {
        Event testEvent = createTestEvent("event");
        final GenericExpressionEvaluator evaluator = mock(GenericExpressionEvaluator.class);
        when(evaluator.evaluateConditional("getEventType() == \"event\"", testEvent)).thenReturn(true);
        assertDoesNotThrow(() -> evaluator.evaluateConditional("getEventType() == \"event\"", testEvent));
    }
}