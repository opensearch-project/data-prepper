package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void testGetEventTypeReturnsCorrectType() {
        GetEventTypeExpressionFunction function = createObjectUnderTest();
        Event testEvent = createTestEvent("LOG");

        Object result = function.evaluate(List.of(), testEvent, Function.identity());

        assertThat(result, equalTo("LOG"));
    }

    @Test
    void testGetEventTypeThrowsExceptionWhenArgumentsProvided() {
        GetEventTypeExpressionFunction function = createObjectUnderTest();
        Event testEvent = createTestEvent("LOG");

        assertThrows(RuntimeException.class, () -> function.evaluate(List.of("arg1"), testEvent, Function.identity()));
    }

    @Test
    void testGetEventTypeReturnsNullForNullEventType() {
        GetEventTypeExpressionFunction function = createObjectUnderTest();
        Event testEvent = createTestEvent(null);

        Object result = function.evaluate(List.of(), testEvent, Function.identity());

        assertThat(result, equalTo(null));
    }
}