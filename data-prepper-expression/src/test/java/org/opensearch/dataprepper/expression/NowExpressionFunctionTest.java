package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.dataprepper.expression.NowExpressionFunction.NOW_FUNCTION_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

public class NowExpressionFunctionTest {

    private Event testEvent;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private ExpressionFunction createObjectUnderTest() {
        return new NowExpressionFunction();
    }

    @Test
    void now_returns_current_time_in_millis_when_evaluated() {
        testEvent = createTestEvent(Map.of());

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getFunctionName(), equalTo(NOW_FUNCTION_NAME));

        final Object result = objectUnderTest.evaluate(List.of(), testEvent, mock(Function.class));

        assertThat(result, instanceOf(Long.class));
        long nowInMillis = (Long) result;
        long currentTimeInMillis = Instant.now().toEpochMilli();
        assertThat(Math.abs(currentTimeInMillis - nowInMillis), lessThan(5000L));
    }

    @Test
    void now_with_arguments_throws_exception() {
        testEvent = createTestEvent(Map.of());

        final ExpressionFunction objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getFunctionName(), equalTo(NOW_FUNCTION_NAME));

        RuntimeException exception = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () ->
                objectUnderTest.evaluate(List.of("unexpected_argument"), testEvent, mock(Function.class))
        );

        assertThat(exception.getMessage(), equalTo("now() does not take any arguments"));
    }
}
