package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.event.Event;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Arrays;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DropEventsWhenConditionTest {
    @Mock
    private ExpressionEvaluator<Boolean> evaluator;

    @Test
    void testGivenNullWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withWhenSetting(null)
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.shouldEvaluateConditional(), is(false));
    }

    @Test
    void testGivenWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withWhenSetting("true")
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.shouldEvaluateConditional(), is(true));
    }

    @Test
    void testIsStatementFalseReturnsEvaluatorResult() {
        final String whenStatement = UUID.randomUUID().toString();
        final Event event = mock(Event.class);
        doReturn(true)
                .when(evaluator)
                .evaluate(eq(whenStatement), eq(event));

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withWhenSetting(whenStatement)
                .withExpressionEvaluator(evaluator)
                .build();

        final boolean result = whenCondition.isStatementFalseWith(event);

        assertThat(result, is(true));
        verify(evaluator).evaluate(eq(whenStatement), eq(event));
    }

    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void testAllHandleFailedEventsSettingOptions(final HandleFailedEventsOption option) {
        doThrow(RuntimeException.class)
                .when(evaluator)
                .evaluate(any(), any());

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withHandleFailedEventsSetting(option.toString())
                .withExpressionEvaluator(evaluator)
                .build();
        final boolean result = whenCondition.isStatementFalseWith(null);

        if (Arrays.asList(HandleFailedEventsOption.drop, HandleFailedEventsOption.drop_silently).contains(option)) {
            assertThat(result, is(true));
        }
        else if (Arrays.asList(HandleFailedEventsOption.skip, HandleFailedEventsOption.skip_silently).contains(option)) {
            assertThat(result, is(false));
        }
        else {
            throw new RuntimeException("Missing test coverage for enum case " + option);
        }
    }

    @Nested
    class BuilderTest {
        @Test
        void testGivenNullParametersThenDropEventsWhenConditionCreated() {
            final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                    .withExpressionEvaluator(mock(ExpressionEvaluator.class))
                    .build();

            assertThat(whenCondition, isA(DropEventsWhenCondition.class));
        }

        @Test
        void testGivenValidParametersThenDropEventsWhenConditionCreated() {
            final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                    .withWhenSetting("true")
                    .withHandleFailedEventsSetting("skip")
                    .withExpressionEvaluator(mock(ExpressionEvaluator.class))
                    .build();

            assertThat(whenCondition, isA(DropEventsWhenCondition.class));
        }

        @Test
        void testGivenInvalidHandleEventSettingThenDropEventsWhenConditionThrows() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DropEventsWhenCondition.Builder().withHandleFailedEventsSetting("ice cream")
            );
        }

        @Test
        void testGivenUnexpectedWhenSettingParameterTypeThenDropEventsWhenConditionThrows() {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->  new DropEventsWhenCondition.Builder().withWhenSetting(new Object())
            );
        }

        @Test
        void testGivenUnexpectedHandleEventSettingParameterTypeThenDropEventsWhenConditionThrows() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new DropEventsWhenCondition.Builder().withHandleFailedEventsSetting(new Object())
            );
        }

        @Test
        void testGivenWhenSettingWithoutExpressionEvaluatorThenBuildThrows() {
            assertThrows(
                    IllegalStateException.class,
                    () -> new DropEventsWhenCondition.Builder()
                            .withWhenSetting(UUID.randomUUID().toString())
                            .build()
            );
        }
    }
}
