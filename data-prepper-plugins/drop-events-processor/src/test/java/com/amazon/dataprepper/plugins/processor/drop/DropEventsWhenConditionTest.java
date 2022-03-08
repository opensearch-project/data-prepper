package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DropEventsWhenConditionTest {
    @Mock
    private ExpressionEvaluator<Boolean> evaluator;
    @Mock
    private DropEventProcessorConfig dropEventProcessorConfig;

    @BeforeEach
    void beforeEach() {
        doReturn(HandleFailedEventsOption.SKIP)
                .when(dropEventProcessorConfig)
                .getHandleFailedEventsOption();
    }

    @Test
    void testGivenNullWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getWhen();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.shouldEvaluateConditional(), is(false));
    }

    @Test
    void testGivenNotHardcodedTrueWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        doReturn("false")
                .when(dropEventProcessorConfig)
                .getWhen();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.shouldEvaluateConditional(), is(true));
    }

    @Test
    void testIsStatementFalseReturnsEvaluatorResult() {
        final String whenStatement = UUID.randomUUID().toString();
        final Event event = mock(Event.class);
        doReturn(whenStatement)
                .when(dropEventProcessorConfig)
                .getWhen();
        doReturn(true)
                .when(evaluator)
                .evaluate(eq(whenStatement), eq(event));

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        final boolean result = whenCondition.isStatementFalseWith(event);

        assertThat(result, is(true));
        verify(evaluator).evaluate(eq(whenStatement), eq(event));
    }

    // TODO: I recommend to use @Nested for tests which require some additional common setup. In this case, I'd prefer to have a different
    //  test suite entirely - DropEventsWhenCondition_BuilderTest
//    @ParameterizedTest
//    @EnumSource(HandleFailedEventsOption.class)
//    void testAllHandleFailedEventsSettingOptions(final HandleFailedEventsOption option) {
//        doThrow(RuntimeException.class)
//                .when(evaluator)
//                .evaluate(any(), any());
//
//        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
//                .withHandleFailedEventsSetting(option.toString())
//                .withExpressionEvaluator(evaluator)
//                .build();
//        final boolean result = whenCondition.isStatementFalseWith(null);
//
//        if (Arrays.asList(HandleFailedEventsOption.DROP, HandleFailedEventsOption.DROP_SILENTLY).contains(option)) {
//            assertThat(result, is(true));
//        }
//        else if (Arrays.asList(HandleFailedEventsOption.SKIP, HandleFailedEventsOption.SKIP_SILENTLY).contains(option)) {
//            assertThat(result, is(false));
//        }
//        else {
//            throw new RuntimeException("Missing test coverage for enum case " + option);
//        }
//    }

//    @Nested
//    class BuilderTest {
//        @Test
//        void testGivenNullParametersThenDropEventsWhenConditionCreated() {
//            final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
//                    .withExpressionEvaluator(mock(ExpressionEvaluator.class))
//                    .build();
//
//            assertThat(whenCondition, isA(DropEventsWhenCondition.class));
//        }
//
//        @Test
//        void testGivenValidParametersThenDropEventsWhenConditionCreated() {
//            final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
//                    .withDropEventsProcessorConfig(dropEventProcessorConfig)
//                    .withHandleFailedEventsSetting("skip")
//                    .withExpressionEvaluator(mock(ExpressionEvaluator.class))
//                    .build();
//
//            assertThat(whenCondition, isA(DropEventsWhenCondition.class));
//        }
//
//        @Test
//        void testGivenInvalidHandleEventSettingThenDropEventsWhenConditionThrows() {
//            assertThrows(
//                    IllegalArgumentException.class,
//                    () -> new DropEventsWhenCondition.Builder().withHandleFailedEventsSetting("ice cream")
//            );
//        }
//
//        @Test
//        void testGivenUnexpectedWhenSettingParameterTypeThenDropEventsWhenConditionThrows() {
//            assertThrows(
//                    IllegalArgumentException.class,
//                    () ->  new DropEventsWhenCondition.Builder().withWhenSetting(new Object())
//            );
//        }
//
//        @Test
//        void testGivenUnexpectedHandleEventSettingParameterTypeThenDropEventsWhenConditionThrows() {
//            assertThrows(
//                    IllegalArgumentException.class,
//                    () -> new DropEventsWhenCondition.Builder().withHandleFailedEventsSetting(new Object())
//            );
//        }
//
//        @Test
//        void testGivenWhenSettingWithoutExpressionEvaluatorThenBuildThrows() {
//            assertThrows(
//                    IllegalStateException.class,
//                    () -> new DropEventsWhenCondition.Builder()
//                            .withWhenSetting(UUID.randomUUID().toString())
//                            .build()
//            );
//        }
//    }
}
