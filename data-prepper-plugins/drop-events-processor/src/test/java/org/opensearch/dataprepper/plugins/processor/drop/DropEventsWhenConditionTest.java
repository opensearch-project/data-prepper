/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.opensearch.dataprepper.model.event.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;

import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DropEventsWhenConditionTest {
    @Mock
    private ExpressionEvaluator evaluator;
    @Mock
    private DropEventProcessorConfig dropEventProcessorConfig;

    @Test
    void testGivenNullWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        doReturn(HandleFailedEventsOption.SKIP)
                .when(dropEventProcessorConfig)
                .getHandleFailedEventsOption();

        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.isNotAlwaysTrue(), is(false));
    }

    @Test
    void testGivenNotHardcodedTrueWhenSettingThenShouldEvaluateConditionalReturnFalse() {
        doReturn(HandleFailedEventsOption.SKIP)
                .when(dropEventProcessorConfig)
                .getHandleFailedEventsOption();

        doReturn("false")
                .when(dropEventProcessorConfig)
                .getDropWhen();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        assertThat(whenCondition.isNotAlwaysTrue(), is(true));
    }

    @Test
    void testIsStatementFalseReturnsInvertedEvaluatorResult() {
        doReturn(HandleFailedEventsOption.SKIP)
                .when(dropEventProcessorConfig)
                .getHandleFailedEventsOption();

        final String whenStatement = UUID.randomUUID().toString();
        final Event event = mock(Event.class);
        final boolean evaluatorResult = true;
        doReturn(whenStatement)
                .when(dropEventProcessorConfig)
                .getDropWhen();
        doReturn(evaluatorResult)
                .when(evaluator)
                .evaluateConditional(eq(whenStatement), eq(event));

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        final boolean result = whenCondition.isStatementFalseWith(event);

        assertThat(result, not(is(evaluatorResult)));
        verify(evaluator).evaluateConditional(eq(whenStatement), eq(event));
    }


    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void testAllHandleEventOptions(final HandleFailedEventsOption option) {
        final String whenStatement = UUID.randomUUID().toString();
        final DropEventProcessorConfig dropEventProcessorConfig = mock(DropEventProcessorConfig.class);

        doReturn(whenStatement).when(dropEventProcessorConfig).getDropWhen();
        doReturn(option).when(dropEventProcessorConfig).getHandleFailedEventsOption();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();
        assertThat(whenCondition, isA(DropEventsWhenCondition.class));
    }

    private static Stream<Arguments> provideHandleFailedEventsOptionAndExpectedResult() {
        return Stream.of(
                Arguments.of(HandleFailedEventsOption.DROP, true),
                Arguments.of(HandleFailedEventsOption.DROP_SILENTLY, true),
                Arguments.of(HandleFailedEventsOption.SKIP, false),
                Arguments.of(HandleFailedEventsOption.SKIP_SILENTLY, false)
        );
    }

    @ParameterizedTest
    @MethodSource("provideHandleFailedEventsOptionAndExpectedResult")
    void test(final HandleFailedEventsOption option, final Boolean isStatementFalseWith) {
        final String whenStatement = UUID.randomUUID().toString();
        final DropEventProcessorConfig dropEventProcessorConfig = mock(DropEventProcessorConfig.class);

        doThrow(RuntimeException.class).when(evaluator).evaluateConditional(any(), any());
        doReturn(whenStatement).when(dropEventProcessorConfig).getDropWhen();
        doReturn(option).when(dropEventProcessorConfig).getHandleFailedEventsOption();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(evaluator)
                .build();

        final boolean result = whenCondition.isStatementFalseWith(null);
        assertThat(result, is(isStatementFalseWith));
    }
}
