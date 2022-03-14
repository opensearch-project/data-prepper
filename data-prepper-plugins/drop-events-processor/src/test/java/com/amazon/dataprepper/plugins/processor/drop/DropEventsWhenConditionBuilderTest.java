/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
class DropEventsWhenConditionBuilderTest {
    @Mock
    private ExpressionEvaluator<Boolean> evaluator;
    @Mock
    private DropEventProcessorConfig dropEventProcessorConfig;

    @Test
    void testGivenWithDropEventsProcessorConfigNotCalledThenThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DropEventsWhenCondition.Builder()
                        .withExpressionEvaluator(evaluator)
                        .build()
        );
    }

    @Test
    void testGivenValidParametersThenDropEventsWhenConditionCreated() {
        doReturn(UUID.randomUUID().toString()).when(dropEventProcessorConfig).getDropWhen();
        doReturn(HandleFailedEventsOption.SKIP).when(dropEventProcessorConfig).getHandleFailedEventsOption();

        final DropEventsWhenCondition whenCondition = new DropEventsWhenCondition.Builder()
                .withExpressionEvaluator(evaluator)
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .build();

        assertThat(whenCondition, isA(DropEventsWhenCondition.class));
    }

    @Test
    void testGivenEmptyConfigThenDropEventsWhenConditionThrows() {
        assertThrows(
                NullPointerException.class,
                () -> new DropEventsWhenCondition.Builder().withDropEventsProcessorConfig(dropEventProcessorConfig)
        );
    }

    @Test
    void testGivenNullHandleEventSettingThenDropEventsWhenConditionThrows() {
        doReturn(UUID.randomUUID().toString()).when(dropEventProcessorConfig).getDropWhen();
        assertThrows(
                NullPointerException.class,
                () -> new DropEventsWhenCondition.Builder().withDropEventsProcessorConfig(dropEventProcessorConfig)
        );
    }

    @Test
    void testGivenWhenSettingWithoutExpressionEvaluatorThenBuildThrows() {
        doReturn(UUID.randomUUID().toString()).when(dropEventProcessorConfig).getDropWhen();
        doReturn(HandleFailedEventsOption.SKIP).when(dropEventProcessorConfig).getHandleFailedEventsOption();

        assertThrows(
                IllegalStateException.class,
                () -> new DropEventsWhenCondition.Builder()
                        .withDropEventsProcessorConfig(dropEventProcessorConfig)
                        .build()
        );
    }
}
