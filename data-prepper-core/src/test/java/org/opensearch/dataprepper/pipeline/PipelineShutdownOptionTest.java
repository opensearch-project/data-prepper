/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineShutdownOptionTest {
    @ParameterizedTest
    @EnumSource(PipelineShutdownOption.class)
    void fromOptionName_returns_same_option(final PipelineShutdownOption option) {
        assertThat(PipelineShutdownOption.fromOptionName(option.getOptionName()), equalTo(option));
        assertThat(option.getShouldShutdownOnPipelineFailurePredicate(), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(PipelineShutdownOption.class)
    void getShouldShutdownOnPipelineFailurePredicate_is_not_null(final PipelineShutdownOption option) {
        assertThat(option.getShouldShutdownOnPipelineFailurePredicate(),
                notNullValue());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 10})
    void getShouldShutdownOnPipelineFailurePredicate_for_ON_ANY_PIPELINE_FAILURE_returns_true(final int numberOfPipelines) {
        final Map<String, Pipeline> pipelinesMap = createPipelinesMap(numberOfPipelines);

        assertThat(
                PipelineShutdownOption.ON_ANY_PIPELINE_FAILURE.getShouldShutdownOnPipelineFailurePredicate().test(pipelinesMap),
                equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void getShouldShutdownOnPipelineFailurePredicate_for_ON_ALL_PIPELINE_FAILURES_returns_false_for_values_greater_than_0(final int numberOfPipelines) {
        final Map<String, Pipeline> pipelinesMap = createPipelinesMap(numberOfPipelines);

        assertThat(
                PipelineShutdownOption.ON_ALL_PIPELINE_FAILURES.getShouldShutdownOnPipelineFailurePredicate().test(pipelinesMap),
                equalTo(false));
    }

    @Test
    void getShouldShutdownOnPipelineFailurePredicate_for_ON_ALL_PIPELINE_FAILURES_returns_true_for_empty_map() {
        final Map<String, Pipeline> pipelinesMap = Collections.emptyMap();

        assertThat(
                PipelineShutdownOption.ON_ALL_PIPELINE_FAILURES.getShouldShutdownOnPipelineFailurePredicate().test(pipelinesMap),
                equalTo(true));
    }

    private Map<String, Pipeline> createPipelinesMap(final int numberOfPipelines) {
        return IntStream.range(0, numberOfPipelines)
                .mapToObj(i -> mock(Pipeline.class))
                .peek(pipeline -> when(pipeline.getName()).thenReturn(UUID.randomUUID().toString()))
                .collect(Collectors.toMap(Pipeline::getName, Function.identity()));
    }
}