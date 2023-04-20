/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public enum PipelineShutdownOption {
    ON_ANY_PIPELINE_FAILURE("on-any-pipeline-failure", pipelines -> true),
    ON_ALL_PIPELINE_FAILURES("on-all-pipeline-failures", pipelines -> pipelines.size() == 0);

    private static final Map<String, PipelineShutdownOption> OPTION_NAMES_MAP = Arrays.stream(PipelineShutdownOption.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));
    private final String optionName;
    private final Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate;

    PipelineShutdownOption(
            final String optionName,
            final Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate) {
        this.optionName = optionName;
        this.shouldShutdownOnPipelineFailurePredicate = shouldShutdownOnPipelineFailurePredicate;
    }

    public Predicate<Map<String, Pipeline>> getShouldShutdownOnPipelineFailurePredicate() {
        return shouldShutdownOnPipelineFailurePredicate;
    }

    public String getOptionName() {
        return optionName;
    }

    @JsonCreator
    static PipelineShutdownOption fromOptionName(final String optionName) {
        return OPTION_NAMES_MAP.get(optionName);
    }
}
