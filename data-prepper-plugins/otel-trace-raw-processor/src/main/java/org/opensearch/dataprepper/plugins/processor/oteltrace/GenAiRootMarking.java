/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Which spans the processor marks with {@code derived.gen_ai.is_root}. A marker the source set is
 * never changed.
 */
public enum GenAiRootMarking {
    /**
     * The outermost GenAI span of each trace, found from the other spans in the batch and from the
     * GenAI coverage cache. The default.
     */
    OUTERMOST("outermost"),
    /**
     * Every span whose {@code gen_ai.operation.name} is {@code invoke_agent} or
     * {@code invoke_workflow}, decided from the span alone, so the result does not depend on how a
     * trace's spans are split into batches. Nested agents are marked too.
     */
    AGENT_SPANS("agent_spans"),
    /** No span. Only markers set at the source, for example by the OpenSearch GenAI SDK. */
    NONE("none");

    private static final Map<String, GenAiRootMarking> OPTIONS = Arrays.stream(values())
            .collect(Collectors.toMap(value -> value.option, value -> value, (a, b) -> a, LinkedHashMap::new));

    private final String option;

    GenAiRootMarking(final String option) {
        this.option = option;
    }

    @JsonCreator
    static GenAiRootMarking fromOptionValue(final String option) {
        if (option == null) {
            return null;
        }
        final GenAiRootMarking value = OPTIONS.get(option);
        if (value == null) {
            throw new IllegalArgumentException("Unknown genai_root_marking value '" + option
                    + "'. Use one of: " + String.join(", ", OPTIONS.keySet()));
        }
        return value;
    }

    @JsonValue
    public String getOptionValue() {
        return option;
    }

    @Override
    public String toString() {
        return option;
    }
}
