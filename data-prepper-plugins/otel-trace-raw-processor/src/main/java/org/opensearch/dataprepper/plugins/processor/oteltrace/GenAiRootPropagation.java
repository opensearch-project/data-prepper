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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Which trace root spans get GenAI attributes and token totals copied from their children.
 */
public enum GenAiRootPropagation {
    /**
     * Only a root that is its trace's AI root: a GenAI span, or a span marked
     * {@code derived.gen_ai.is_root: true} at the source. A non-GenAI root, such as the HTTP span
     * an agent runs behind, keeps its own attributes; consumers read the marked agent span instead.
     */
    AI_ROOT("ai_root"),
    /** Every trace root, including non-GenAI roots. The behavior before {@code ai_root}. */
    ALL_ROOTS("all_roots"),
    /** No root. */
    NONE("none");

    private static final Map<String, GenAiRootPropagation> OPTIONS = Arrays.stream(values())
            .collect(Collectors.toMap(value -> value.option, value -> value));

    private final String option;

    GenAiRootPropagation(final String option) {
        this.option = option;
    }

    @JsonCreator
    static GenAiRootPropagation fromOptionValue(final String option) {
        return OPTIONS.get(option.toLowerCase());
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
