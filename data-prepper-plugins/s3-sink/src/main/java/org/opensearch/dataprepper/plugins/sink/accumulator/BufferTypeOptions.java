/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines all the buffer types enumerations.
 */
public enum BufferTypeOptions {

    INMEMORY("in_memory", new InMemoryBufferFactory()),
    LOCALFILE("local_file", new LocalFileBufferFactory());

    private final String option;
    private final BufferFactory bufferType;
    private static final Map<String, BufferTypeOptions> OPTIONS_MAP = Arrays.stream(BufferTypeOptions.values())
            .collect(Collectors.toMap(value -> value.option, value -> value));

    BufferTypeOptions(final String option, final BufferFactory bufferType) {
        this.option = option.toLowerCase();
        this.bufferType = bufferType;
    }

    public BufferFactory getBufferType() {
        return bufferType;
    }

    @JsonCreator
    static BufferTypeOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}