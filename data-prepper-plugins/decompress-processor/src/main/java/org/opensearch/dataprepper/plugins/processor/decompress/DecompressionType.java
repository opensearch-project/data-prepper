/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.plugins.codec.GZipDecompressionEngine;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum DecompressionType implements DecompressionEngineFactory {
    GZIP("gzip");

    private final String option;

    private static final Map<String, DecompressionType> OPTIONS_MAP = Arrays.stream(DecompressionType.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private static final Map<String, DecompressionEngine> DECOMPRESSION_ENGINE_MAP = Map.of(
            "gzip", new GZipDecompressionEngine()
    );

    DecompressionType(final String option) {
        this.option = option;
    }

    @JsonCreator
    static DecompressionType fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }

    @Override
    public DecompressionEngine getDecompressionEngine() {
        return DECOMPRESSION_ENGINE_MAP.get(this.option);
    }
}
