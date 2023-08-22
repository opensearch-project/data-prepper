/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum CompressionOption {
    NONE("none"),
    GZIP("gzip"),
    SNAPPY("snappy"),
    AUTOMATIC("automatic");

    private static final Map<String, CompressionOption> OPTIONS_MAP = Arrays.stream(CompressionOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private static final Map<String, DecompressionEngine> DECOMPRESSION_ENGINE_MAP = Map.of(
            "none", new NoneDecompressionEngine(),
            "gzip", new GZipDecompressionEngine(),
            "snappy", new SnappyDecompressionEngine()
    );

    private final String option;

    CompressionOption(final String option) {
        this.option = option.toLowerCase();
    }

    public static CompressionOption fromFileName(final String fileName) {
        if (fileName.endsWith(".gz")) {
            return CompressionOption.GZIP;
        } else if(fileName.endsWith(".snappy")){
            return CompressionOption.SNAPPY;
        }else {
            return CompressionOption.NONE;
        }
    }

    public DecompressionEngine getDecompressionEngine() {
        return DECOMPRESSION_ENGINE_MAP.getOrDefault(this.option, new NoneDecompressionEngine());
    }

    @JsonCreator
    public static CompressionOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
