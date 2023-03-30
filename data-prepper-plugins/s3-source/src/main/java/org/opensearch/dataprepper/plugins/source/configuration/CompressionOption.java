/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import org.opensearch.dataprepper.plugins.source.compression.AutomaticCompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.GZipCompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.NoneCompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.SnappyCompressionEngine;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum CompressionOption {
    NONE("none", new NoneCompressionEngine()),
    GZIP("gzip", new GZipCompressionEngine()),
    SNAPPY("snappy", new SnappyCompressionEngine()),
    AUTOMATIC("automatic", new AutomaticCompressionEngine());

    private static final Map<String, CompressionOption> OPTIONS_MAP = Arrays.stream(CompressionOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    private final CompressionEngine engine;

    CompressionOption(final String option, final CompressionEngine engine) {
        this.option = option.toLowerCase();
        this.engine = engine;
    }

    public CompressionEngine getEngine() {
        return engine;
    }

    @JsonCreator
    static CompressionOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
