/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum OTelOutputFormat {
    OTEL("otel"),
    OPENSEARCH("opensearch");
 
    private static final Map<String, OTelOutputFormat> NAMES_MAP = Arrays.stream(OTelOutputFormat.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));
 
    private final String optionName;
 
    OTelOutputFormat(final String optionName) {
        this.optionName = optionName;
    }
 
    @JsonValue
    public String getFormatName() {
        return optionName;
    }
 
    @JsonCreator
    public static OTelOutputFormat fromFormatName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}

