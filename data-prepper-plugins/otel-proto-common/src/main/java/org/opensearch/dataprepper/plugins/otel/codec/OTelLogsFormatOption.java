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

public enum OTelLogsFormatOption {
    JSON("json");
 
    private static final Map<String, OTelLogsFormatOption> NAMES_MAP = Arrays.stream(OTelLogsFormatOption.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));
 
    private final String optionName;
 
    OTelLogsFormatOption(final String optionName) {
        this.optionName = optionName;
    }
 
    @JsonValue
    public String getFormatName() {
        return optionName;
    }
 
    @JsonCreator
    public static OTelLogsFormatOption fromFormatName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}