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

public enum OTelFormatOption {
    JSON("json"),
    PROTOBUF("protobuf");
 
    private static final Map<String, OTelFormatOption> NAMES_MAP = Arrays.stream(OTelFormatOption.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));
 
    private final String optionName;
 
    OTelFormatOption(final String optionName) {
        this.optionName = optionName;
    }
 
    @JsonValue
    public String getFormatName() {
        return optionName;
    }
 
    @JsonCreator
    public static OTelFormatOption fromFormatName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}
