/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum OTelTracesFormatOption {
    JSON("json"),
    PROTOBUF("protobuf");

    private static final Map<String, OTelTracesFormatOption> NAMES_MAP = Arrays.stream(OTelTracesFormatOption.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));

    private final String optionName;

    OTelTracesFormatOption(final String optionName) {
        this.optionName = optionName;
    }

    @JsonValue
    public String getFormatName() {
        return optionName;
    }

    @JsonCreator
    public static OTelTracesFormatOption fromFormatName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}
