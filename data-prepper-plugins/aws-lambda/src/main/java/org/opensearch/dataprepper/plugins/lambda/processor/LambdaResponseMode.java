/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum LambdaResponseMode {
    REPLACE("replace"),
    MERGE("merge");

    private static final Map<String, LambdaResponseMode> OPTIONS_MAP = Arrays.stream(LambdaResponseMode.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    LambdaResponseMode(final String option) {
        this.option = option;
    }

    @JsonValue
    public String getOption() {
        return option;
    }

    @JsonCreator
    public static LambdaResponseMode fromOption(final String option) {
        return OPTIONS_MAP.get(option);
    }

}
