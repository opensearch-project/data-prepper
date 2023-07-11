/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum AwsIamAuthConfig {
    ROLE("role"),
    DEFAULT("default");
    //TODO add "PROFILE" option

    private static final Map<String, AwsIamAuthConfig> OPTIONS_MAP = Arrays.stream(AwsIamAuthConfig.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    AwsIamAuthConfig(final String option) {
        this.option = option;
    }

    @JsonCreator
    public static AwsIamAuthConfig fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
