/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum NotificationTypeOption {
    SQS("sqs");

    private static final Map<String, NotificationTypeOption> OPTIONS_MAP = Arrays.stream(NotificationTypeOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    NotificationTypeOption(final String option) {
        this.option = option.toLowerCase();
    }

    @JsonCreator
    static NotificationTypeOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
