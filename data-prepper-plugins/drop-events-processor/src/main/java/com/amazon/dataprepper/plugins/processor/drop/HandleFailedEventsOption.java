/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

enum HandleFailedEventsOption {
    SKIP("skip"),
    SKIP_SILENTLY("skip_silently"),
    DROP("drop"),
    DROP_SILENTLY("drop_silently");

    private static final Map<String, HandleFailedEventsOption> OPTIONS_MAP = Arrays.stream(HandleFailedEventsOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    HandleFailedEventsOption(final String option) {
        this.option = option.toLowerCase();
    }

    @JsonCreator
    static HandleFailedEventsOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
