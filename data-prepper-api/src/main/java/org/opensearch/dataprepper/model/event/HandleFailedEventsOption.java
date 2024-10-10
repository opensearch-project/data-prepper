/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum HandleFailedEventsOption {
    DROP("drop", true, true),
    DROP_SILENTLY("drop_silently", true, false),
    SKIP("skip", false, true),
    SKIP_SILENTLY("skip_silently", false, false);

    private static final Map<String, HandleFailedEventsOption> OPTIONS_MAP = Arrays.stream(HandleFailedEventsOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;
    private final boolean isDropEventOption;
    private final boolean isLogRequired;

    HandleFailedEventsOption(final String option, final boolean isDropEventOption, final boolean isLogRequired) {
        this.option = option.toLowerCase();
        this.isDropEventOption = isDropEventOption;
        this.isLogRequired = isLogRequired;
    }

    public boolean shouldDropEvent() {
        return isDropEventOption;
    }

    public boolean shouldLog() {
        return isLogRequired;
    }

    @JsonCreator
    static HandleFailedEventsOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }

    @JsonValue
    public String toOptionValue() {
        return option;
    }
}
