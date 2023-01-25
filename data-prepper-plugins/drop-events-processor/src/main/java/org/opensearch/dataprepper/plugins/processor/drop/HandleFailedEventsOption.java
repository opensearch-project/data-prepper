/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.opensearch.dataprepper.model.event.Event;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

enum HandleFailedEventsOption {
    DROP("drop", true, false),
    DROP_SILENTLY("drop_silently", true, true),
    SKIP("skip", false, false),
    SKIP_SILENTLY("skip_silently", false, true);

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

    public boolean isDropEventOption(final Event event, final Throwable cause, final Logger log) {
        if (isLogRequired) {
            log.warn(EVENT, "An exception occurred while processing when expression for event {}", event, cause);
        }
        return isDropEventOption;
    }

    @JsonCreator
    static HandleFailedEventsOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
