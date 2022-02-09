/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class DateProcessorConfig {
    private static final Boolean DEFAULT_FROM_TIME_RECEIVED = false;
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_TIMEZONE = "UTC";

    @JsonProperty("from_time_received")
    private Boolean fromTimeReceived = DEFAULT_FROM_TIME_RECEIVED;

    @JsonProperty("match")
    private Map<String, List<String>> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("timezone")
    private String timezone = DEFAULT_TIMEZONE;

    @JsonProperty("locale")
    private String locale;

    public Boolean getFromTimeReceived() {
        return fromTimeReceived;
    }

    public Map<String, List<String>> getMatch() {
        return match;
    }

    public String getDestination() {
        return destination;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getLocale() {
        return locale;
    }
}
