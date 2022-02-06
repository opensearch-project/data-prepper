/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class DateProcessorConfig {
    static final Boolean DEFAULT_FROM_TIME_RECEIVED = false;
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_TIMEZONE = "UTC";
    static final String DEFAULT_LOCALE = "en-US";
    static final String DEFAULT_OUTPUT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    @JsonProperty("from_time_received")
    private Boolean fromTimeReceived = DEFAULT_FROM_TIME_RECEIVED;

    @JsonProperty("match")
    private Map<String, List<String>> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("timezone")
    private String timezone = DEFAULT_TIMEZONE;

    @JsonProperty("locale")
    private String locale = DEFAULT_LOCALE;

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
