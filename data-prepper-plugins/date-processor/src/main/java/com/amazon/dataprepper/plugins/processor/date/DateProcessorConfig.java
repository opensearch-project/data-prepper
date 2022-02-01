/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class DateProcessorConfig {
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_TIMEZONE = ZoneId.systemDefault().toString();
    static final String DEFAULT_LOCALE = "en-US";

    @JsonProperty("match")
    @NotEmpty(message = "match can not be empty")
    private Map<String, List<String>> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("timezone")
    private String timezone = DEFAULT_TIMEZONE;

    @JsonProperty("locale")
    private String locale = DEFAULT_LOCALE;

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
