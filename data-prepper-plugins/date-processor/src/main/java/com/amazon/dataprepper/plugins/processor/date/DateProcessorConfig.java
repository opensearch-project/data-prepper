/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.time.ZoneId;
import java.util.List;

public class DateProcessorConfig {
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_TIMEZONE = ZoneId.systemDefault().toString();
    static final String DEFAULT_LOCALE = "en-US";

    @JsonProperty("match")
    @NotEmpty(message = "match can not be empty")
    @Size(min = 2, message = "match should have field name and at least one pattern")
    private List<String> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("timezone")
    private String timezone = DEFAULT_TIMEZONE;

    @JsonProperty("locale")
    private String locale = DEFAULT_LOCALE;

    public List<String> getMatch() {
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
