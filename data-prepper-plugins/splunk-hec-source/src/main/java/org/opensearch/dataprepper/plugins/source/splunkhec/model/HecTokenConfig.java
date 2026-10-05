/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotBlank;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class HecTokenConfig {

    @JsonProperty("token")
    @JsonPropertyDescription("The HEC token value clients present on the Authorization: Splunk header.")
    @NotBlank(message = "token must not be null or blank")
    private String token;

    @JsonProperty("enabled")
    @JsonPropertyDescription("Whether this token is accepted. When false, requests using it are rejected with 403. Defaults to true.")
    private boolean enabled = true;

    @JsonProperty("defaults")
    @JsonPropertyDescription("Default metadata applied to events for this token when the request does not specify them.")
    private HecTokenDefaults defaults;

    public String getToken() {
        return token;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public HecTokenDefaults getDefaults() {
        return defaults;
    }

    public static class HecTokenDefaults {

        @JsonProperty("index")
        @JsonPropertyDescription("Default index metadata attribute used for sink routing when the event omits it.")
        private String index;

        @JsonProperty("sourcetype")
        @JsonPropertyDescription("Default sourcetype applied when the event omits it.")
        private String sourcetype;

        @JsonProperty("source")
        @JsonPropertyDescription("Default source applied when the event omits it.")
        private String source;

        @JsonProperty("host")
        @JsonPropertyDescription("Default host applied when the event omits it.")
        private String host;

        @JsonProperty("fields")
        @JsonPropertyDescription("Default additional fields merged into each event for this token.")
        private Map<String, String> fields;

        public String getIndex() {
            return index;
        }

        public String getSourcetype() {
            return sourcetype;
        }

        public String getSource() {
            return source;
        }

        public String getHost() {
            return host;
        }

        public Map<String, String> getFields() {
            if (fields == null) {
                return Collections.emptyMap();
            }
            return Collections.unmodifiableMap(fields);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HecTokenConfig that = (HecTokenConfig) o;
        return Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }
}
