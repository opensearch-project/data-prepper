/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.prometheus;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

public class ScrapeAuthenticationConfig {

    @Valid
    @JsonProperty("http_basic")
    private HttpBasicConfig httpBasic;

    @JsonProperty("bearer_token")
    private String bearerToken;

    public HttpBasicConfig getHttpBasic() {
        return httpBasic;
    }

    public String getBearerToken() {
        return bearerToken;
    }

    public static class HttpBasicConfig {

        @JsonProperty("username")
        @NotBlank
        private String username;

        @JsonProperty("password")
        @NotBlank
        private String password;

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }
    }
}
