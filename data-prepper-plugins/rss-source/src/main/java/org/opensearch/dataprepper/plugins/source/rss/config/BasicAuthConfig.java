/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class BasicAuthConfig {

    @JsonProperty("username")
    @NotBlank(message = "username is required for basic authentication")
    private String username;

    @JsonProperty("password")
    @NotBlank(message = "password is required for basic authentication")
    private String password;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
