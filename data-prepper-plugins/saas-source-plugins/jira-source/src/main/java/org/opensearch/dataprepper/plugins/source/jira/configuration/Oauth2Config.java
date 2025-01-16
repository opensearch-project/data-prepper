/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

@Getter
public class Oauth2Config {
    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @JsonProperty("access_token")
    private PluginConfigVariable accessToken;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @AssertTrue(message = "Client ID, Client Secret, Access Token, and Refresh Token are both required for Oauth2")
    private boolean isOauth2ConfigValid() {
        return clientId != null && clientSecret != null && accessToken != null && refreshToken != null;
    }
}
