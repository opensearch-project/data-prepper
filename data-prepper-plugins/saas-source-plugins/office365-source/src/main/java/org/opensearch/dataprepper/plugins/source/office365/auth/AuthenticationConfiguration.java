/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class AuthenticationConfiguration {
    @JsonProperty("oauth2")
    @NotNull
    private OAuth2Credentials oauth2;

    @Getter
    public static class OAuth2Credentials {
        @JsonProperty("client_id")
        @NotNull
        private String clientId;

        @JsonProperty("client_secret")
        @NotNull
        private String clientSecret;
    }
}
