/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.BEARER_TOKEN;
import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.OAUTH2;


@Getter
public class AuthenticationConfig {
    @JsonProperty("basic")
    @Valid
    private BasicConfig basicConfig;

    @JsonProperty("oauth2")
    @Valid
    private Oauth2Config oauth2Config;

    @JsonProperty("bearer_token")
    private PluginConfigVariable bearerToken;

    @JsonIgnore
    public String getBearerTokenValue() {
        if (bearerToken == null || bearerToken.getValue() == null) {
            return null;
        }
        final Object value = bearerToken.getValue();
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    @AssertTrue(message = "Authentication config should have exactly one of basic, oauth2, or bearer_token")
    private boolean isValidAuthenticationConfig() {
        boolean hasBasic = basicConfig != null;
        boolean hasOauth = oauth2Config != null;
        final String tokenValue = getBearerTokenValue();
        boolean hasBearer = tokenValue != null && !tokenValue.isEmpty();
        int count = (hasBasic ? 1 : 0) + (hasOauth ? 1 : 0) + (hasBearer ? 1 : 0);
        return count == 1;
    }

    public String getAuthType() {
        if (basicConfig != null) {
            return BASIC;
        }
        final String tokenValue = getBearerTokenValue();
        if (tokenValue != null && !tokenValue.isEmpty()) {
            return BEARER_TOKEN;
        }
        return OAUTH2;
    }
}
