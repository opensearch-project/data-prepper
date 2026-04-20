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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;

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
    private String bearerToken;

    @AssertTrue(message = "Authentication config should have exactly one of basic, oauth2, or bearer_token")
    private boolean isValidAuthenticationConfig() {
        boolean hasBasic = basicConfig != null;
        boolean hasOauth = oauth2Config != null;
        boolean hasBearer = bearerToken != null && !bearerToken.isBlank();
        int count = (hasBasic ? 1 : 0) + (hasOauth ? 1 : 0) + (hasBearer ? 1 : 0);
        return count == 1;
    }

    public String getAuthType() {
        if (basicConfig != null) {
            return BASIC;
        } else if (bearerToken != null && !bearerToken.isBlank()) {
            return BEARER_TOKEN;
        } else {
            return OAUTH2;
        }
    }
}
