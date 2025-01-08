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
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@Getter
public class AuthenticationConfig {
    @JsonProperty("basic")
    @Valid
    private BasicConfig basicConfig;

    @JsonProperty("oauth2")
    @Valid
    private Oauth2Config oauth2Config;

    @AssertTrue(message = "Authentication config should have either basic or oauth2")
    private boolean isValidAuthenticationConfig() {
        boolean hasBasic = basicConfig != null;
        boolean hasOauth = oauth2Config != null;
        return hasBasic ^ hasOauth;
    }

    public String getAuthType() {
        if (basicConfig != null) {
            return BASIC;
        } else {
            return OAUTH2;
        }
    }
}
