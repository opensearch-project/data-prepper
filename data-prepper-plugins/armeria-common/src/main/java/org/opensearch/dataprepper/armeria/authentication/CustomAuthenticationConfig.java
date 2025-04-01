/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.armeria.authentication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomAuthenticationConfig {
    private final String customToken;

    @JsonCreator
    public CustomAuthenticationConfig(
            @JsonProperty("custom_token") String customToken) {
        this.customToken = customToken;
    }

    public String customToken() {
        return customToken;
    }
}
