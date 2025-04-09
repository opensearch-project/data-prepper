/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.testcustomauth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestCustomAuthenticationConfig {
    private final String customToken;
    private final String header;

    @JsonCreator
    public TestCustomAuthenticationConfig(
            @JsonProperty("custom_token") String customToken,
            @JsonProperty("header") String header) {
        this.customToken = customToken;
        this.header = header != null ? header : "authentication";
    }

    public String customToken() {
        return customToken;
    }

    public String header() {
        return header;
    }
}
