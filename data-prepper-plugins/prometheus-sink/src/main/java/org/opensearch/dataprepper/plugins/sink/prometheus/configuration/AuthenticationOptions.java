/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthenticationOptions {

    @JsonProperty("http_basic")
    private BasicAuthCredentials httpBasic;

    @JsonProperty("bearer_token")
    private BearerTokenOptions bearerTokenOptions;

    public BasicAuthCredentials getHttpBasic() {
        return httpBasic;
    }

    public BearerTokenOptions getBearerTokenOptions() {
        return bearerTokenOptions;
    }
}
