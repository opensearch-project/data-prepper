/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class AzureFederatedAuthConfig {

    @NotNull(message = "client_id is required for azure_federated authentication")
    @JsonProperty("client_id")
    private String clientId;

    @NotNull(message = "token_endpoint is required for azure_federated authentication")
    @JsonProperty("token_endpoint")
    private String tokenEndpoint;

    @NotNull(message = "scope is required for azure_federated authentication")
    @JsonProperty("scope")
    private String scope;

    public String getClientId() {
        return clientId;
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    public String getScope() {
        return scope;
    }
}
