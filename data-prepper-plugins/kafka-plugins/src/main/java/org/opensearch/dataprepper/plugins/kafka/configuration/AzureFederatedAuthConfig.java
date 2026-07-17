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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.net.URISyntaxException;

public class AzureFederatedAuthConfig {

    @NotNull(message = "azure_tenant_id is required for azure_federated authentication")
    @JsonProperty("azure_tenant_id")
    private String azureTenantId;

    @NotNull(message = "azure_client_id is required for azure_federated authentication")
    @JsonProperty("azure_client_id")
    private String azureClientId;

    @NotNull(message = "azure_token_endpoint is required for azure_federated authentication")
    @JsonProperty("azure_token_endpoint")
    private String azureTokenEndpoint;

    @NotNull(message = "scope is required for azure_federated authentication")
    @JsonProperty("scope")
    private String scope;

    public String getAzureTenantId() {
        return azureTenantId;
    }

    public String getAzureClientId() {
        return azureClientId;
    }

    public String getAzureTokenEndpoint() {
        return azureTokenEndpoint;
    }

    public String getScope() {
        return scope;
    }

    @AssertTrue(message = "azure_tenant_id must match the tenant in azure_token_endpoint")
    public boolean isTenantConsistentWithEndpoint() {
        // null fields are reported by their own @NotNull constraints
        if (azureTenantId == null || azureTokenEndpoint == null) {
            return true;
        }
        final String endpointTenant = extractTenantFromEndpoint(azureTokenEndpoint);
        return azureTenantId.equals(endpointTenant);
    }

    private static String extractTenantFromEndpoint(final String endpoint) {
        try {
            final String path = new URI(endpoint).getPath();
            if (path == null || path.isEmpty()) {
                return null;
            }
            final String withoutLeadingSlash = path.startsWith("/") ? path.substring(1) : path;
            final int nextSlash = withoutLeadingSlash.indexOf('/');
            final String tenant = nextSlash < 0 ? withoutLeadingSlash : withoutLeadingSlash.substring(0, nextSlash);
            return tenant.isEmpty() ? null : tenant;
        } catch (final URISyntaxException e) {
            return null;
        }
    }
}
