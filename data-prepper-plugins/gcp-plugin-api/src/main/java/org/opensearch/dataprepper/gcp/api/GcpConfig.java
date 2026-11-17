/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * The shared {@code gcp:} configuration block for Data Prepper plugins that use Google Cloud.
 */
public class GcpConfig {
    @JsonProperty("project_id")
    @Size(min = 1, message = "project_id cannot be empty")
    private String projectId;

    @JsonProperty("auth_mode")
    private GcpAuthMode authMode;

    @JsonProperty("private_key")
    private String privateKey;

    @JsonProperty("client_email")
    private String clientEmail;

    @JsonProperty("token_uri")
    private String tokenUri;

    @JsonProperty("scopes")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<String> scopes;

    /**
     * @return the GCP project, or null
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * @return the auth mode, or null when this block does not choose credentials
     */
    public GcpAuthMode getAuthMode() {
        return authMode;
    }

    /**
     * @return the service account email, or null
     */
    public String getClientEmail() {
        return clientEmail;
    }

    /**
     * @return the configured OAuth2 token endpoint, or null for the default
     */
    public String getTokenUri() {
        return tokenUri;
    }

    /**
     * @return the configured OAuth2 scopes, or null for the default
     */
    public List<String> getScopes() {
        return scopes;
    }

    /**
     * @return false when auth_mode adc is combined with private_key or client_email
     */
    @AssertTrue(message = "auth_mode adc cannot be combined with private_key or client_email")
    public boolean isValidAdcConfiguration() {
        return authMode != GcpAuthMode.ADC || !hasKeyFields();
    }

    /**
     * @return false when private_key or client_email is set without an auth_mode
     */
    @AssertTrue(message = "private_key and client_email require auth_mode service_account_key")
    public boolean isValidKeyFieldsConfiguration() {
        return authMode != null || !hasKeyFields();
    }

    /**
     * @return false when auth_mode service_account_key is missing private_key or client_email
     */
    @AssertTrue(message = "auth_mode service_account_key requires private_key and client_email")
    public boolean isValidServiceAccountKeyConfiguration() {
        return authMode != GcpAuthMode.SERVICE_ACCOUNT_KEY || (privateKey != null && clientEmail != null);
    }

    /**
     * @return the credentials options described by this block
     */
    public GcpCredentialsOptions toCredentialsOptions() {
        return GcpCredentialsOptions.builder()
                .withAuthMode(authMode)
                .withPrivateKey(privateKey)
                .withClientEmail(clientEmail)
                .withTokenUri(tokenUri)
                .withScopes(scopes)
                .withProjectId(projectId)
                .build();
    }

    private boolean hasKeyFields() {
        return privateKey != null || clientEmail != null;
    }
}
