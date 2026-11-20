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
     * @return false when auth_mode adc is combined with private_key, client_email or token_uri
     */
    @AssertTrue(message = GcpCredentialsRules.ADC_WITH_KEY_FIELDS)
    public boolean isValidAdcConfiguration() {
        return GcpCredentialsRules.isValidAdc(authMode, privateKey, clientEmail, tokenUri);
    }

    /**
     * @return false when private_key, client_email or token_uri is set without an auth_mode
     */
    @AssertTrue(message = GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE)
    public boolean isValidKeyFieldsConfiguration() {
        return GcpCredentialsRules.isValidWithoutMode(authMode, privateKey, clientEmail, tokenUri);
    }

    /**
     * @return false when auth_mode service_account_key is missing a non-blank private_key or client_email
     */
    @AssertTrue(message = GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY)
    public boolean isValidServiceAccountKeyConfiguration() {
        return GcpCredentialsRules.isCompleteServiceAccountKey(authMode, privateKey, clientEmail);
    }

    /**
     * @return false when scopes contains a null or blank value
     */
    @AssertTrue(message = GcpCredentialsRules.INVALID_SCOPES)
    public boolean isValidScopesConfiguration() {
        return GcpCredentialsRules.isValidScopes(scopes);
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
}
