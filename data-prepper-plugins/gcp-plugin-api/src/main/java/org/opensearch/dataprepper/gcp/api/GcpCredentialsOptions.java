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

import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Immutable options describing which Google credentials a plugin needs. Also used as the credentials cache key.
 */
public final class GcpCredentialsOptions {
    /**
     * The OAuth2 scope used when no scopes are configured.
     */
    public static final String DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    /**
     * The OAuth2 token endpoint used for service-account keys when no token URI is configured.
     */
    public static final String DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token";

    private final GcpAuthMode authMode;
    private final String privateKey;
    private final String clientEmail;
    private final String configuredTokenUri;
    private final List<String> configuredScopes;
    private final String tokenUri;
    private final List<String> scopes;
    private final String projectId;

    private GcpCredentialsOptions(final Builder builder) {
        this.authMode = builder.authMode;
        this.privateKey = builder.privateKey;
        this.clientEmail = builder.clientEmail;
        this.configuredTokenUri = builder.tokenUri;
        this.configuredScopes = builder.scopes == null || builder.scopes.isEmpty()
                ? null : List.copyOf(new TreeSet<>(builder.scopes));
        this.tokenUri = configuredTokenUri != null ? configuredTokenUri : DEFAULT_TOKEN_URI;
        this.scopes = configuredScopes != null ? configuredScopes : List.of(DEFAULT_SCOPE);
        this.projectId = builder.projectId;
        validate();
    }

    private void validate() {
        if (!GcpCredentialsRules.isValidAdc(authMode, privateKey, clientEmail, configuredTokenUri)) {
            throw new IllegalArgumentException(GcpCredentialsRules.ADC_WITH_KEY_FIELDS);
        }
        if (!GcpCredentialsRules.isValidWithoutMode(authMode, privateKey, clientEmail, configuredTokenUri)) {
            throw new IllegalArgumentException(GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE);
        }
        if (!GcpCredentialsRules.isCompleteServiceAccountKey(authMode, privateKey, clientEmail)) {
            throw new IllegalArgumentException(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY);
        }
    }

    /**
     * Combines these options with the credentials identity of another set of options, typically the extension
     * default. The result uses the identity's auth mode, private key, client email and token URI, and keeps these
     * options' scopes and project when they were set, falling back to the identity's otherwise.
     *
     * @param identity the options that supply the credentials identity
     * @return the combined options
     */
    public GcpCredentialsOptions withIdentityFrom(final GcpCredentialsOptions identity) {
        return builder()
                .withAuthMode(identity.authMode)
                .withPrivateKey(identity.privateKey)
                .withClientEmail(identity.clientEmail)
                .withTokenUri(identity.configuredTokenUri)
                .withScopes(configuredScopes != null ? configuredScopes : identity.configuredScopes)
                .withProjectId(projectId != null ? projectId : identity.projectId)
                .build();
    }

    /**
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return the auth mode, or null when these options do not choose credentials
     */
    public GcpAuthMode getAuthMode() {
        return authMode;
    }

    /**
     * @return the PKCS#8 PEM private key for the service-account key mode, or null
     */
    public String getPrivateKey() {
        return privateKey;
    }

    /**
     * @return the service account email for the service-account key mode, or null
     */
    public String getClientEmail() {
        return clientEmail;
    }

    /**
     * @return the OAuth2 token endpoint; defaults to {@link #DEFAULT_TOKEN_URI}
     */
    public String getTokenUri() {
        return tokenUri;
    }

    /**
     * @return the OAuth2 scopes, sorted and without duplicates; defaults to {@link #DEFAULT_SCOPE}
     */
    public List<String> getScopes() {
        return scopes;
    }

    /**
     * @return the GCP project, or null
     */
    public String getProjectId() {
        return projectId;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GcpCredentialsOptions)) {
            return false;
        }
        final GcpCredentialsOptions that = (GcpCredentialsOptions) other;
        return authMode == that.authMode
                && Objects.equals(privateKey, that.privateKey)
                && Objects.equals(clientEmail, that.clientEmail)
                && tokenUri.equals(that.tokenUri)
                && scopes.equals(that.scopes)
                && Objects.equals(projectId, that.projectId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authMode, privateKey, clientEmail, tokenUri, scopes, projectId);
    }

    @Override
    public String toString() {
        return "GcpCredentialsOptions{authMode=" + authMode + ", clientEmail=" + clientEmail + ", tokenUri=" + tokenUri
                + ", scopes=" + scopes + ", projectId=" + projectId + "}";
    }

    /**
     * Builds {@link GcpCredentialsOptions}.
     */
    public static final class Builder {
        private GcpAuthMode authMode;
        private String privateKey;
        private String clientEmail;
        private String tokenUri;
        private List<String> scopes;
        private String projectId;

        private Builder() {
        }

        /**
         * @param authMode the auth mode; null leaves the choice to the extension default or Application Default Credentials
         * @return this builder
         */
        public Builder withAuthMode(final GcpAuthMode authMode) {
            this.authMode = authMode;
            return this;
        }

        /**
         * @param privateKey the PKCS#8 PEM private key; requires {@link GcpAuthMode#SERVICE_ACCOUNT_KEY}
         * @return this builder
         */
        public Builder withPrivateKey(final String privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        /**
         * @param clientEmail the service account email; requires {@link GcpAuthMode#SERVICE_ACCOUNT_KEY}
         * @return this builder
         */
        public Builder withClientEmail(final String clientEmail) {
            this.clientEmail = clientEmail;
            return this;
        }

        /**
         * @param tokenUri the OAuth2 token endpoint; null uses {@link #DEFAULT_TOKEN_URI}
         * @return this builder
         */
        public Builder withTokenUri(final String tokenUri) {
            this.tokenUri = tokenUri;
            return this;
        }

        /**
         * @param scopes the OAuth2 scopes; null or empty uses {@link #DEFAULT_SCOPE}
         * @return this builder
         */
        public Builder withScopes(final List<String> scopes) {
            this.scopes = scopes;
            return this;
        }

        /**
         * @param projectId the GCP project
         * @return this builder
         */
        public Builder withProjectId(final String projectId) {
            this.projectId = projectId;
            return this;
        }

        /**
         * @return the options
         * @throws IllegalArgumentException if the auth mode and key fields are inconsistent
         */
        public GcpCredentialsOptions build() {
            return new GcpCredentialsOptions(this);
        }
    }
}
