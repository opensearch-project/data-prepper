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

/**
 * Immutable options describing which Google credentials a plugin needs. Also used as the credentials cache key.
 */
public final class GcpCredentialsOptions {
    public static final String DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    public static final String DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token";

    private final GcpAuthMode authMode;
    private final String privateKey;
    private final String clientEmail;
    private final String tokenUri;
    private final List<String> scopes;
    private final String projectId;

    private GcpCredentialsOptions(final Builder builder) {
        this.authMode = builder.authMode;
        this.privateKey = builder.privateKey;
        this.clientEmail = builder.clientEmail;
        this.tokenUri = builder.tokenUri != null ? builder.tokenUri : DEFAULT_TOKEN_URI;
        this.scopes = builder.scopes == null || builder.scopes.isEmpty() ? List.of(DEFAULT_SCOPE) : List.copyOf(builder.scopes);
        this.projectId = builder.projectId;
        validate();
    }

    private void validate() {
        final boolean hasKeyFields = privateKey != null || clientEmail != null;
        if (authMode == GcpAuthMode.ADC && hasKeyFields) {
            throw new IllegalArgumentException("auth_mode adc cannot be combined with private_key or client_email");
        }
        if (authMode == null && hasKeyFields) {
            throw new IllegalArgumentException("private_key and client_email require auth_mode service_account_key");
        }
        if (authMode == GcpAuthMode.SERVICE_ACCOUNT_KEY && (privateKey == null || clientEmail == null)) {
            throw new IllegalArgumentException("auth_mode service_account_key requires private_key and client_email");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public GcpAuthMode getAuthMode() {
        return authMode;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public String getClientEmail() {
        return clientEmail;
    }

    public String getTokenUri() {
        return tokenUri;
    }

    public List<String> getScopes() {
        return scopes;
    }

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

        public Builder withAuthMode(final GcpAuthMode authMode) {
            this.authMode = authMode;
            return this;
        }

        public Builder withPrivateKey(final String privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        public Builder withClientEmail(final String clientEmail) {
            this.clientEmail = clientEmail;
            return this;
        }

        public Builder withTokenUri(final String tokenUri) {
            this.tokenUri = tokenUri;
            return this;
        }

        public Builder withScopes(final List<String> scopes) {
            this.scopes = scopes;
            return this;
        }

        public Builder withProjectId(final String projectId) {
            this.projectId = projectId;
            return this;
        }

        public GcpCredentialsOptions build() {
            return new GcpCredentialsOptions(this);
        }
    }
}
