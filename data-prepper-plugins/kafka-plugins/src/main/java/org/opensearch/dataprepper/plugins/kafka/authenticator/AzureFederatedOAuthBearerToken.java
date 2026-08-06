/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Set;

public class AzureFederatedOAuthBearerToken implements OAuthBearerToken {
    private final String value;
    private final long lifetimeMs;
    private final long startTimeMs;
    private final String scope;
    private final String principalName;

    public AzureFederatedOAuthBearerToken(final String value, final long expiresInSeconds,
                                          final String scope, final String principalName) {
        this.value = value;
        this.startTimeMs = System.currentTimeMillis();
        this.lifetimeMs = this.startTimeMs + (expiresInSeconds * 1000L);
        this.scope = scope;
        this.principalName = principalName;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Set<String> scope() {
        return scope == null ? Set.of() : Set.of(scope);
    }
}
