/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest.auth;

import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;

public class AtlassianBearerTokenAuthConfig implements AtlassianAuthConfig {

    private String accountUrl;
    private final String bearerToken;

    public AtlassianBearerTokenAuthConfig(AtlassianSourceConfig sourceConfig) {
        this.bearerToken = sourceConfig.getAuthenticationConfig().getBearerToken();
        accountUrl = sourceConfig.getAccountUrl();
        if (!accountUrl.endsWith("/")) {
            accountUrl += "/";
        }
    }

    @Override
    public String getUrl() {
        return accountUrl;
    }

    public String getBearerToken() {
        return bearerToken;
    }

    @Override
    public void renewCredentials() {
        // static token, no renewal needed
    }
}
