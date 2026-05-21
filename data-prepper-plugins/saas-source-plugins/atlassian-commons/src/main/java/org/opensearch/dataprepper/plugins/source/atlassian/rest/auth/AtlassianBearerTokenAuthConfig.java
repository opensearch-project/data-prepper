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

import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;

import java.util.Objects;

public class AtlassianBearerTokenAuthConfig implements AtlassianAuthConfig {

    private final String accountUrl;
    private final PluginConfigVariable bearerTokenVariable;

    public AtlassianBearerTokenAuthConfig(AtlassianSourceConfig sourceConfig) {
        Objects.requireNonNull(sourceConfig, "sourceConfig must not be null");
        this.bearerTokenVariable = sourceConfig.getAuthenticationConfig().getBearerToken();
        Objects.requireNonNull(bearerTokenVariable, "bearer_token must not be null");
        String url = sourceConfig.getAccountUrl();
        if (!url.endsWith("/")) {
            url += "/";
        }
        this.accountUrl = url;
    }

    @Override
    public String getUrl() {
        return accountUrl;
    }

    public String getBearerToken() {
        final Object value = bearerTokenVariable.getValue();
        if (value instanceof String) {
            return (String) value;
        }
        return value != null ? value.toString() : null;
    }

    @Override
    public void renewCredentials() {
        if (bearerTokenVariable.isUpdatable()) {
            bearerTokenVariable.refresh();
        }
    }
}
