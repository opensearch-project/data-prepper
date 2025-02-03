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


import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AtlassianSourceConfig;

public class AtlassianBasicAuthConfig implements AtlassianAuthConfig {

    private String accountUrl;
    private final AtlassianSourceConfig confluenceSourceConfig;

    public AtlassianBasicAuthConfig(AtlassianSourceConfig confluenceSourceConfig) {
        this.confluenceSourceConfig = confluenceSourceConfig;
        accountUrl = confluenceSourceConfig.getAccountUrl();
        if (!accountUrl.endsWith("/")) {
            accountUrl += "/";
        }
    }

    @Override
    public String getUrl() {
        return accountUrl;
    }

    @Override
    public void initCredentials() {
        //do nothing for basic authentication
    }

    @Override
    public void renewCredentials() {
        //do nothing for basic authentication
    }


}
