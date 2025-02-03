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
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Configuration;

import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.OAUTH2;

@Configuration
public class AtlassianAuthFactory implements FactoryBean<AtlassianAuthConfig> {

    private final AtlassianSourceConfig sourceConfig;

    public AtlassianAuthFactory(AtlassianSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public AtlassianAuthConfig getObject() {
        String authType = sourceConfig.getAuthType();
        if (OAUTH2.equals(authType)) {
            return new AtlassianOauthConfig(sourceConfig);
        }
        return new AtlassianBasicAuthConfig(sourceConfig);
    }

    @Override
    public Class<?> getObjectType() {
        return AtlassianAuthConfig.class;
    }
}
