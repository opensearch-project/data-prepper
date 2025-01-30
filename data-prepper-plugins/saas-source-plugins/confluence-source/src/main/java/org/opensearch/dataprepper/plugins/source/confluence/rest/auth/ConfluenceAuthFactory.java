/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.rest.auth;

import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceSourceConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Configuration;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

@Configuration
public class ConfluenceAuthFactory implements FactoryBean<ConfluenceAuthConfig> {

    private final ConfluenceSourceConfig sourceConfig;

    public ConfluenceAuthFactory(ConfluenceSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public ConfluenceAuthConfig getObject() {
        String authType = sourceConfig.getAuthType();
        if (OAUTH2.equals(authType)) {
            return new ConfluenceOauthConfig(sourceConfig);
        }
        return new ConfluenceBasicAuthConfig(sourceConfig);
    }

    @Override
    public Class<?> getObjectType() {
        return ConfluenceAuthConfig.class;
    }
}
