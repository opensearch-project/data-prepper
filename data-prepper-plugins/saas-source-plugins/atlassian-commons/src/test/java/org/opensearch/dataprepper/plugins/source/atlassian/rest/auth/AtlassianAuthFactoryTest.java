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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class AtlassianAuthFactoryTest {

    @Mock
    private AtlassianSourceConfig sourceConfig;

    @Mock
    private AuthenticationConfig authenticationConfig;

    @Mock
    private Oauth2Config oauth2Config;

    @Mock
    private PluginConfigVariable accessTokenPluginConfigVariable;

    @Mock
    private PluginConfigVariable refreshTokenPluginConfigVariable;

    private AtlassianAuthFactory confluenceAuthFactory;

    @BeforeEach
    void setUp() {
        confluenceAuthFactory = new AtlassianAuthFactory(sourceConfig);
    }

    @Test
    void testGetObjectOauth2() {
        when(sourceConfig.getAuthType()).thenReturn(OAUTH2);
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getOauth2Config()).thenReturn(oauth2Config);
        when(oauth2Config.getRefreshToken()).thenReturn(refreshTokenPluginConfigVariable);
        when(oauth2Config.getAccessToken()).thenReturn(accessTokenPluginConfigVariable);
        when(accessTokenPluginConfigVariable.getValue()).thenReturn("mockRefreshToken");
        assertInstanceOf(AtlassianOauthConfig.class, confluenceAuthFactory.getObject());
    }

    @Test
    void testGetObjectBasicAuth() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://example.com");
        assertInstanceOf(AtlassianBasicAuthConfig.class, confluenceAuthFactory.getObject());
    }

    @Test
    void testGetObjectType() {
        assertEquals(AtlassianAuthConfig.class, confluenceAuthFactory.getObjectType());
    }
}
