/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.Oauth2Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class JiraAuthFactoryTest {

    @Mock
    private JiraSourceConfig sourceConfig;

    @Mock
    private AuthenticationConfig  authenticationConfig;

    @Mock
    private Oauth2Config oauth2Config;

    private JiraAuthFactory jiraAuthFactory;

    @BeforeEach
    void setUp() {
        jiraAuthFactory = new JiraAuthFactory(sourceConfig);
    }

    @Test
    void testGetObjectOauth2() {
        when(sourceConfig.getAuthType()).thenReturn(OAUTH2);
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getOauth2Config()).thenReturn(oauth2Config);
        assertInstanceOf(JiraOauthConfig.class, jiraAuthFactory.getObject());
    }

    @Test
    void testGetObjectBasicAuth() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://example.com");
        assertInstanceOf(JiraBasicAuthConfig.class, jiraAuthFactory.getObject());
    }

    @Test
    void testGetObjectType() {
        assertEquals(JiraAuthConfig.class, jiraAuthFactory.getObjectType());
    }
}
