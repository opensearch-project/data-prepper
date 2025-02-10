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
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AtlassianBasicAuthConfigTest {

    String url = "https://example.com";
    @Mock
    private AtlassianSourceConfig confluenceSourceConfig;
    private AtlassianBasicAuthConfig jiraBasicAuthConfig;

    @BeforeEach
    void setUp() {
        when(confluenceSourceConfig.getAccountUrl()).thenReturn(url);
        jiraBasicAuthConfig = new AtlassianBasicAuthConfig(confluenceSourceConfig);
    }

    @Test
    void testGetUrl() {
        assertEquals(jiraBasicAuthConfig.getUrl(), url + '/');

    }

    @Test
    void DoNothingForBasicAuthentication() {
        jiraBasicAuthConfig.initCredentials();
        jiraBasicAuthConfig.renewCredentials();
    }
}