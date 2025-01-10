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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JiraBasicAuthConfigTest {

    @Mock
    private JiraSourceConfig jiraSourceConfig;

    private JiraBasicAuthConfig jiraBasicAuthConfig;
    String url = "https://example.com";

    @BeforeEach
    void setUp() {
        when(jiraSourceConfig.getAccountUrl()).thenReturn(url);
        jiraBasicAuthConfig = new JiraBasicAuthConfig(jiraSourceConfig);
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