/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.BasicConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.FilterConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.IssueTypeConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.NameConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.jira.configuration.ProjectConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.StatusConfig;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
public class JiraConfigHelperTest {

    @Mock
    JiraSourceConfig jiraSourceConfig;

    @Mock
    FilterConfig filterConfig;

    @Mock
    StatusConfig statusConfig;

    @Mock
    IssueTypeConfig issueTypeConfig;

    @Mock
    ProjectConfig projectConfig;

    @Mock
    NameConfig nameConfig;

    @Mock
    AuthenticationConfig authenticationConfig;

    @Mock
    BasicConfig basicConfig;

    @Mock
    Oauth2Config oauth2Config;

    @Mock
    PluginConfigVariable pluginConfigVariable;

    @Test
    void testInitialization() {
        JiraConfigHelper jiraConfigHelper = new JiraConfigHelper();
        assertNotNull(jiraConfigHelper);
    }

    @Test
    void testGetIssueStatusFilter() {
        when(jiraSourceConfig.getFilterConfig()).thenReturn(filterConfig);
        when(filterConfig.getStatusConfig()).thenReturn(statusConfig);
        assertTrue(JiraConfigHelper.getIssueStatusIncludeFilter(jiraSourceConfig).isEmpty());
        assertTrue(JiraConfigHelper.getIssueStatusExcludeFilter(jiraSourceConfig).isEmpty());
        List<String> issueStatusFilter = List.of("Done", "In Progress");
        List<String> issueStatusExcludeFilter = List.of("Done2", "In Progress2");
        when(statusConfig.getInclude()).thenReturn(issueStatusFilter);
        when(statusConfig.getExclude()).thenReturn(issueStatusExcludeFilter);
        assertEquals(issueStatusFilter, JiraConfigHelper.getIssueStatusIncludeFilter(jiraSourceConfig));
        assertEquals(issueStatusExcludeFilter, JiraConfigHelper.getIssueStatusExcludeFilter(jiraSourceConfig));
    }

    @Test
    void testGetIssueTypeFilter() {
        when(jiraSourceConfig.getFilterConfig()).thenReturn(filterConfig);
        when(filterConfig.getIssueTypeConfig()).thenReturn(issueTypeConfig);
        assertTrue(JiraConfigHelper.getIssueTypeIncludeFilter(jiraSourceConfig).isEmpty());
        assertTrue(JiraConfigHelper.getIssueTypeExcludeFilter(jiraSourceConfig).isEmpty());
        List<String> issueTypeFilter = List.of("Bug", "Story");
        List<String> issueTypeExcludeFilter = List.of("Bug2", "Story2");
        when(issueTypeConfig.getInclude()).thenReturn(issueTypeFilter);
        when(issueTypeConfig.getExclude()).thenReturn(issueTypeExcludeFilter);
        assertEquals(issueTypeFilter, JiraConfigHelper.getIssueTypeIncludeFilter(jiraSourceConfig));
        assertEquals(issueTypeExcludeFilter, JiraConfigHelper.getIssueTypeExcludeFilter(jiraSourceConfig));
    }

    @Test
    void testGetProjectNameFilter() {
        when(jiraSourceConfig.getFilterConfig()).thenReturn(filterConfig);
        when(filterConfig.getProjectConfig()).thenReturn(projectConfig);
        when(projectConfig.getNameConfig()).thenReturn(nameConfig);
        assertTrue(JiraConfigHelper.getProjectNameIncludeFilter(jiraSourceConfig).isEmpty());
        assertTrue(JiraConfigHelper.getProjectNameExcludeFilter(jiraSourceConfig).isEmpty());
        List<String> projectNameFilter = List.of("TEST", "TEST2");
        List<String> projectNameExcludeFilter = List.of("TEST3", "TEST4");
        when(nameConfig.getInclude()).thenReturn(projectNameFilter);
        when(nameConfig.getExclude()).thenReturn(projectNameExcludeFilter);
        assertEquals(projectNameFilter, JiraConfigHelper.getProjectNameIncludeFilter(jiraSourceConfig));
        assertEquals(projectNameExcludeFilter, JiraConfigHelper.getProjectNameExcludeFilter(jiraSourceConfig));
    }


    @Test
    void testValidateConfig() {
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(jiraSourceConfig.getAuthType()).thenReturn("fakeType");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }

    @Test
    void testValidateConfigBasic() {
        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(jiraSourceConfig.getAuthType()).thenReturn(BASIC);
        when(jiraSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBasicConfig()).thenReturn(basicConfig);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(basicConfig.getUsername()).thenReturn("id");
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(basicConfig.getPassword()).thenReturn("credential");
        when(basicConfig.getUsername()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(basicConfig.getUsername()).thenReturn("id");
        assertDoesNotThrow(() -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }

    @Test
    void testValidateConfigOauth2() {
        when(jiraSourceConfig.getAccountUrl()).thenReturn("https://test.com");
        when(jiraSourceConfig.getAuthType()).thenReturn(OAUTH2);
        when(jiraSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getOauth2Config()).thenReturn(oauth2Config);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(oauth2Config.getAccessToken()).thenReturn(pluginConfigVariable);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(authenticationConfig.getOauth2Config().getRefreshToken()).thenReturn("refreshToken");
        when(oauth2Config.getAccessToken()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> JiraConfigHelper.validateConfig(jiraSourceConfig));

        when(oauth2Config.getAccessToken()).thenReturn(pluginConfigVariable);
        assertDoesNotThrow(() -> JiraConfigHelper.validateConfig(jiraSourceConfig));
    }
}
