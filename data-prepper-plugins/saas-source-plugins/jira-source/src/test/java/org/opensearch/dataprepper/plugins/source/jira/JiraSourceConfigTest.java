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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.jira.utils.MockPluginConfigVariableImpl;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

public class JiraSourceConfigTest {
    private JiraSourceConfig jiraSourceConfig;
    private final PluginConfigVariable accessToken = new MockPluginConfigVariableImpl("access token test");
    private final PluginConfigVariable refreshToken = new MockPluginConfigVariableImpl("refresh token test");
    private final String clientId = "client id test";
    private final String clientSecret = "client secret test";
    private final String password = "test Jira Credential";
    private final String username = "test Jira Id";
    private final String accountUrl = "https://example.atlassian.net";
    private final List<String> projectList = new ArrayList<>();
    private final List<String> issueTypeList = new ArrayList<>();
    private final List<String> statusList = new ArrayList<>();

    private JiraSourceConfig createJiraSourceConfig(String authtype, boolean hasToken) throws Exception {
        PluginConfigVariable pcvAccessToken = null;
        PluginConfigVariable pcvRefreshToken = null;
        Map<String, Object> configMap = new HashMap<>();
        List<String> hosts = new ArrayList<>();
        hosts.add(accountUrl);

        configMap.put("hosts", hosts);

        Map<String, Object> authenticationMap = new HashMap<>();
        Map<String, String> basicMap = new HashMap<>();
        Map<String, Object> oauth2Map = new HashMap<>();
        if (authtype.equals(BASIC)) {
            basicMap.put("username", username);
            basicMap.put("password", password);
            authenticationMap.put("basic", basicMap);
        } else if (authtype.equals(OAUTH2)) {
            if (hasToken) {
                pcvRefreshToken = refreshToken;
                pcvAccessToken = accessToken;
            } else {
                oauth2Map.put("refresh_token", null);
            }
            oauth2Map.put("client_id", clientId);
            oauth2Map.put("client_secret", clientSecret);
            authenticationMap.put("oauth2", oauth2Map);
        }

        configMap.put("authentication", authenticationMap);

        projectList.add("project1");
        projectList.add("project2");

        issueTypeList.add("issue type 1");
        issueTypeList.add("issue type 2");

        statusList.add("status 1");
        statusList.add("status 2");

        Map<String, Object> filterMap = new HashMap<>();
        Map<String, Object> projectMap = new HashMap<>();
        Map<String, Object> issueTypeMap = new HashMap<>();
        Map<String, Object> statusMap = new HashMap<>();

        issueTypeMap.put("include", issueTypeList);
        filterMap.put("issue_type", issueTypeMap);

        statusMap.put("include", statusList);
        filterMap.put("status", statusMap);

        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("include", projectList);
        projectMap.put("key", nameMap);
        filterMap.put("project", projectMap);

        configMap.put("filter", filterMap);

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfig = objectMapper.writeValueAsString(configMap);
        JiraSourceConfig config = objectMapper.readValue(jsonConfig, JiraSourceConfig.class);
        if (config.getAuthenticationConfig().getOauth2Config() != null && pcvAccessToken != null) {
            ReflectivelySetField.setField(Oauth2Config.class,
                    config.getAuthenticationConfig().getOauth2Config(), "accessToken", pcvAccessToken);
            ReflectivelySetField.setField(Oauth2Config.class,
                    config.getAuthenticationConfig().getOauth2Config(), "refreshToken", pcvRefreshToken);
        }
        return config;
    }

    @Test
    void testGetters() throws Exception {
        jiraSourceConfig = createJiraSourceConfig(BASIC, false);
        assertEquals(jiraSourceConfig.getFilterConfig().getIssueTypeConfig().getInclude(), issueTypeList);
        assertEquals(jiraSourceConfig.getFilterConfig().getProjectConfig().getNameConfig().getInclude(), projectList);
        assertEquals(jiraSourceConfig.getFilterConfig().getStatusConfig().getInclude(), statusList);
        assertEquals(jiraSourceConfig.getAccountUrl(), accountUrl);
        assertEquals(jiraSourceConfig.getAuthenticationConfig().getBasicConfig().getPassword(), password);
        assertEquals(jiraSourceConfig.getAuthenticationConfig().getBasicConfig().getUsername(), username);
    }

    @Test
    void testFetchGivenOauthAttributeWrongAuthType() throws Exception {
        jiraSourceConfig = createJiraSourceConfig(BASIC, true);
        assertThrows(RuntimeException.class, () -> jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
    }

    @Test
    void testFetchGivenOauthAtrribute() throws Exception {
        jiraSourceConfig = createJiraSourceConfig(OAUTH2, true);
        assertEquals(accessToken, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
        assertEquals(refreshToken, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getRefreshToken());
        assertEquals(clientId, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientId());
        assertEquals(clientSecret, jiraSourceConfig.getAuthenticationConfig().getOauth2Config().getClientSecret());
    }

    @Test
    void testGetOauth2UrlContext() throws Exception {
        jiraSourceConfig = createJiraSourceConfig(OAUTH2, false);
        assertEquals("jira", jiraSourceConfig.getOauth2UrlContext());
    }
}
