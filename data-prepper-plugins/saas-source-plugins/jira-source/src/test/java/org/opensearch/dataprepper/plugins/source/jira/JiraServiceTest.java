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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.jira.utils.MockPluginConfigVariableImpl;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;


/**
 * The type Jira service.
 */
@ExtendWith(MockitoExtension.class)
public class JiraServiceTest {

    private static final Logger log = LoggerFactory.getLogger(JiraServiceTest.class);
    @Mock
    private JiraRestClient jiraRestClient;
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("JiraServiceTest", "jira");

    private static InputStream getResourceAsStream(String resourceName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null) {
            inputStream = JiraServiceTest.class.getResourceAsStream("/" + resourceName);
        }
        return inputStream;
    }

    public static JiraSourceConfig createJiraConfigurationFromYaml(String fileName) {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        try (InputStream inputStream = getResourceAsStream(fileName)) {
            JiraSourceConfig jiraSourceConfig = objectMapper.readValue(inputStream, JiraSourceConfig.class);
            Oauth2Config oauth2Config = jiraSourceConfig.getAuthenticationConfig().getOauth2Config();
            if (oauth2Config != null) {
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "accessToken",
                        new MockPluginConfigVariableImpl("mockAccessToken"));
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "refreshToken",
                        new MockPluginConfigVariableImpl("mockRefreshToken"));
            }
            return jiraSourceConfig;
        } catch (IOException ex) {
            log.error("Failed to parse pipeline Yaml", ex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static JiraSourceConfig createJiraConfiguration(String auth_type,
                                                           List<String> issueType,
                                                           List<String> issueStatus,
                                                           List<String> projectKey) throws JsonProcessingException {
        PluginConfigVariable pcvAccessToken = null;
        PluginConfigVariable pcvRefreshToken = null;
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> authenticationMap = new HashMap<>();
        Map<String, String> basicMap = new HashMap<>();
        Map<String, String> oauth2Map = new HashMap<>();
        if (auth_type.equals(BASIC)) {
            basicMap.put("username", "test_username");
            basicMap.put("password", "test_password");
            authenticationMap.put("basic", basicMap);
        } else if (auth_type.equals(OAUTH2)) {
            oauth2Map.put("client_id", "test-client-id");
            oauth2Map.put("client_secret", "test-client-secret");
            pcvAccessToken = new MockPluginConfigVariableImpl("test-access-token");
            pcvRefreshToken = new MockPluginConfigVariableImpl("test-refresh-token");
            authenticationMap.put("oauth2", oauth2Map);
        }

        Map<String, Object> jiraSourceConfigMap = new HashMap<>();
        List<String> hosts = new ArrayList<>();
        hosts.add(ACCESSIBLE_RESOURCES);

        Map<String, Object> filterMap = new HashMap<>();
        Map<String, Object> projectMap = new HashMap<>();
        Map<String, Object> issueTypeMap = new HashMap<>();
        Map<String, Object> statusMap = new HashMap<>();

        issueTypeMap.put("include", issueType);
        filterMap.put("issue_type", issueTypeMap);

        statusMap.put("include", issueStatus);
        filterMap.put("status", statusMap);

        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("include", projectKey);
        projectMap.put("key", nameMap);
        filterMap.put("project", projectMap);


        jiraSourceConfigMap.put("hosts", hosts);
        jiraSourceConfigMap.put("authentication", authenticationMap);
        jiraSourceConfigMap.put("filter", filterMap);

        String jiraSourceConfigJsonString = objectMapper.writeValueAsString(jiraSourceConfigMap);
        JiraSourceConfig jiraSourceConfig = objectMapper.readValue(jiraSourceConfigJsonString, JiraSourceConfig.class);
        if (jiraSourceConfig.getAuthenticationConfig().getOauth2Config() != null && pcvAccessToken != null) {
            try {
                ReflectivelySetField.setField(Oauth2Config.class,
                        jiraSourceConfig.getAuthenticationConfig().getOauth2Config(), "accessToken", pcvAccessToken);
                ReflectivelySetField.setField(Oauth2Config.class,
                        jiraSourceConfig.getAuthenticationConfig().getOauth2Config(), "refreshToken", pcvRefreshToken);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return jiraSourceConfig;
    }

    @AfterEach
    void tearDown() {
        executorServiceProvider.terminateExecutor();
    }

    @Test
    void testJiraServiceInitialization() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics);
        assertNotNull(jiraService);
        when(jiraRestClient.getIssue(anyString())).thenReturn("test String");
        assertNotNull(jiraService.getIssue("test Key"));
    }

    @Test
    public void testGetJiraEntities() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        issueStatus.add("Done");
        projectKey.add("KAN");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics));
        List<IssueBean> mockIssues = new ArrayList<>();
        IssueBean issue1 = createIssueBean(false, false);
        mockIssues.add(issue1);
        IssueBean issue2 = createIssueBean(true, false);
        mockIssues.add(issue2);
        IssueBean issue3 = createIssueBean(false, true);
        mockIssues.add(issue3);

        SearchResults mockSearchResults = mock(SearchResults.class);
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(mockIssues.size());

        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue);
        assertEquals(mockIssues.size(), itemInfoQueue.size());
    }

    @Test
    public void buildIssueItemInfoMultipleFutureThreads() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics));
        List<IssueBean> mockIssues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            IssueBean issue1 = createIssueBean(false, false);
            mockIssues.add(issue1);
        }

        SearchResults mockSearchResults = mock(SearchResults.class);
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(100);

        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue);
        assertTrue(itemInfoQueue.size() >= 100);
    }

    @Test
    public void testBadProjectKeys() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        issueStatus.add("Done");
        projectKey.add("Bad Project Key");
        projectKey.add("A");
        projectKey.add("!@#$");
        projectKey.add("AAAAAAAAAAAAAA");

        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics);

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(InvalidPluginConfigurationException.class,
                () -> jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue));
    }

    @Test
    public void testGetJiraEntitiesException() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics));

        doThrow(RuntimeException.class).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(RuntimeException.class, () -> jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue));
    }


    private IssueBean createIssueBean(boolean nullFields, boolean createdNull) {
        IssueBean issue1 = new IssueBean();
        issue1.setId(UUID.randomUUID().toString());
        issue1.setKey("issue_1_key");
        issue1.setSelf("https://example.com/rest/api/2/issue/123");
        issue1.setExpand("operations,versionedRepresentations,editmeta");

        Map<String, Object> fieldMap = new HashMap<>();
        if (!nullFields) {
            fieldMap.put(CREATED, "2024-07-06T21:12:23.437-0700");
            fieldMap.put(UPDATED, "2024-07-06T21:12:23.106-0700");
        } else {
            fieldMap.put(CREATED, 0);
            fieldMap.put(UPDATED, 0);
        }
        if (createdNull) {
            fieldMap.put(CREATED, null);
        }

        Map<String, Object> issueTypeMap = new HashMap<>();
        issueTypeMap.put("name", "Task");
        issueTypeMap.put("self", "https://example.com/rest/api/2/issuetype/1");
        issueTypeMap.put("id", "1");
        fieldMap.put("issuetype", issueTypeMap);

        Map<String, Object> projectMap = new HashMap<>();
        if (!nullFields) {
            projectMap.put(NAME, "project name test");
            projectMap.put(KEY, "TEST");
        }
        fieldMap.put(PROJECT, projectMap);

        Map<String, Object> priorityMap = new HashMap<>();
        priorityMap.put(NAME, "Medium");
        fieldMap.put("priority", priorityMap);

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put(NAME, "In Progress");
        fieldMap.put("statuses", statusMap);

        issue1.setFields(fieldMap);

        return issue1;
    }

    @Test
    public void testCreateContentFilterCriteria() throws JsonProcessingException {
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, List.of(), List.of(), List.of());
        JiraService jiraService = new JiraService(jiraSourceConfig, jiraRestClient, pluginMetrics);
        Instant pollingTime = Instant.now();
        StringBuilder contentFilterCriteria = jiraService.createIssueFilterCriteria(jiraSourceConfig, pollingTime);
        assertNotNull(contentFilterCriteria);
        String cqlToAssert = "updated>" + pollingTime.toEpochMilli() + " order by updated asc ";
        assertEquals(cqlToAssert, contentFilterCriteria.toString());
    }

}