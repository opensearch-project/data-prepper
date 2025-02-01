/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.confluence.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceItem;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.rest.ConfluenceRestClient;
import org.opensearch.dataprepper.plugins.source.confluence.utils.MockPluginConfigVariableImpl;
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
import static org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.LAST_MODIFIED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.SPACE;


/**
 * The type Jira service.
 */
@ExtendWith(MockitoExtension.class)
public class ConfluenceServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ConfluenceServiceTest.class);
    @Mock
    private ConfluenceRestClient confluenceRestClient;
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    private static InputStream getResourceAsStream(String resourceName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null) {
            inputStream = ConfluenceServiceTest.class.getResourceAsStream("/" + resourceName);
        }
        return inputStream;
    }

    public static ConfluenceSourceConfig createJiraConfigurationFromYaml(String fileName) {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        try (InputStream inputStream = getResourceAsStream(fileName)) {
            ConfluenceSourceConfig confluenceSourceConfig = objectMapper.readValue(inputStream, ConfluenceSourceConfig.class);
            Oauth2Config oauth2Config = confluenceSourceConfig.getAuthenticationConfig().getOauth2Config();
            if (oauth2Config != null) {
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "accessToken",
                        new MockPluginConfigVariableImpl("mockAccessToken"));
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "refreshToken",
                        new MockPluginConfigVariableImpl("mockRefreshToken"));
            }
            return confluenceSourceConfig;
        } catch (IOException ex) {
            log.error("Failed to parse pipeline Yaml", ex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static ConfluenceSourceConfig createJiraConfiguration(String auth_type,
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
        ConfluenceSourceConfig confluenceSourceConfig = objectMapper.readValue(jiraSourceConfigJsonString, ConfluenceSourceConfig.class);
        if (confluenceSourceConfig.getAuthenticationConfig().getOauth2Config() != null && pcvAccessToken != null) {
            try {
                ReflectivelySetField.setField(Oauth2Config.class,
                        confluenceSourceConfig.getAuthenticationConfig().getOauth2Config(), "accessToken", pcvAccessToken);
                ReflectivelySetField.setField(Oauth2Config.class,
                        confluenceSourceConfig.getAuthenticationConfig().getOauth2Config(), "refreshToken", pcvRefreshToken);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return confluenceSourceConfig;
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
        ConfluenceSourceConfig confluenceSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceService confluenceService = new ConfluenceService(confluenceSourceConfig, confluenceRestClient);
        assertNotNull(confluenceService);
        when(confluenceRestClient.getContent(anyString())).thenReturn("test String");
        assertNotNull(confluenceService.getContent("test Key"));
    }

    @Test
    public void testGetPages() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        issueStatus.add("Done");
        projectKey.add("KAN");
        ConfluenceSourceConfig confluenceSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient));
        List<ConfluenceItem> mockIssues = new ArrayList<>();
        ConfluenceItem issue1 = createConfluenceItemBean(false, false);
        mockIssues.add(issue1);
        ConfluenceItem issue2 = createConfluenceItemBean(true, false);
        mockIssues.add(issue2);
        ConfluenceItem issue3 = createConfluenceItemBean(false, true);
        mockIssues.add(issue3);

        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);

        doReturn(mockConfluenceSearchResults).when(confluenceRestClient).getAllContent(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue);
        assertEquals(mockIssues.size(), itemInfoQueue.size());
    }

    @Test
    public void buildIssueItemInfoMultipleFutureThreads() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        ConfluenceSourceConfig confluenceSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient));
        List<ConfluenceItem> mockIssues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            ConfluenceItem issue1 = createConfluenceItemBean(false, false);
            mockIssues.add(issue1);
        }

        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);


        doReturn(mockConfluenceSearchResults).when(confluenceRestClient).getAllContent(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue);
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

        ConfluenceSourceConfig confluenceSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceService confluenceService = new ConfluenceService(confluenceSourceConfig, confluenceRestClient);

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(BadRequestException.class, () -> confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue));
    }

    @Test
    public void testGetPagesException() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        ConfluenceSourceConfig confluenceSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient));

        doThrow(RuntimeException.class).when(confluenceRestClient).getAllContent(any(StringBuilder.class), anyInt());

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(RuntimeException.class, () -> confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue));
    }


    private ConfluenceItem createConfluenceItemBean(boolean nullFields, boolean createdNull) {
        ConfluenceItem issue1 = new ConfluenceItem();
        issue1.setId(UUID.randomUUID().toString());
        issue1.setTitle("issue_1_key");

        Map<String, Object> fieldMap = new HashMap<>();
        if (!nullFields) {
            fieldMap.put(CREATED, "2024-07-06T21:12:23.437-0700");
            fieldMap.put(LAST_MODIFIED, "2024-07-06T21:12:23.106-0700");
        } else {
            fieldMap.put(CREATED, 0);
            fieldMap.put(LAST_MODIFIED, 0);
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
        fieldMap.put(SPACE, projectMap);

        Map<String, Object> priorityMap = new HashMap<>();
        priorityMap.put(NAME, "Medium");
        fieldMap.put("priority", priorityMap);

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put(NAME, "In Progress");
        fieldMap.put("statuses", statusMap);

        return issue1;
    }

}