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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceItem;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluencePaginationLinks;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceServerMetadata;
import org.opensearch.dataprepper.plugins.source.confluence.models.SpaceItem;
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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.confluence.ConfluenceService.CQL_LAST_MODIFIED_DATE_FORMAT;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;


/**
 * The type Confluence service Test.
 */
@ExtendWith(MockitoExtension.class)
public class ConfluenceServiceTest {

    private static final Logger log = LoggerFactory.getLogger(ConfluenceServiceTest.class);
    @Mock
    private ConfluenceRestClient confluenceRestClient;
    @Mock
    private ConfluenceServerMetadata confluenceServerMetadata;
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("confluenceService", "aws");

    private static InputStream getResourceAsStream(String resourceName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null) {
            inputStream = ConfluenceServiceTest.class.getResourceAsStream("/" + resourceName);
        }
        return inputStream;
    }

    public static ConfluenceSourceConfig createConfluenceConfigurationFromYaml(String fileName) {
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

    public static ConfluenceSourceConfig createConfluenceConfiguration(String auth_type,
                                                                       List<String> pageTypes,
                                                                       List<String> spaceKey) throws JsonProcessingException {
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

        Map<String, Object> sourceConfigMap = new HashMap<>();
        List<String> hosts = new ArrayList<>();
        hosts.add(ACCESSIBLE_RESOURCES);

        Map<String, Object> filterMap = new HashMap<>();
        Map<String, Object> spacesMap = new HashMap<>();
        Map<String, Object> contentTypeMap = new HashMap<>();

        contentTypeMap.put("include", pageTypes);
        filterMap.put("page_type", contentTypeMap);

        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("include", spaceKey);
        spacesMap.put("key", nameMap);
        filterMap.put("space", spacesMap);


        sourceConfigMap.put("hosts", hosts);
        sourceConfigMap.put("authentication", authenticationMap);
        sourceConfigMap.put("filter", filterMap);

        String sourceConfigJsonString = objectMapper.writeValueAsString(sourceConfigMap);
        ConfluenceSourceConfig confluenceSourceConfig = objectMapper.readValue(sourceConfigJsonString, ConfluenceSourceConfig.class);
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
    void testConfluenceServiceInitialization() throws JsonProcessingException {
        List<String> contentType = new ArrayList<>();
        List<String> spacesKey = new ArrayList<>();
        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, contentType, spacesKey);
        ConfluenceService confluenceService = new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics);
        assertNotNull(confluenceService);
        when(confluenceRestClient.getContent(anyString())).thenReturn("test String");
        assertNotNull(confluenceService.getContent("test Key"));
    }

    @Test
    public void testGetPages() throws JsonProcessingException {
        List<String> contentType = new ArrayList<>();
        List<String> spaceKey = new ArrayList<>();
        contentType.add("PAGE");
        spaceKey.add("KAN");
        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, contentType, spaceKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics));
        List<ConfluenceItem> mockPages = new ArrayList<>();
        ConfluenceItem item1 = createConfluenceItemBean();
        mockPages.add(item1);
        ConfluenceItem item2 = createConfluenceItemBean();
        mockPages.add(item2);
        ConfluenceItem item3 = createConfluenceItemBean();
        mockPages.add(item3);
        ConfluencePaginationLinks paginationLinks = mock(ConfluencePaginationLinks.class);

        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);
        when(mockConfluenceSearchResults.getResults()).thenReturn(mockPages);
        when(mockConfluenceSearchResults.getLinks()).thenReturn(paginationLinks);
        //End the pagination.
        when(paginationLinks.getNext()).thenReturn(null);

        doReturn(mockConfluenceSearchResults).when(confluenceRestClient).getAllContent(any(StringBuilder.class), anyInt(), any());
        doReturn(confluenceServerMetadata).when(confluenceRestClient).getConfluenceServerMetadata();
        doReturn(ZoneId.of("UTC")).when(confluenceServerMetadata).getDefaultTimeZone();

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue);
        assertEquals(mockPages.size(), itemInfoQueue.size());
    }

    @Test
    public void buildIssueItemInfoMultipleFutureThreads() throws JsonProcessingException {
        List<String> pageType = new ArrayList<>();
        List<String> spaceKey = new ArrayList<>();
        pageType.add("PAGE");
        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, pageType, spaceKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics));
        List<ConfluenceItem> mockIssues = new ArrayList<>();
        Random random = new Random();
        int numberOfIssues = random.nextInt(100);
        for (int i = 0; i < numberOfIssues; i++) {
            mockIssues.add(createConfluenceItemBean());
        }

        ConfluenceSearchResults mockConfluenceSearchResults = mock(ConfluenceSearchResults.class);
        when(mockConfluenceSearchResults.getResults()).thenReturn(mockIssues);

        doReturn(mockConfluenceSearchResults).when(confluenceRestClient).getAllContent(any(StringBuilder.class), anyInt(), any());
        doReturn(confluenceServerMetadata).when(confluenceRestClient).getConfluenceServerMetadata();
        doReturn(ZoneId.of("UTC")).when(confluenceServerMetadata).getDefaultTimeZone();

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue);
        assertEquals(numberOfIssues, itemInfoQueue.size());
    }

    @Test
    public void testBadSpaceKeys() throws JsonProcessingException {
        List<String> pageType = new ArrayList<>();
        List<String> spaceKey = new ArrayList<>();
        pageType.add("PAGE");
        spaceKey.add("Bad Project Key");
        spaceKey.add("A");
        spaceKey.add("!@#$");
        spaceKey.add("AAAAAAAAAAAAAA");

        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, pageType, spaceKey);
        ConfluenceService confluenceService = new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics);

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(InvalidPluginConfigurationException.class, () -> confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue));
    }

    @Test
    public void testGetPagesException() throws JsonProcessingException {
        List<String> pageType = new ArrayList<>();
        List<String> SpaceKey = new ArrayList<>();
        pageType.add("Task");
        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, pageType, SpaceKey);
        ConfluenceService confluenceService = spy(new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics));

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        assertThrows(RuntimeException.class, () -> confluenceService.getPages(confluenceSourceConfig, timestamp, itemInfoQueue));
    }

    @ParameterizedTest
    @ValueSource(strings = {"America/Los_Angeles", "America/New_York", "Asia/Kolkata"})
    public void testCreateContentFilterCriteria(String confluenceServerTimezone) throws JsonProcessingException {
        List<String> pageType = new ArrayList<>();
        List<String> spaceKey = new ArrayList<>();
        ConfluenceSourceConfig confluenceSourceConfig = createConfluenceConfiguration(BASIC, pageType, spaceKey);
        doReturn(confluenceServerMetadata).when(confluenceRestClient).getConfluenceServerMetadata();
        ZoneId confluenceZoneId = ZoneId.of(confluenceServerTimezone);
        doReturn(confluenceZoneId).when(confluenceServerMetadata).getDefaultTimeZone();
        ConfluenceService confluenceService = new ConfluenceService(confluenceSourceConfig, confluenceRestClient, pluginMetrics);
        Instant pollingTime = Instant.now();
        String formattedZonedPollingTime = pollingTime.atZone(confluenceZoneId)
                .format(DateTimeFormatter.ofPattern(CQL_LAST_MODIFIED_DATE_FORMAT));
        StringBuilder contentFilterCriteria = confluenceService.createContentFilterCriteria(confluenceSourceConfig, pollingTime);
        assertNotNull(contentFilterCriteria);
        assertTrue(contentFilterCriteria.toString().contains(formattedZonedPollingTime));
    }


    private ConfluenceItem createConfluenceItemBean() {
        ConfluenceItem confluenceItem = new ConfluenceItem();
        confluenceItem.setId(UUID.randomUUID().toString());
        confluenceItem.setTitle("item_1_key");
        SpaceItem spaceItem = new SpaceItem();
        spaceItem.setId(new Random().nextInt());
        spaceItem.setKey(UUID.randomUUID().toString());
        confluenceItem.setSpaceItem(spaceItem);
        return confluenceItem;
    }

}