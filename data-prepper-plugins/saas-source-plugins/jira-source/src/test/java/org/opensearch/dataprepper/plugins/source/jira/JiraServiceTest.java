package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;


/**
 * The type Jira service.
 */
@ExtendWith(MockitoExtension.class)
public class JiraServiceTest {

    private static final Logger log = LoggerFactory.getLogger(JiraServiceTest.class);
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    @Mock
    private JiraRestClient jiraRestClient;


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
            return objectMapper.readValue(inputStream, JiraSourceConfig.class);
        } catch (IOException ex) {
            log.error("Failed to parse pipeline Yaml", ex);
        }
        return null;
    }

    public static JiraSourceConfig createJiraConfiguration(String auth_type,
                                                           List<String> issueType,
                                                           List<String> issueStatus,
                                                           List<String> projectKey) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> connectorCredentialsMap = new HashMap<>();
        connectorCredentialsMap.put("auth_type", auth_type);

        Map<String, Object> jiraSourceConfigMap = new HashMap<>();
        jiraSourceConfigMap.put("account_url", ACCESSIBLE_RESOURCES);
        jiraSourceConfigMap.put("connector_credentials", connectorCredentialsMap);
        jiraSourceConfigMap.put("issue_types", issueType);
        jiraSourceConfigMap.put("statuses", issueStatus);
        jiraSourceConfigMap.put("projects", projectKey);


        String jiraSourceConfigJsonString = objectMapper.writeValueAsString(jiraSourceConfigMap);
        return objectMapper.readValue(jiraSourceConfigJsonString, JiraSourceConfig.class);
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
        JiraService jiraService = new JiraService(jiraSourceConfig, jiraRestClient);
        assertNotNull(jiraService);
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
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient));
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

        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt(), any(JiraSourceConfig.class));

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue);

        assertEquals(mockIssues.size() + 1, itemInfoQueue.size());
    }

    @Test
    public void buildIssueItemInfoMultipleFutureThreads() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient));
        List<IssueBean> mockIssues = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            IssueBean issue1 = createIssueBean(false, false);
            mockIssues.add(issue1);
        }

        SearchResults mockSearchResults = mock(SearchResults.class);
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(100);

        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt(), any(JiraSourceConfig.class));

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
        JiraService jiraService = new JiraService(jiraSourceConfig, jiraRestClient);

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        assertThrows(BadRequestException.class, () -> jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue));
    }

    @Test
    public void testGetJiraEntitiesException() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient));

        doThrow(RuntimeException.class).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt(), any(JiraSourceConfig.class));

        Instant timestamp = Instant.ofEpochSecond(0);
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        assertThrows(RuntimeException.class, () -> jiraService.getJiraEntities(jiraSourceConfig, timestamp, itemInfoQueue));
    }


    @Test
    public void testGetAllIssuesBasic() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(restTemplate, jiraSourceConfig, authConfig);
        SearchResults mockSearchResults = mock(SearchResults.class);
        doReturn(new ResponseEntity<>(mockSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        SearchResults results = jiraService.getAllIssues(jql, 0, jiraSourceConfig);
        assertNotNull(results);
    }

    @Test
    public void testGetAllIssuesOauth2() throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        issueType.add("Task");
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(OAUTH2, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(restTemplate, jiraSourceConfig, authConfig);
        SearchResults mockSearchResults = mock(SearchResults.class);
        doReturn("http://mock-service.jira.com").when(authConfig).getUrl();
        doReturn(new ResponseEntity<>(mockSearchResults, HttpStatus.OK)).when(restTemplate).getForEntity(any(URI.class), any(Class.class));
        SearchResults results = jiraService.getAllIssues(jql, 0, jiraSourceConfig);
        assertNotNull(results);
    }

    private JiraSourceConfig createJiraConfiguration(String auth_type, List<String> issueType, List<String> issueStatus, List<String> projectKey) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> connectorCredentialsMap = new HashMap<>();
        connectorCredentialsMap.put("auth_type", auth_type);

        Map<String, Object> jiraSourceConfigMap = new HashMap<>();
        jiraSourceConfigMap.put("account_url", ACCESSIBLE_RESOURCES);
        jiraSourceConfigMap.put("connector_credentials", connectorCredentialsMap);
        jiraSourceConfigMap.put("issue_types", issueType);
        jiraSourceConfigMap.put("statuses", issueStatus);
        jiraSourceConfigMap.put("projects", projectKey);


        String jiraSourceConfigJsonString = objectMapper.writeValueAsString(jiraSourceConfigMap);
        return objectMapper.readValue(jiraSourceConfigJsonString, JiraSourceConfig.class);
    }

    @Test
    void testInvokeRestApiTokenExpiredInterruptException() throws JsonProcessingException, InterruptedException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(restTemplate, jiraSourceConfig, authConfig);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));
        jiraService.setSleepTimeMultiplier(100000);

        Thread testThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> {
                try {
                    jiraService.getIssue("key");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });
        testThread.start();
        Thread.sleep(100);
        testThread.interrupt();
    }

    @ParameterizedTest
    @MethodSource("provideHttpStatusCodesWithExceptionClass")
    void testInvokeRestApiTokenExpired(HttpStatus statusCode, Class expectedExceptionType) throws JsonProcessingException {
        List<String> issueType = new ArrayList<>();
        List<String> issueStatus = new ArrayList<>();
        List<String> projectKey = new ArrayList<>();
        JiraSourceConfig jiraSourceConfig = createJiraConfiguration(BASIC, issueType, issueStatus, projectKey);
        JiraService jiraService = new JiraService(restTemplate, jiraSourceConfig, authConfig);
        jiraService.setSleepTimeMultiplier(1);
        when(authConfig.getUrl()).thenReturn("https://example.com/rest/api/2/issue/key");
        when(restTemplate.getForEntity(any(URI.class), any(Class.class))).thenThrow(new HttpClientErrorException(statusCode));
        assertThrows(expectedExceptionType, () -> jiraService.getIssue("key"));
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
//            fieldMap.put(UPDATED, Instant.now());
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

}
