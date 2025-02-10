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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;

@ExtendWith(MockitoExtension.class)
public class JiraIteratorTest {

    @Mock
    private SearchResults mockSearchResults;
    @Mock
    private JiraRestClient jiraRestClient;
    private JiraService jiraService;
    @Mock
    private JiraSourceConfig jiraSourceConfig;
    private JiraIterator jiraIterator;
    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    @BeforeEach
    void setUp() {
        jiraService = spy(new JiraService(jiraSourceConfig, jiraRestClient,
                PluginMetrics.fromNames("jiraIteratorTest", "jira")));
    }

    public JiraIterator createObjectUnderTest() {
        return new JiraIterator(jiraService, executorServiceProvider, jiraSourceConfig);
    }

    @Test
    void testInitialization() {
        jiraIterator = createObjectUnderTest();
        assertNotNull(jiraIterator);
        jiraIterator.initialize(Instant.ofEpochSecond(0));
        when(mockSearchResults.getIssues()).thenReturn(new ArrayList<>());
        when(mockSearchResults.getTotal()).thenReturn(0);
        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());
        assertFalse(jiraIterator.hasNext());
    }

    @Test
    void sleepInterruptionTest() {
        jiraIterator = createObjectUnderTest();
        jiraIterator.initialize(Instant.ofEpochSecond(0));

        Thread testThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> {
                try {
                    jiraIterator.hasNext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });

        testThread.start();
        testThread.interrupt();
    }

    @Test
    void testItemInfoQueueNotEmpty() {
        jiraIterator = createObjectUnderTest();
        List<IssueBean> mockIssues = new ArrayList<>();
        IssueBean issue1 = createIssueBean(false);
        mockIssues.add(issue1);
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(0);
        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        jiraIterator.initialize(Instant.ofEpochSecond(0));
        jiraIterator.setCrawlerQWaitTimeMillis(1);
        assertTrue(jiraIterator.hasNext());
        assertNotNull(jiraIterator.next());
    }

    @Test
    void testStartCrawlerThreads() {
        jiraIterator = createObjectUnderTest();
        jiraIterator.initialize(Instant.ofEpochSecond(0));
        jiraIterator.hasNext();
        jiraIterator.hasNext();
        assertEquals(1, jiraIterator.showFutureList().size());
    }

    @Test
    void testFuturesCompleted() throws InterruptedException {
        jiraIterator = createObjectUnderTest();
        List<IssueBean> mockIssues = new ArrayList<>();
        IssueBean issue1 = createIssueBean(false);
        mockIssues.add(issue1);
        IssueBean issue2 = createIssueBean(false);
        mockIssues.add(issue2);
        IssueBean issue3 = createIssueBean(false);
        mockIssues.add(issue3);
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(0);
        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        jiraIterator.initialize(Instant.ofEpochSecond(0));
        jiraIterator.setCrawlerQWaitTimeMillis(1);
        jiraIterator.hasNext();

        Thread.sleep(1);
        jiraIterator.showFutureList().forEach(future -> assertTrue(future.isDone()));
        assertEquals(jiraIterator.showItemInfoQueue().size(), mockIssues.size());
    }

    @Test
    void testItemInfoQueueEmpty() {
        jiraIterator = createObjectUnderTest();
        List<IssueBean> mockIssues = new ArrayList<>();
        when(mockSearchResults.getIssues()).thenReturn(mockIssues);
        when(mockSearchResults.getTotal()).thenReturn(0);
        doReturn(mockSearchResults).when(jiraRestClient).getAllIssues(any(StringBuilder.class), anyInt());

        jiraIterator.initialize(Instant.ofEpochSecond(0));
        jiraIterator.setCrawlerQWaitTimeMillis(1);
        assertFalse(jiraIterator.hasNext());
        assertThrows(NoSuchElementException.class, () -> jiraIterator.next());
    }


    private IssueBean createIssueBean(boolean nullFields) {
        IssueBean issue1 = new IssueBean();
        issue1.setId(UUID.randomUUID().toString());
        issue1.setKey("issue_1_key");
        issue1.setSelf("https://example.com/rest/api/2/issue/123");
        issue1.setExpand("operations,versionedRepresentations,editmeta");

        Map<String, Object> fieldMap = new HashMap<>();
        if (!nullFields) {
            fieldMap.put(CREATED, Instant.now());
            fieldMap.put(UPDATED, Instant.now());
        } else {
            fieldMap.put(CREATED, 0);
            fieldMap.put(UPDATED, 0);
        }

        Map<String, Object> issueTypeMap = new HashMap<>();
        issueTypeMap.put("name", "Task");
        issueTypeMap.put("self", "https://example.com/rest/api/2/issuetype/1");
        issueTypeMap.put("id", "1");
        fieldMap.put("issuetype", issueTypeMap);

        Map<String, Object> projectMap = new HashMap<>();
        if (!nullFields) {
            projectMap.put("name", "project name test");
            projectMap.put(KEY, "TEST");
        }
        fieldMap.put("project", projectMap);

        Map<String, Object> priorityMap = new HashMap<>();
        priorityMap.put("name", "Medium");
        fieldMap.put("priority", priorityMap);

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("name", "In Progress");
        fieldMap.put("status", statusMap);

        issue1.setFields(fieldMap);

        return issue1;
    }
}
