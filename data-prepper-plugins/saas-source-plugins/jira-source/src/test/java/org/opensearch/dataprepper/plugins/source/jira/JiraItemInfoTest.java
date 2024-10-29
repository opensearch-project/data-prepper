package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.JiraItemInfo.PROJECT;

@ExtendWith(MockitoExtension.class)
public class JiraItemInfoTest {
    private String project;
    private String issueType;
    private String id;
    private String itemId;
    private Instant eventTime;

    @Mock
    private Map<String, Object> metadata;

    @Mock
    private Map<String, Object> newMetadata;

    @Mock
    private JiraItemInfo jiraItemInfo;

    @BeforeEach
    void setUP() {
        issueType = "TestIssue";
        id = UUID.randomUUID().toString();
        project = "TestProject";
        itemId = UUID.randomUUID().toString();
        eventTime = Instant.ofEpochSecond(0);
        jiraItemInfo = new JiraItemInfo(id, itemId, project, issueType, metadata, eventTime);
    }

    @Test
    void testGetters() {
        assertEquals(jiraItemInfo.getItemId(), itemId);
        assertEquals(jiraItemInfo.getId(), id);
        assertEquals(jiraItemInfo.getProject(), project);
        assertEquals(jiraItemInfo.getIssueType(), issueType);
        assertEquals(jiraItemInfo.getMetadata(), metadata);
        assertEquals(jiraItemInfo.getEventTime(), eventTime);
    }

    @Test
    void testSetter() {
        jiraItemInfo.setEventTime(Instant.now());
        assertNotEquals(jiraItemInfo.getEventTime(), eventTime);
        jiraItemInfo.setItemId("newItemID");
        assertNotEquals(jiraItemInfo.getItemId(), itemId);
        jiraItemInfo.setId("newID");
        assertNotEquals(jiraItemInfo.getId(), id);
        jiraItemInfo.setProject("newProject");
        assertNotEquals(jiraItemInfo.getProject(), project);
        jiraItemInfo.setMetadata(newMetadata);
        assertNotEquals(jiraItemInfo.getMetadata(), metadata);
        jiraItemInfo.setIssueType("newIssueType");
        assertNotEquals(jiraItemInfo.getIssueType(), issueType);

    }

    @Test
    void testGetPartitionKey() {
        String partitionKey = jiraItemInfo.getPartitionKey();
        assertTrue(partitionKey.contains(project));
        assertTrue(partitionKey.contains(issueType));
    }

    @Test
    void testGetKeyAttributes() {
        assertNotNull(jiraItemInfo.getKeyAttributes().get(PROJECT));
    }

    @Test
    void testGetLastModifiedAt() {
        when(metadata.getOrDefault("updated", "0")).thenReturn("5");
        when(metadata.getOrDefault("created", "0")).thenReturn("0");
        assertEquals(Instant.ofEpochMilli(5), jiraItemInfo.getLastModifiedAt());

        when(metadata.getOrDefault("updated", "0")).thenReturn("5");
        when(metadata.getOrDefault("created", "0")).thenReturn("7");
        assertEquals(Instant.ofEpochMilli(7), jiraItemInfo.getLastModifiedAt());
    }

}
