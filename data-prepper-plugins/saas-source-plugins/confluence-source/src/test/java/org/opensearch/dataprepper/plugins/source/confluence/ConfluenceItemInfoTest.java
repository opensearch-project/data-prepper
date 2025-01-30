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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceItemInfo;
import org.opensearch.dataprepper.plugins.source.confluence.utils.Constants;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConfluenceItemInfoTest {
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
    private ConfluenceItemInfo confluenceItemInfo;

    @BeforeEach
    void setUP() {
        issueType = "TestIssue";
        id = UUID.randomUUID().toString();
        project = "TestProject";
        itemId = UUID.randomUUID().toString();
        eventTime = Instant.ofEpochSecond(0);
        confluenceItemInfo = new ConfluenceItemInfo(id, itemId, project, issueType, metadata, eventTime);
    }

    @Test
    void testGetters() {
        assertEquals(confluenceItemInfo.getItemId(), itemId);
        assertEquals(confluenceItemInfo.getId(), id);
        assertEquals(confluenceItemInfo.getProject(), project);
        assertEquals(confluenceItemInfo.getIssueType(), issueType);
        assertEquals(confluenceItemInfo.getMetadata(), metadata);
        assertEquals(confluenceItemInfo.getEventTime(), eventTime);
    }

    @Test
    void testGetKeyAttributes() {
        assertInstanceOf(Map.class, confluenceItemInfo.getKeyAttributes());
    }

    @Test
    void testSetter() {
        confluenceItemInfo.setEventTime(Instant.now());
        assertNotEquals(confluenceItemInfo.getEventTime(), eventTime);
        confluenceItemInfo.setItemId("newItemID");
        assertNotEquals(confluenceItemInfo.getItemId(), itemId);
        confluenceItemInfo.setId("newID");
        assertNotEquals(confluenceItemInfo.getId(), id);
        confluenceItemInfo.setProject("newProject");
        assertNotEquals(confluenceItemInfo.getProject(), project);
        confluenceItemInfo.setMetadata(newMetadata);
        assertNotEquals(confluenceItemInfo.getMetadata(), metadata);
        confluenceItemInfo.setIssueType("newIssueType");
        assertNotEquals(confluenceItemInfo.getIssueType(), issueType);

    }

    @Test
    void testGetPartitionKey() {
        String partitionKey = confluenceItemInfo.getPartitionKey();
        assertTrue(partitionKey.contains(project));
        assertTrue(partitionKey.contains(issueType));
    }


    @Test
    void testGetLastModifiedAt() {
        when(metadata.get(Constants.UPDATED)).thenReturn("5");
        when(metadata.get(Constants.CREATED)).thenReturn("0");
        assertEquals(Instant.ofEpochMilli(5), confluenceItemInfo.getLastModifiedAt());

        when(metadata.get(Constants.UPDATED)).thenReturn("5");
        when(metadata.get(Constants.CREATED)).thenReturn("7");
        assertEquals(Instant.ofEpochMilli(7), confluenceItemInfo.getLastModifiedAt());
    }

}