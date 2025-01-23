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

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.utils.Constants;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraContentType;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.opensearch.dataprepper.plugins.source.jira.JiraService.CONTENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants._ISSUE;

@Setter
@Getter
public class JiraItemInfo implements ItemInfo {
    private String project;
    private String issueType;
    private String id;
    private String itemId;
    private Map<String, Object> metadata;
    private Instant eventTime;

    public JiraItemInfo(String id,
                        String itemId,
                        String project,
                        String issueType,
                        Map<String, Object> metadata,
                        Instant eventTime
    ) {
        this.id = id;
        this.project = project;
        this.issueType = issueType;
        this.itemId = itemId;
        this.metadata = metadata;
        this.eventTime = eventTime;
    }

    public static JiraItemInfoBuilder builder() {
        return new JiraItemInfoBuilder();
    }

    @Override
    public String getPartitionKey() {
        return project + "|" + issueType + "|" + UUID.randomUUID();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Map<String, Object> getKeyAttributes() {
        return Map.of(Constants.PROJECT, project);
    }

    @Override
    public Instant getLastModifiedAt() {
        long updatedAtMillis = getMetadataField(Constants.UPDATED);
        long createdAtMillis = getMetadataField(CREATED);
        return createdAtMillis > updatedAtMillis ?
                Instant.ofEpochMilli(createdAtMillis) : Instant.ofEpochMilli(updatedAtMillis);
    }

    private Long getMetadataField(String fieldName) {
        Object value = this.metadata.get(fieldName);
        if (value == null) {
            return 0L;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (Exception e) {
                return 0L;
            }
        }
        return 0L;
    }

    public static class JiraItemInfoBuilder {
        private Map<String, Object> metadata;
        private Instant eventTime;
        private String id;
        private String itemId;
        private String project;
        private String issueType;

        public JiraItemInfoBuilder() {
        }

        public JiraItemInfo build() {
            return new JiraItemInfo(id, itemId, project, issueType, metadata, eventTime);
        }

        public JiraItemInfoBuilder withMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public JiraItemInfoBuilder withEventTime(Instant eventTime) {
            this.eventTime = eventTime;
            return this;
        }

        public JiraItemInfoBuilder withItemId(String itemId) {
            this.itemId = itemId;
            return this;
        }

        public JiraItemInfoBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public JiraItemInfoBuilder withProject(String project) {
            this.project = project;
            return this;
        }

        public JiraItemInfoBuilder withIssueBean(IssueBean issue) {
            Map<String, Object> issueMetadata = new HashMap<>();
            issueMetadata.put(PROJECT_KEY, issue.getProject());
            issueMetadata.put(PROJECT_NAME, issue.getProjectName());
            issueMetadata.put(CREATED, issue.getCreatedTimeMillis());
            issueMetadata.put(UPDATED, issue.getUpdatedTimeMillis());
            issueMetadata.put(ISSUE_KEY, issue.getKey());
            issueMetadata.put(CONTENT_TYPE, JiraContentType.ISSUE.getType());

            this.project = issue.getProject();
            this.id = issue.getKey();
            this.issueType = JiraContentType.ISSUE.getType();
            this.itemId = _ISSUE + issueMetadata.get(PROJECT_KEY) + "-" + issue.getKey();
            this.metadata = issueMetadata;
            return this;
        }
    }

}