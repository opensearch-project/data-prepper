package org.opensearch.dataprepper.plugins.source.jira;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
public class JiraItemInfo implements ItemInfo {
    public static final String PROJECT = "project";
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
        return Map.of(PROJECT, project);
    }

    @Override
    public Instant getLastModifiedAt() {
        long updatedAtMillis = Long.parseLong((String) this.metadata.getOrDefault("updated", "0"));
        long createdAtMillis = Long.parseLong((String) this.metadata.getOrDefault("created", "0"));
        return createdAtMillis > updatedAtMillis ?
                Instant.ofEpochMilli(createdAtMillis) : Instant.ofEpochMilli(updatedAtMillis);
    }

    public static class JiraItemInfoBuilder {
        Map<String, Object> metadata;
        Instant eventTime;
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

        public JiraItemInfoBuilder withIssueType(String issueType) {
            this.issueType = issueType;
            return this;
        }
    }

}
