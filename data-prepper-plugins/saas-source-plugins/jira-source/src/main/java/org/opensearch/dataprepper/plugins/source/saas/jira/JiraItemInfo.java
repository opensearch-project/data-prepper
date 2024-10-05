package org.opensearch.dataprepper.plugins.source.saas.jira;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Setter
@Getter
public class JiraItemInfo extends ItemInfo {
    private String project;
    private String issueType;
    private String id;

    public JiraItemInfo(String id, String itemId, String project, String issueType) {
        super(itemId);
        this.id = id;
        this.project = project;
        this.issueType = issueType;
    }

    @Override
    public String getPartitionKey() {
        return project+"|"+issueType+ "|" + UUID.randomUUID();
    }

    @Override
    public String getId(){
        return id;
    }

    @Override
    public Map<String, Object> getKeyAttributes() {
        return Map.of("project", project);
    }

    public static JiraItemInfoBuilder builder() {
        return new JiraItemInfoBuilder();
    }

    public static class JiraItemInfoBuilder {
        private String id;
        private String itemId;
        private String project;
        private String issueType;
        Map<String, String> metadata;
        Long eventTime;

        public JiraItemInfo build() {
            return new JiraItemInfo(id, itemId, project, issueType);
        }
        public JiraItemInfoBuilder withMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }
        public JiraItemInfoBuilder withEventTime(Long eventTime) {
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
        public JiraItemInfoBuilder() {
        }
    }

}
