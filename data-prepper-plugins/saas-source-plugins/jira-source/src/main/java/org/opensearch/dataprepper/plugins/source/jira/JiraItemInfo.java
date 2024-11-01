package org.opensearch.dataprepper.plugins.source.jira;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.utils.Constants;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraContentType;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.source.jira.JiraService.CONTENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
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
        long updatedAtMillis = Long.parseLong((String) this.metadata.getOrDefault(Constants.UPDATED, "0"));
        long createdAtMillis = Long.parseLong((String) this.metadata.getOrDefault(Constants.CREATED, "0"));
        return createdAtMillis > updatedAtMillis ?
                Instant.ofEpochMilli(createdAtMillis) : Instant.ofEpochMilli(updatedAtMillis);
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

        public JiraItemInfoBuilder withIssueType(String issueType) {
            this.issueType = issueType;
            return this;
        }

        public JiraItemInfoBuilder withIssueBean(IssueBean issue) {
            Map<String, Object> issueMetadata = new HashMap<>();
            if (Objects.nonNull(((Map) issue.getFields().get(PROJECT)).get(KEY))) {
                issueMetadata.put(PROJECT_KEY, ((Map) issue.getFields().get(PROJECT)).get(KEY).toString());
                this.project = ((Map) issue.getFields().get(PROJECT)).get(KEY).toString();
            }

            if (Objects.nonNull(((Map) issue.getFields().get(PROJECT)).get(NAME))) {
                issueMetadata.put(PROJECT_NAME, ((Map) issue.getFields().get(PROJECT)).get(NAME).toString());
            }

            long created = 0;
            Pattern JiraDateTimePattern = Pattern.compile(
                    "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]\\d{4}$");
            DateTimeFormatter offsetDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            if (Objects.nonNull(issue.getFields().get(CREATED)) && JiraDateTimePattern.matcher(issue.getFields().get(CREATED)
                    .toString()).matches()) {
                String charSequence = issue.getFields().get(CREATED).toString();
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence, offsetDateTimeFormatter);
                new Date(offsetDateTime.toInstant().toEpochMilli());
                created = offsetDateTime.toEpochSecond() * 1000;
            }
            issueMetadata.put(CREATED, String.valueOf(created));

            long updated = 0;
            if (JiraDateTimePattern.matcher(issue.getFields().get(UPDATED).toString()).matches()) {
                String charSequence = issue.getFields().get(UPDATED).toString();
                OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence, offsetDateTimeFormatter);
                new Date(offsetDateTime.toInstant().toEpochMilli());
                updated = offsetDateTime.toEpochSecond() * 1000;
            }
            issueMetadata.put(UPDATED, String.valueOf(updated));

            issueMetadata.put(ISSUE_KEY, issue.getKey());
            this.id = issue.getKey();

            issueMetadata.put(CONTENT_TYPE, JiraContentType.ISSUE.getType());
            this.issueType = JiraContentType.ISSUE.getType();

            this.itemId = _ISSUE + issueMetadata.get(PROJECT_KEY) + "-" + issue.getKey();

            this.metadata = issueMetadata;

            return this;
        }
    }

}