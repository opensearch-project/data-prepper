/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;


public class IssueBean {

    /**
     * Expand options that include additional issue details in the response.
     */
    @Getter
    @Setter
    @JsonProperty("expand")
    private String expand = null;
    /**
     * The ID of the issue.
     */
    @Getter
    @Setter
    @JsonProperty("id")
    private String id = null;
    /**
     * The URL of the issue details.
     */
    @Getter
    @Setter
    @JsonProperty("self")
    private String self = null;
    /**
     * The key of the issue.
     */
    @Getter
    @Setter
    @JsonProperty("key")
    private String key = null;

    @Getter
    @Setter
    @JsonProperty("renderedFields")
    private Map<String, Object> renderedFields = null;

    @Getter
    @Setter
    @JsonProperty("properties")
    private Map<String, Object> properties = null;

    @Getter
    @Setter
    @JsonProperty("names")
    private Map<String, String> names = null;

    @Getter
    @Setter
    @JsonProperty("fields")
    private Map<String, Object> fields = null;
    
    @JsonIgnore
    private final Pattern JiraDateTimePattern = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]\\d{4}$");
    @JsonIgnore
    private final DateTimeFormatter offsetDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    @JsonIgnore
    public String getProject() {
        if (fields != null && Objects.nonNull(((Map) fields.get(PROJECT)).get(KEY))) {
            return ((Map) fields.get(PROJECT)).get(KEY).toString();
        }
        return null;
    }

    @JsonIgnore
    public String getProjectName() {
        if (fields != null && Objects.nonNull(((Map) fields.get(PROJECT)).get(NAME))) {
            return ((Map) fields.get(PROJECT)).get(NAME).toString();
        }
        return null;
    }

    @JsonIgnore
    public long getCreatedTimeMillis() {
        return getGivenDateField(CREATED);
    }

    @JsonIgnore
    public long getUpdatedTimeMillis() {
        return getGivenDateField(UPDATED);
    }

    @JsonIgnore
    private long getGivenDateField(String dateTimeFieldToPull) {
        long dateTimeField = 0;

        if (fields != null && Objects.nonNull(fields.get(dateTimeFieldToPull)) && JiraDateTimePattern.matcher(fields.get(dateTimeFieldToPull)
                .toString()).matches()) {
            String charSequence = fields.get(dateTimeFieldToPull).toString();
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence, offsetDateTimeFormatter);
            dateTimeField = offsetDateTime.toInstant().toEpochMilli();
        }
        return dateTimeField;
    }


}
