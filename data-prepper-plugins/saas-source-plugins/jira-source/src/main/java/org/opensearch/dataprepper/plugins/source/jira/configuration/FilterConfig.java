package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class FilterConfig {
    @JsonProperty("project")
    private ProjectConfig  projectConfig;

    @JsonProperty("status")
    private StatusConfig statusConfig;

    @JsonProperty("issue_type")
    private IssueTypeConfig issueTypeConfig;
}
