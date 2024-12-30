package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class IssueTypeConfig {
    @JsonProperty("include")
    @Size(max = 1000, message = "Issue type filter should not be more than 1000")
    private List<String> include = new ArrayList<>();

    @JsonProperty("exclude")
    @Size(max = 1000, message = "Issue type filter should not be more than 1000")
    private List<String> exclude = new ArrayList<>();
}
