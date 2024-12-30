package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class ProjectConfig {
    @JsonProperty("name")
    private NameConfig nameConfig;
}
