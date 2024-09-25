package org.opensearch.dataprepper.connector.jira;

import org.opensearch.dataprepper.connector.sdk.base.BaseConnectorSourceConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Getter;

@Getter
public class JiraSourceConfig implements BaseConnectorSourceConfig {

    /**
     * This connector's account url
     */
    @JsonProperty("account_url")
    private String accountUrl;

    /**
     * A map of connector credentials specific to this connector
     */
    @JsonProperty("connector_credentials")
    private Map<String, Object> connectorCredentials;

    /**
     * List of projects to ingest
     */
    @JsonProperty("project")
    private List<String> project = new ArrayList<>();

    /**
     * List of specific issue types to ingest.
     * Ex: Story, Epic, Task etc
     */
    @JsonProperty("issue_type")
    private List<String> issueType = new ArrayList<>();

    /**
     * Optional Inclusion patterns for filtering some tickets
     */
    @JsonProperty("inclusion_patterns")
    private String inclusionPatterns;

    /**
     * Optional Exclusion patterns for excluding some tickets
     */
    @JsonProperty("exclusion_patterns")
    private String exclusionPatterns;

    /**
     * Optional Status filter to ingest the tickets
     */
    @JsonProperty("status")
    private String status;


}
