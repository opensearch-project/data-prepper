package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourceConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
public class JiraSourceConfig implements SaasSourceConfig {

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

    /**
     * Number of worker threads to spawn to parallel source fetching
     */
    @JsonProperty("workers")
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

    /**
     * Default time to wait (with exponential backOff) in the case of
     * waiting for the source service to respond
     */
    @JsonProperty("backoff_time")
    private Duration backOff = DEFAULT_BACKOFF_MILLIS;


}
