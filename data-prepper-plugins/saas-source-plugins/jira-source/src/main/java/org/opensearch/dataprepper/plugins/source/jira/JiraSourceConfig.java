package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Getter
public class JiraSourceConfig implements CrawlerSourceConfig {

    private static final Duration DEFAULT_BACKOFF_MILLIS = Duration.ofMinutes(2);

    /**
     * Jira account url
     */
    @JsonProperty("hosts")
    private List<String> hosts;

    /**
     * Authentication Config to Access Jira
     */
    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfig authenticationConfig;

    /**
     * List of projects to ingest
     */
    @JsonProperty("projects")
    @Size(max = 1000, message = "Project type filter should not be more than 1000")
    private List<String> project = new ArrayList<>();

    /**
     * List of specific issue types to ingest.
     * Ex: Story, Epic, Task etc
     */
    @JsonProperty("issue_types")
    @Size(max = 1000, message = "Issue type filter should be less than 1000")
    private List<String> issueType = new ArrayList<>();

    /**
     * Optional Inclusion patterns for filtering some tickets
     */
    @JsonProperty("inclusion_patterns")
    @Size(max = 100, message = "inclusion pattern filters should not be more than 1000")
    private List<String> inclusionPatterns;

    /**
     * Optional Exclusion patterns for excluding some tickets
     */
    @JsonProperty("exclusion_patterns")
    @Size(max = 1000, message = "exclusion pattern filter should be less than 1000")
    private List<String> exclusionPatterns;

    /**
     * Optional Status filter to ingest the tickets
     */
    @JsonProperty("statuses")
    @Size(max = 1000, message = "Status filter should be less than 1000")
    private List<String> status = new ArrayList<>();

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

    public String getAccountUrl() {
        return this.getHosts().get(0);
    }

    public String getAuthType() {
        return this.getAuthenticationConfig().getAuthType();
    }
}
