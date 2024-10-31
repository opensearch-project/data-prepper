package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@Getter
public class JiraSourceConfig implements CrawlerSourceConfig {

    private static final Duration DEFAULT_BACKOFF_MILLIS = Duration.ofMinutes(2);
    /**
     * This field aims at holding all the additional properties which are not specified above.
     *
     * <p>Needs to be a Map of String to Object to ensure that the value could be a nested structure
     * as well.
     */
    @Setter(AccessLevel.NONE)
    Map<String, Object> additionalProperties = new HashMap<>();
    /**
     * Jira account url
     */
    @JsonProperty("account_url")
    private String accountUrl;
    /**
     * A map of connector credentials specific to this source
     */
    @JsonProperty("connector_credentials")
    private Map<String, String> connectorCredentials;
    /**
     * List of projects to ingest
     */
    @JsonProperty("project")
    @Size(max = 1000, message = "Project type filter should not be more than 1000")
    private List<String> project = new ArrayList<>();
    /**
     * List of specific issue types to ingest.
     * Ex: Story, Epic, Task etc
     */
    @JsonProperty("issue_type")
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
    @JsonProperty("status")
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

    public String getJiraId() {
        return this.getConnectorCredentials().get("jira_id");
    }

    public String getJiraCredential() {
        return this.getConnectorCredentials().get("jira_credential");
    }

    public String getAuthType() {
        return this.getConnectorCredentials().get("auth_type");
    }

    public String getAccessToken() {
        return fetchGivenOAuthAttribute("access_token");
    }

    public String getRefreshToken() {
        return fetchGivenOAuthAttribute("refresh_token");
    }

    public String getClientId() {
        return fetchGivenOAuthAttribute("client_id");
    }

    public String getClientSecret() {
        return fetchGivenOAuthAttribute("client_secret");
    }

    private String fetchGivenOAuthAttribute(String givenAttribute) {
        if (!OAUTH2.equals(getAuthType())) {
            throw new RuntimeException("Authentication Type is not OAuth2.");
        }
        String attributeValue = this.getConnectorCredentials().get(givenAttribute);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new RuntimeException(String.format("%s is required for OAuth2 AuthType", givenAttribute));
        }
        return attributeValue;
    }

}
