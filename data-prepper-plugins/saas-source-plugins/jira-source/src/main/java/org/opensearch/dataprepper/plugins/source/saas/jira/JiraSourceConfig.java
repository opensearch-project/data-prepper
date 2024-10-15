package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.jira.rest.OAuth2RestHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAuth2_URL;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.REST_API_FETCH_ISSUE;

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
    private Map<String, String> connectorCredentials;

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
    private List<String> inclusionPatterns;

    /**
     * Optional Exclusion patterns for excluding some tickets
     */
    @JsonProperty("exclusion_patterns")
    private List<String> exclusionPatterns;

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


    /**
     * This field aims at holding all the additional properties which are not specified above.
     *
     * <p>Needs to be a Map of String to Object to ensure that the value could be a nested structure
     * as well.
     */
    @Setter(AccessLevel.NONE)
    Map<String, Object> additionalProperties = new HashMap<>();

    /**
     * We will intialize this variable only in the case of Authentication Type OAuth2
     */
    private String cloudId;

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
        if(!OAUTH2.equals(getAuthType())) {
            throw new RuntimeException("Authentication Type is not OAuth2.");
        }
        String accessToken = this.getConnectorCredentials().get("access_token");
        if(accessToken == null || accessToken.isEmpty()) {
            throw new RuntimeException("Access Token is required for OAuth2 AuthType");
        }
        return accessToken;
    }

    public String getRefreshToken() {
        if(!OAUTH2.equals(getAuthType())) {
            throw new RuntimeException("Authentication Type is not OAuth2.");
        }
        String refreshToken = this.getConnectorCredentials().get("refresh_token");
        if(refreshToken == null || refreshToken.isEmpty()) {
            throw new RuntimeException("Refresh Token is required for OAuth2 AuthType");
        }
        return refreshToken;
    }

}
