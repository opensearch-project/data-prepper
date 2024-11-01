package org.opensearch.dataprepper.plugins.source.jira.rest;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.inject.Named;
import java.net.URI;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.AUTHORIZATION_ERROR_CODE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.EXPAND_VALUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.FIFTY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.JQL_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.MAX_RESULT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RATE_LIMIT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.REST_API_FETCH_ISSUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.REST_API_SEARCH;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT_SLEEP_TIME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.START_AT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.TOKEN_EXPIRED;

@Slf4j
@Named
public class JiraRestClient {

    private static final String TICKET_FETCH_LATENCY_TIMER = "ticketFetchLatency";
    private static final String SEARCH_CALL_LATENCY_TIMER = "searchCallLatency";
    private static final String ISSUES_REQUESTED = "issuesRequested";
    private final RestTemplate restTemplate;
    private final JiraAuthConfig authConfig;
    private final Timer ticketFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter issuesRequestedCounter;
    private final PluginMetrics jiraPluginMetrics = PluginMetrics.fromNames("jiraRestClient", "aws");
    private int sleepTimeMultiplier = 1000;

    public JiraRestClient(RestTemplate restTemplate, JiraAuthConfig authConfig) {
        this.restTemplate = restTemplate;
        this.authConfig = authConfig;

        ticketFetchLatencyTimer = jiraPluginMetrics.timer(TICKET_FETCH_LATENCY_TIMER);
        searchCallLatencyTimer = jiraPluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        issuesRequestedCounter = jiraPluginMetrics.counter(ISSUES_REQUESTED);
    }

    /**
     * Method to get Issues.
     *
     * @param jql           input parameter.
     * @param startAt       the start at
     * @param configuration input parameter.
     * @return InputStream input stream
     */
    @Timed(SEARCH_CALL_LATENCY_TIMER)
    public SearchResults getAllIssues(StringBuilder jql, int startAt,
                                      JiraSourceConfig configuration) {

        String url = configuration.getAccountUrl() + REST_API_SEARCH;
        if (configuration.getAuthType().equals(OAUTH2)) {
            url = authConfig.getUrl() + REST_API_SEARCH;
        }

        URI uri = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam(MAX_RESULT, FIFTY)
                .queryParam(START_AT, startAt)
                .queryParam(JQL_FIELD, jql)
                .queryParam(EXPAND_FIELD, EXPAND_VALUE)
                .buildAndExpand().toUri();
        return invokeRestApi(uri, SearchResults.class).getBody();
    }

    /**
     * Gets issue.
     *
     * @param issueKey the item info
     * @return the issue
     */
    @Timed(TICKET_FETCH_LATENCY_TIMER)
    public String getIssue(String issueKey) {
        issuesRequestedCounter.increment();
        String url = authConfig.getUrl() + REST_API_FETCH_ISSUE + "/" + issueKey;
        URI uri = UriComponentsBuilder.fromHttpUrl(url).buildAndExpand().toUri();
        return invokeRestApi(uri, String.class).getBody();
    }

    private <T> ResponseEntity<T> invokeRestApi(URI uri, Class<T> responseType) {

        int retryCount = 0;
        while (retryCount < RETRY_ATTEMPT) {
            try {
                return restTemplate.getForEntity(uri, responseType);
            } catch (HttpClientErrorException ex) {
                int statusCode = ex.getRawStatusCode();
                String statusMessage = ex.getMessage();
                log.error("An exception has occurred while getting response from Jira search API  {}", ex.getMessage(), ex);
                if (statusCode == AUTHORIZATION_ERROR_CODE) {
                    throw new UnAuthorizedException(statusMessage);
                } else if (statusCode == TOKEN_EXPIRED) {
                    log.error(NOISY, "Token expired. We will try to renew the tokens now", ex);
                    authConfig.renewCredentials();
                } else if (statusCode == RATE_LIMIT) {
                    log.error(NOISY, "Hitting API rate limit. Backing off with sleep timer.", ex);
                    try {
                        Thread.sleep((long) RETRY_ATTEMPT_SLEEP_TIME.get(retryCount) * sleepTimeMultiplier);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Sleep in the retry attempt got interrupted", e);
                    }
                }
            }
            retryCount++;
        }
        String errorMessage = String.format("Exceeded max retry attempts. Failed to execute the Rest API call %s", uri.toString());
        log.error(errorMessage);
        throw new RuntimeException(errorMessage);
    }

    @VisibleForTesting
    public void setSleepTimeMultiplier(int multiplier) {
        sleepTimeMultiplier = multiplier;
    }
}
