/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.rest;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.utils.AddressValidation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.inject.Named;
import java.net.URI;
import java.util.List;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.EXPAND_VALUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.JQL_FIELD;

@Slf4j
@Named
public class JiraRestClient {

    public static final String REST_API_SEARCH = "rest/api/3/search";
    public static final String REST_API_FETCH_ISSUE = "rest/api/3/issue";
    //public static final String REST_API_PROJECTS = "/rest/api/3/project/search";
    public static final String FIFTY = "50";
    public static final String START_AT = "startAt";
    public static final String MAX_RESULT = "maxResults";
    public static final List<Integer> RETRY_ATTEMPT_SLEEP_TIME = List.of(1, 2, 5, 10, 20, 40);
    private static final String TICKET_FETCH_LATENCY_TIMER = "ticketFetchLatency";
    private static final String SEARCH_CALL_LATENCY_TIMER = "searchCallLatency";
    private static final String PROJECTS_FETCH_LATENCY_TIMER = "projectFetchLatency";
    private static final String ISSUES_REQUESTED = "issuesRequested";
    private int sleepTimeMultiplier = 1000;
    private final RestTemplate restTemplate;
    private final JiraAuthConfig authConfig;
    private final Timer ticketFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Timer projectFetchLatencyTimer;
    private final Counter issuesRequestedCounter;

    public JiraRestClient(RestTemplate restTemplate, JiraAuthConfig authConfig, PluginMetrics pluginMetrics) {
        this.restTemplate = restTemplate;
        this.authConfig = authConfig;

        ticketFetchLatencyTimer = pluginMetrics.timer(TICKET_FETCH_LATENCY_TIMER);
        searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        projectFetchLatencyTimer = pluginMetrics.timer(PROJECTS_FETCH_LATENCY_TIMER);
        issuesRequestedCounter = pluginMetrics.counter(ISSUES_REQUESTED);
    }

    /**
     * Method to get Issues.
     *
     * @param jql     input parameter.
     * @param startAt the start at
     * @return InputStream input stream
     */
    @Timed(SEARCH_CALL_LATENCY_TIMER)
    public SearchResults getAllIssues(StringBuilder jql, int startAt) {

        String url = authConfig.getUrl() + REST_API_SEARCH;

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

    private <T> ResponseEntity<T> invokeRestApi(URI uri, Class<T> responseType) throws BadRequestException {
        AddressValidation.validateInetAddress(AddressValidation.getInetAddress(uri.toString()));
        int retryCount = 0;
        while (retryCount < RETRY_ATTEMPT) {
            try {
                return restTemplate.getForEntity(uri, responseType);
            } catch (HttpClientErrorException ex) {
                HttpStatus statusCode = ex.getStatusCode();
                String statusMessage = ex.getMessage();
                log.error("An exception has occurred while getting response from Jira search API  {}", ex.getMessage());
                if (statusCode == HttpStatus.FORBIDDEN) {
                    throw new UnAuthorizedException(statusMessage);
                } else if (statusCode == HttpStatus.UNAUTHORIZED) {
                    log.error(NOISY, "Token expired. We will try to renew the tokens now", ex);
                    authConfig.renewCredentials();
                } else if (statusCode == HttpStatus.TOO_MANY_REQUESTS) {
                    log.error(NOISY, "Hitting API rate limit. Backing off with sleep timer.", ex);
                }
                try {
                    Thread.sleep((long) RETRY_ATTEMPT_SLEEP_TIME.get(retryCount) * sleepTimeMultiplier);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Sleep in the retry attempt got interrupted", e);
                }
            }
            retryCount++;
        }
        String errorMessage = String.format("Exceeded max retry attempts. Failed to execute the Rest API call %s", uri);
        log.error(errorMessage);
        throw new RuntimeException(errorMessage);
    }

    @VisibleForTesting
    public void setSleepTimeMultiplier(int multiplier) {
        sleepTimeMultiplier = multiplier;
    }
}
