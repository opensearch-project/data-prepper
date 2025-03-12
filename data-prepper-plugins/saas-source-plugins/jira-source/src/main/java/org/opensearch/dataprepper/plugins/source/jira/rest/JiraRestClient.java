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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.AtlassianRestClient;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.inject.Named;
import java.net.URI;

import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.EXPAND_VALUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.JQL_FIELD;

@Slf4j
@Named
public class JiraRestClient extends AtlassianRestClient {

    public static final String REST_API_SEARCH = "rest/api/3/search";
    public static final String REST_API_FETCH_ISSUE = "rest/api/3/issue";
    public static final String FIFTY = "50";
    public static final String START_AT = "startAt";
    public static final String MAX_RESULT = "maxResults";
    private static final String TICKET_FETCH_LATENCY_TIMER = "ticketFetchLatency";
    private static final String SEARCH_CALL_LATENCY_TIMER = "searchCallLatency";
    private static final String TICKETS_REQUESTED = "ticketsRequested";
    private static final String TICKET_REQUESTS_FAILED = "ticketRequestsFailed";
    private static final String TICKET_REQUESTS_SUCCESS = "ticketRequestsSuccess";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";
    private final RestTemplate restTemplate;
    private final AtlassianAuthConfig authConfig;
    private final Timer ticketFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter ticketsRequestedCounter;
    private final Counter ticketRequestsFailedCounter;
    private final Counter ticketRequestsSuccessCounter;
    private final Counter searchRequestsFailedCounter;

    public JiraRestClient(RestTemplate restTemplate, AtlassianAuthConfig authConfig, PluginMetrics pluginMetrics) {
        super(restTemplate, authConfig);
        this.restTemplate = restTemplate;
        this.authConfig = authConfig;

        ticketFetchLatencyTimer = pluginMetrics.timer(TICKET_FETCH_LATENCY_TIMER);
        searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        ticketsRequestedCounter = pluginMetrics.counter(TICKETS_REQUESTED);
        ticketRequestsFailedCounter = pluginMetrics.counter(TICKET_REQUESTS_FAILED);
        ticketRequestsSuccessCounter = pluginMetrics.counter(TICKET_REQUESTS_SUCCESS);
        searchRequestsFailedCounter = pluginMetrics.counter(SEARCH_REQUESTS_FAILED);
    }

    /**
     * Method to get Issues.
     *
     * @param jql     input parameter.
     * @param startAt the start at
     * @return InputStream input stream
     */
    public SearchResults getAllIssues(StringBuilder jql, int startAt) {

        String url = authConfig.getUrl() + REST_API_SEARCH;

        URI uri = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam(MAX_RESULT, FIFTY)
                .queryParam(START_AT, startAt)
                .queryParam(JQL_FIELD, jql)
                .queryParam(EXPAND_FIELD, EXPAND_VALUE)
                .buildAndExpand().toUri();
        return searchCallLatencyTimer.record(
                () -> {
                    try {
                        return invokeRestApi(uri, SearchResults.class).getBody();
                    } catch (Exception e) {
                        log.error("Error while fetching issues with jql {}", jql);
                        searchRequestsFailedCounter.increment();
                        throw e;
                    }
                }
        );
    }

    /**
     * Gets issue.
     *
     * @param issueKey the item info
     * @return the issue
     */
    public String getIssue(String issueKey) {
        ticketsRequestedCounter.increment();
        String url = authConfig.getUrl() + REST_API_FETCH_ISSUE + "/" + issueKey;
        URI uri = UriComponentsBuilder.fromHttpUrl(url).buildAndExpand().toUri();
        return ticketFetchLatencyTimer.record(() -> {
            try {
                String body = invokeRestApi(uri, String.class).getBody();
                ticketRequestsSuccessCounter.increment();
                return body;
            } catch (Exception e) {
                log.error("Error while fetching issue with key {}", issueKey);
                ticketRequestsFailedCounter.increment();
                throw e;
            }
        });
    }
}
