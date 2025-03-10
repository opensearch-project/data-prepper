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
    private static final String ISSUES_REQUESTED = "issuesRequested";
    private final RestTemplate restTemplate;
    private final AtlassianAuthConfig authConfig;
    private final Timer ticketFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter issuesRequestedCounter;

    public JiraRestClient(RestTemplate restTemplate, AtlassianAuthConfig authConfig, PluginMetrics pluginMetrics) {
        super(restTemplate, authConfig);
        this.restTemplate = restTemplate;
        this.authConfig = authConfig;

        ticketFetchLatencyTimer = pluginMetrics.timer(TICKET_FETCH_LATENCY_TIMER);
        searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        issuesRequestedCounter = pluginMetrics.counter(ISSUES_REQUESTED);
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
                () -> invokeRestApi(uri, SearchResults.class).getBody()
        );
    }

    /**
     * Gets issue.
     *
     * @param issueKey the item info
     * @return the issue
     */
    public String getIssue(String issueKey) {
        issuesRequestedCounter.increment();
        String url = authConfig.getUrl() + REST_API_FETCH_ISSUE + "/" + issueKey;
        URI uri = UriComponentsBuilder.fromHttpUrl(url).buildAndExpand().toUri();
        return ticketFetchLatencyTimer.record(
                () -> invokeRestApi(uri, String.class).getBody()
        );
    }
}
