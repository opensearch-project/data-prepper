/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.rest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.AtlassianRestClient;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluencePaginationLinks;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceServerMetadata;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.inject.Named;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceNextLinkValidator.validateAndSanitizeURL;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.CQL_FIELD;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.EXPAND_VALUE;

@Slf4j
@Named
public class ConfluenceRestClient extends AtlassianRestClient {

    public static final String SYSTEM_INFO_API = "wiki/rest/api/settings/systemInfo";
    public static final String REST_API_SEARCH = "wiki/rest/api/content/search";
    public static final String REST_API_FETCH_CONTENT = "wiki/rest/api/content/";
    public static final String REST_API_CONTENT_EXPAND_PARAM = "?expand=body.view";
    //public static final String REST_API_SPACES = "/rest/api/api/spaces";
    public static final String WIKI_PARAM = "wiki";
    public static final String FIFTY = "50";
    public static final String START_AT = "startAt";
    public static final String LIMIT_PARAM = "limit";
    private static final String PAGE_FETCH_LATENCY_TIMER = "pageFetchLatency";
    private static final String SEARCH_CALL_LATENCY_TIMER = "searchCallLatency";
    private static final String PAGES_REQUESTED = "pagesRequested";
    private static final String PAGE_REQUESTS_FAILED = "pageRequestsFailed";
    private static final String PAGE_REQUESTS_SUCCESS = "pageRequestsSuccess";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";
    private final RestTemplate restTemplate;
    private final AtlassianAuthConfig authConfig;
    private final Timer contentFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter contentRequestedCounter;
    private final Counter pageRequestsFailedCounter;
    private final Counter pageRequestsSuccessCounter;
    private final Counter searchRequestsFailedCounter;

    public ConfluenceRestClient(RestTemplate restTemplate, AtlassianAuthConfig authConfig,
                                PluginMetrics pluginMetrics) {
        super(restTemplate, authConfig);
        this.restTemplate = restTemplate;
        this.authConfig = authConfig;

        contentFetchLatencyTimer = pluginMetrics.timer(PAGE_FETCH_LATENCY_TIMER);
        searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        contentRequestedCounter = pluginMetrics.counter(PAGES_REQUESTED);
        pageRequestsFailedCounter = pluginMetrics.counter(PAGE_REQUESTS_FAILED);
        pageRequestsSuccessCounter = pluginMetrics.counter(PAGE_REQUESTS_SUCCESS);
        searchRequestsFailedCounter = pluginMetrics.counter(SEARCH_REQUESTS_FAILED);
    }

    public ConfluenceServerMetadata getConfluenceServerMetadata() {
        URI uri = UriComponentsBuilder.fromHttpUrl(authConfig.getUrl() + SYSTEM_INFO_API)
                .buildAndExpand().toUri();
        return invokeRestApi(uri, ConfluenceServerMetadata.class).getBody();
    }

    /**
     * Method to get all Contents in a paginated fashion.
     *
     * @param cql     input parameter.
     * @param startAt the start at
     * @return InputStream input stream
     */
    public ConfluenceSearchResults getAllContent(StringBuilder cql, int startAt,
                                                 ConfluencePaginationLinks paginationLinks) {

        URI uri;
        if (null != paginationLinks && null != paginationLinks.getNext()) {
            try {
                String urlString = authConfig.getUrl() + WIKI_PARAM + paginationLinks.getNext();
                urlString = validateAndSanitizeURL(urlString);
                uri = new URI(urlString);
            } catch (URISyntaxException | MalformedURLException e) {
                throw new RuntimeException("Failed to construct pagination url.", e);
            }
        } else {
            uri = UriComponentsBuilder.fromHttpUrl(authConfig.getUrl() + REST_API_SEARCH)
                    .queryParam(LIMIT_PARAM, FIFTY)
                    .queryParam(START_AT, startAt)
                    .queryParam(CQL_FIELD, cql)
                    .queryParam(EXPAND_FIELD, EXPAND_VALUE)
                    .buildAndExpand().toUri();
        }
        return searchCallLatencyTimer.record(
                () -> {
                    try {
                        return invokeRestApi(uri, ConfluenceSearchResults.class).getBody();
                    } catch (Exception e) {
                        log.error("Error while fetching content with cql {}", cql);
                        searchRequestsFailedCounter.increment();
                        throw e;
                    }
                }
        );
    }

    /**
     * Fetches content based on given the content id.
     *
     * @param contentId the item info
     * @return the content based on the given content id
     */
    public String getContent(String contentId) {
        contentRequestedCounter.increment();
        String url = authConfig.getUrl() + REST_API_FETCH_CONTENT + "/" + contentId + REST_API_CONTENT_EXPAND_PARAM;
        URI uri = UriComponentsBuilder.fromHttpUrl(url).buildAndExpand().toUri();
        return contentFetchLatencyTimer.record(() -> {
            try {
                String body = invokeRestApi(uri, String.class).getBody();
                pageRequestsSuccessCounter.increment();
                return body;
            } catch (Exception e) {
                log.error("Error while fetching content with id {}", contentId);
                pageRequestsFailedCounter.increment();
                throw e;
            }
        });
    }

}
