/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.RetryHandler;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultRetryStrategy;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultStatusCodeHandler;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.getErrorTypeMetricCounterMap;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishErrorTypeMetricCounter;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishGetResponseSizeMetricInBytes;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishGetRequestsSuccessMetric;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.provideGetRequestsFailureCounter;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishSearchResponseSizeMetricInBytes;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishSearchRequestsSuccessMetric;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.provideSearchRequestFailureCounter;

/**
 * REST client for interacting with Office 365 Management API.
 * Handles all HTTP communications with the Office 365 endpoints.
 */
@Slf4j
@Named
public class Office365RestClient {
    private static final String AUDIT_LOG_FETCH_LATENCY = "auditLogFetchLatency";
    private static final String API_CALLS = "apiCalls";
    private static final String AUDIT_LOGS_REQUESTED = "auditLogsRequested";
    private static final String SEARCH_CALL_LATENCY = "searchCallLatency";

    private static final String MANAGEMENT_API_BASE_URL = "https://manage.office.com/api/v1.0/";

    private final RestTemplate restTemplate = new RestTemplate();
    private final RetryHandler retryHandler;
    private final Office365AuthenticationInterface authConfig;
    private final Timer auditLogFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter auditLogsRequestedCounter;
    private final Counter apiCallsCounter;
    private final PluginMetrics pluginMetrics;

    private Map<String, Counter> errorTypeMetricCounterMap;

    public Office365RestClient(final Office365AuthenticationInterface authConfig,
                               final PluginMetrics pluginMetrics) {
        // TODO: Abstract into a Office365PluginMetrics
        this.authConfig = authConfig;
        this.pluginMetrics = pluginMetrics;
        this.auditLogFetchLatencyTimer = pluginMetrics.timer(AUDIT_LOG_FETCH_LATENCY);
        this.searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY);
        this.auditLogsRequestedCounter = pluginMetrics.counter(AUDIT_LOGS_REQUESTED);
        this.apiCallsCounter = pluginMetrics.counter(API_CALLS);
        this.errorTypeMetricCounterMap = getErrorTypeMetricCounterMap(pluginMetrics);
        this.retryHandler = new RetryHandler(
                new DefaultRetryStrategy(),
                new DefaultStatusCodeHandler());
    }

    /**
     * Starts and verifies subscriptions for Office 365 audit logs.
     */
    public void startSubscriptions() {
        log.info("Starting Office 365 subscriptions for audit logs");
        try {
            HttpHeaders headers = new HttpHeaders();

            headers.setContentType(MediaType.APPLICATION_JSON);

            // TODO: Only start the subscriptions only if the call commented
            //  out below doesn't return all the audit log types
            // Check current subscriptions
//            final String SUBSCRIPTION_LIST_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/list";
//            String listUrl = String.format(SUBSCRIPTION_LIST_URL, authConfig.getTenantId());
//
//            ResponseEntity<String> listResponse = restTemplate.exchange(
//                    listUrl,
//                    HttpMethod.GET,
//                    new HttpEntity<>(headers),
//                    String.class
//            );
//            log.debug("Current subscriptions: {}", listResponse.getBody());

            // Start subscriptions for each content type
            headers.setContentLength(0);

            for (String contentType : CONTENT_TYPES) {
                final String SUBSCRIPTION_START_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/start?contentType=%s";
                String url = String.format(SUBSCRIPTION_START_URL,
                        authConfig.getTenantId(),
                        contentType);

                retryHandler.executeWithRetry(() -> {
                    try {
                        headers.setBearerAuth(authConfig.getAccessToken());
                        apiCallsCounter.increment();
                        ResponseEntity<String> response = restTemplate.exchange(
                                url,
                                HttpMethod.POST,
                                new HttpEntity<>(headers),
                                String.class
                        );
                        log.debug("Started subscription for {}: {}", contentType, response.getBody());
                        return response;
                    } catch (HttpClientErrorException | HttpServerErrorException e) {
                        if (e.getResponseBodyAsString().contains("AF20024")) {
                            log.debug("Subscription for {} is already enabled", contentType);
                            return null;
                        }
                        throw e;
                    }
                }, authConfig::renewCredentials);
            }
        } catch (Exception e) {
            publishErrorTypeMetricCounter(e, this.errorTypeMetricCounterMap);
            log.error(NOISY, "Failed to initialize subscriptions", e);
            throw new SaaSCrawlerException("Failed to initialize subscriptions: " + e.getMessage(), e, true);
        }
    }

    /**
     * Searches for audit logs of a specific content type within a time range.
     * Implements retry with exponential backoff for recoverable errors.
     *
     * @param contentType the type of content to search for
     * @param startTime  the start time of the search range
     * @param endTime    the end time of the search range
     * @param pageUri    the URI for pagination (can be null for first page)
     * @return AuditLogsResponse containing the list of audit log entries and the next page URI
     */
    public AuditLogsResponse searchAuditLogs(final String contentType,
                                             final Instant startTime,
                                             final Instant endTime,
                                             String pageUri) {
        final String GET_AUDIT_LOGS_URL = MANAGEMENT_API_BASE_URL +
                "%s/activity/feed/subscriptions/content?contentType=%s&startTime=%s&endTime=%s";

        final String url = pageUri != null ? pageUri :
                String.format(GET_AUDIT_LOGS_URL,
                        authConfig.getTenantId(),
                        contentType,
                        startTime.toString(),
                        endTime.toString());

        log.debug("Searching audit logs with URL: {}", url);
        final HttpHeaders headers = new HttpHeaders();

        return searchCallLatencyTimer.record(() -> {
            try {
                return retryHandler.executeWithRetry(
                        () -> {
                            headers.setBearerAuth(authConfig.getAccessToken());
                            apiCallsCounter.increment();

                            ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                                    url,
                                    HttpMethod.GET,
                                    new HttpEntity<>(headers),
                                    new ParameterizedTypeReference<>() {}
                            );

                            // Log response details
                            List<Map<String, Object>> responseBody = response.getBody();
                            if (responseBody == null) {
                                log.debug("Search audit logs response is null for URL: {}", url);
                            } else {
                                log.debug("Search audit logs response received {} entries for URL: {}",
                                        responseBody.size(), url);
                                String responseStr = responseBody.toString();
                                // Size protection for log limits.
                                if (responseStr.length() > 10000) {
                                    log.debug("Search audit logs response body (truncated to first 10000 chars): {}",
                                            responseStr.substring(0, 10000) + "... [TRUNCATED - total length: " + responseStr.length() + "]");
                                } else {
                                    log.debug("Search audit logs response body: {}", responseBody);
                                }
                            }

                            // Publish centralized search metrics
                            publishSearchResponseSizeMetricInBytes(pluginMetrics, response);
                            publishSearchRequestsSuccessMetric(pluginMetrics);

                            // Extract NextPageUri from response headers
                            List<String> nextPageHeaders = response.getHeaders().get("NextPageUri");
                            String nextPageUri = (nextPageHeaders != null && !nextPageHeaders.isEmpty()) ?
                                    nextPageHeaders.get(0) : null;

                            if (nextPageUri != null) {
                                log.debug("Next page URI found: {}", nextPageUri);
                            }

                            return new AuditLogsResponse(response.getBody(), nextPageUri);
                        },
                        authConfig::renewCredentials,
                        Optional.of(provideSearchRequestFailureCounter(pluginMetrics))
                );
            } catch (Exception e) {
                publishErrorTypeMetricCounter(e, this.errorTypeMetricCounterMap);
                log.error(NOISY, "Error while fetching audit logs for content type {} from URL: {}",
                        contentType, url, e);
                throw new SaaSCrawlerException("Failed to fetch audit logs", e, true);
            }
        });
    }


    /**
     * Retrieves the audit log content from a specific content URI.
     *
     * @param contentUri the URI of the audit log content
     * @return the audit log content as a string
     */
    public String getAuditLog(String contentUri) {
        if (!contentUri.startsWith(MANAGEMENT_API_BASE_URL)) {
            throw new SaaSCrawlerException("ContentUri must be from Office365 Management API: " + contentUri, false);
        }

        log.debug("Getting audit log from content URI: {}", contentUri);
        auditLogsRequestedCounter.increment();
        final HttpHeaders headers = new HttpHeaders();

        return auditLogFetchLatencyTimer.record(() -> {
            try {
                String response = retryHandler.executeWithRetry(() -> {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    apiCallsCounter.increment();
                    ResponseEntity<String> responseEntity = restTemplate.exchange(
                            contentUri,
                            HttpMethod.GET,
                            new HttpEntity<>(headers),
                            String.class
                    );

                    return responseEntity.getBody();
                }, authConfig::renewCredentials, Optional.of(provideGetRequestsFailureCounter(pluginMetrics)));

                // Log response details
                if (response == null) {
                    log.debug("Get audit log response is null for content URI: {}", contentUri);
                } else {
                    log.debug("Get audit log response received {} characters for content URI: {}",
                            response.length(), contentUri);
                    // Size protection for log limits.
                    if (response.length() > 10000) {
                        log.debug("Get audit log response content (truncated to first 10000 chars): {}",
                                response.substring(0, 10000) + "... [TRUNCATED - total length: " + response.length() + "]");
                    } else {
                        log.debug("Get audit log response content: {}", response);
                    }
                }

                // Publish centralized GET request metrics
                publishGetResponseSizeMetricInBytes(pluginMetrics, response);
                publishGetRequestsSuccessMetric(pluginMetrics);

                return response;
            } catch (Exception e) {
                publishErrorTypeMetricCounter(e, this.errorTypeMetricCounterMap);
                log.error(NOISY, "Error while fetching audit log content from URI: {}", contentUri, e);
                throw new SaaSCrawlerException("Failed to fetch audit log", e, true);
            }
        });
    }
}
