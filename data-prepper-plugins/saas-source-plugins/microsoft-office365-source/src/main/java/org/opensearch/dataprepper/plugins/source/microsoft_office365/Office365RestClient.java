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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.exception.Office365Exception;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpStatus;

import javax.inject.Named;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.getErrorTypeMetricCounterMap;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.publishErrorTypeMetricCounter;

/**
 * REST client for interacting with Office 365 Management API.
 * Handles all HTTP communications with the Office 365 endpoints.
 */
@Slf4j
@Named
public class Office365RestClient {
    private static final String AUDIT_LOG_FETCH_LATENCY = "auditLogFetchLatency";
    private static final String AUDIT_LOG_RESPONSE_SIZE = "auditLogResponseSizeBytes";
    private static final String AUDIT_LOG_REQUESTS_FAILED = "auditLogRequestsFailed";
    private static final String AUDIT_LOG_REQUESTS_SUCCESS = "auditLogRequestsSuccess";
    private static final String AUDIT_LOGS_REQUESTED = "auditLogsRequested";
    private static final String SEARCH_CALL_LATENCY = "searchCallLatency";
    private static final String SEARCH_RESPONSE_SIZE = "searchResponseSizeBytes";
    private static final String SEARCH_REQUESTS_SUCCESS = "searchRequestsSuccess";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";

    private static final String MANAGEMENT_API_BASE_URL = "https://manage.office.com/api/v1.0/";

    private final RestTemplate restTemplate = new RestTemplate();
    private final Office365AuthenticationInterface authConfig;
    private final Timer auditLogFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter auditLogsRequestedCounter;
    private final Counter auditLogRequestsFailedCounter;
    private final Counter auditLogRequestsSuccessCounter;
    private final Counter searchRequestsFailedCounter;
    private final Counter searchRequestsSuccessCounter;
    private final DistributionSummary auditLogResponseSizeSummary;
    private final DistributionSummary searchResponseSizeSummary;

    private Map<String, Counter> errorTypeMetricCounterMap;

    public Office365RestClient(final Office365AuthenticationInterface authConfig,
                               final PluginMetrics pluginMetrics) {
        // TODO: Abstract into a Office365PluginMetrics
        this.authConfig = authConfig;
        this.auditLogFetchLatencyTimer = pluginMetrics.timer(AUDIT_LOG_FETCH_LATENCY);
        this.searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY);
        this.auditLogsRequestedCounter = pluginMetrics.counter(AUDIT_LOGS_REQUESTED);
        this.auditLogRequestsFailedCounter = pluginMetrics.counter(AUDIT_LOG_REQUESTS_FAILED);
        this.auditLogRequestsSuccessCounter = pluginMetrics.counter(AUDIT_LOG_REQUESTS_SUCCESS);
        this.searchRequestsFailedCounter = pluginMetrics.counter(SEARCH_REQUESTS_FAILED);
        this.searchRequestsSuccessCounter = pluginMetrics.counter(SEARCH_REQUESTS_SUCCESS);
        this.auditLogResponseSizeSummary = pluginMetrics.summary(AUDIT_LOG_RESPONSE_SIZE);
        this.searchResponseSizeSummary = pluginMetrics.summary(SEARCH_RESPONSE_SIZE);

        this.errorTypeMetricCounterMap = getErrorTypeMetricCounterMap(pluginMetrics);
    }

    /**
     * Lists current subscriptions for Office 365 audit logs.
     * 
     * @return List of subscription maps containing contentType, status, and webhook information
     */
    private List<Map<String, Object>> listSubscriptions() {
        log.info("Listing Office 365 subscriptions");
        final String SUBSCRIPTION_LIST_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/list";
        String listUrl = String.format(SUBSCRIPTION_LIST_URL, authConfig.getTenantId());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        try {
            return RetryHandler.executeWithRetry(() -> {
                headers.setBearerAuth(authConfig.getAccessToken());
                ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                        listUrl,
                        HttpMethod.GET,
                        new HttpEntity<>(headers),
                        new ParameterizedTypeReference<>() {}
                );
                log.debug("Current subscriptions: {}", response.getBody());
                return response.getBody();
            }, authConfig::renewCredentials);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            HttpStatus statusCode = e.getStatusCode();
            publishErrorTypeMetricCounter(statusCode.getReasonPhrase(), this.errorTypeMetricCounterMap);
            log.error(NOISY, "Failed to list subscriptions with status code {}: {}",
                    statusCode, e.getMessage());
            throw new RuntimeException("Failed to list subscriptions: " + e.getMessage(), e);
        } catch (Exception e) {
            if (e instanceof SecurityException) {
                publishErrorTypeMetricCounter(HttpStatus.FORBIDDEN.getReasonPhrase(), this.errorTypeMetricCounterMap);
            }
            log.error(NOISY, "Failed to list subscriptions", e);
            throw new RuntimeException("Failed to list subscriptions: " + e.getMessage(), e);
        }
    }

    /**
     * Starts subscriptions for the specified content types.
     * 
     * @param contentTypesToStart List of content types to start subscriptions for
     */
    private void startSubscriptionsForContentTypes(List<String> contentTypesToStart) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setContentLength(0);

        for (String contentType : contentTypesToStart) {
            final String SUBSCRIPTION_START_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/start?contentType=%s";
            String url = String.format(SUBSCRIPTION_START_URL,
                    authConfig.getTenantId(),
                    contentType);

            RetryHandler.executeWithRetry(() -> {
                try {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    ResponseEntity<String> response = restTemplate.exchange(
                            url,
                            HttpMethod.POST,
                            new HttpEntity<>(headers),
                            String.class
                    );
                    log.info("Successfully started subscription for {}: {}", contentType, response.getBody());
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
        
        log.info("Successfully started {} subscription(s)", contentTypesToStart.size());
    }

    /**
     * Starts and verifies subscriptions for Office 365 audit logs.
     * Only starts subscriptions for content types that are not already enabled.
     * If listing subscriptions fails, falls back to starting all content types.
     */
    public void startSubscriptions() {
        log.info("Starting Office 365 subscriptions for audit logs");
        
        List<String> contentTypesToStart = new ArrayList<>();
        
        // Try to get current subscriptions to determine which need to be started
        try {
            List<Map<String, Object>> currentSubscriptions = listSubscriptions();
            
            // Determine which content types are already enabled
            Set<String> enabledContentTypes = new HashSet<>();
            for (Map<String, Object> subscription : currentSubscriptions) {
                String contentType = (String) subscription.get("contentType");
                String status = (String) subscription.get("status");
                
                if ("enabled".equalsIgnoreCase(status)) {
                    enabledContentTypes.add(contentType);
                    log.info("Content type {} is already enabled", contentType);
                }
            }

            // Identify content types that need to be started
            for (String contentType : CONTENT_TYPES) {
                if (!enabledContentTypes.contains(contentType)) {
                    contentTypesToStart.add(contentType);
                    log.info("Content type {} needs to be started", contentType);
                }
            }

            // If all content types are already enabled, we're done
            if (contentTypesToStart.isEmpty()) {
                log.info("All content types are already enabled. No subscriptions need to be started.");
                return;
            }
        } catch (Exception e) {
            // If listing subscriptions fails, fall back to starting all content types
            log.warn("Failed to list subscriptions, will attempt to start all content types as fallback: {}", e.getMessage());
            for (String contentType : CONTENT_TYPES) {
                contentTypesToStart.add(contentType);
            }
        }

        // Start subscriptions for the identified content types
        try {
            startSubscriptionsForContentTypes(contentTypesToStart);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            HttpStatus statusCode = e.getStatusCode();
            publishErrorTypeMetricCounter(statusCode.getReasonPhrase(), this.errorTypeMetricCounterMap);
            log.error(NOISY, "Failed to initialize subscriptions with status code {}: {}",
                    statusCode, e.getMessage());
            throw new RuntimeException("Failed to initialize subscriptions: " + e.getMessage(), e);
        } catch (Exception e) {
            // FORBIDDEN throws SecurityException in RetryHandler
            if (e instanceof SecurityException) {
                publishErrorTypeMetricCounter(HttpStatus.FORBIDDEN.getReasonPhrase(), this.errorTypeMetricCounterMap);
            }
            log.error(NOISY, "Failed to initialize subscriptions", e);
            throw new RuntimeException("Failed to initialize subscriptions: " + e.getMessage(), e);
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

        final HttpHeaders headers = new HttpHeaders();

        return searchCallLatencyTimer.record(() -> {
            try {
                return RetryHandler.executeWithRetry(
                        () -> {
                            headers.setBearerAuth(authConfig.getAccessToken());

                            ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                                    url,
                                    HttpMethod.GET,
                                    new HttpEntity<>(headers),
                                    new ParameterizedTypeReference<>() {}
                            );
                            // Record search request size.
                            searchResponseSizeSummary.record(response.getHeaders().getContentLength());

                            // Extract NextPageUri from response headers
                            List<String> nextPageHeaders = response.getHeaders().get("NextPageUri");
                            String nextPageUri = (nextPageHeaders != null && !nextPageHeaders.isEmpty()) ?
                                    nextPageHeaders.get(0) : null;

                            if (nextPageUri != null) {
                                log.debug("Next page URI found: {}", nextPageUri);
                            }

                            searchRequestsSuccessCounter.increment();
                            return new AuditLogsResponse(response.getBody(), nextPageUri);
                        },
                        authConfig::renewCredentials,
                        searchRequestsFailedCounter
                );
            } catch (HttpClientErrorException | HttpServerErrorException e) {
                HttpStatus statusCode = e.getStatusCode();
                publishErrorTypeMetricCounter(statusCode.getReasonPhrase(), this.errorTypeMetricCounterMap);
                log.error(NOISY, "Error while fetching audit logs for content type {}", contentType, e);
                throw new RuntimeException("Failed to fetch audit logs", e);
            } catch (Exception e) {
                // FORBIDDEN throws SecurityException in RetryHandler
                if (e instanceof SecurityException) {
                    publishErrorTypeMetricCounter(HttpStatus.FORBIDDEN.getReasonPhrase(), this.errorTypeMetricCounterMap);
                }
                log.error(NOISY, "Error while fetching audit logs for content type {}", contentType, e);
                throw new RuntimeException("Failed to fetch audit logs", e);
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
            throw new Office365Exception("ContentUri must be from Office365 Management API: " + contentUri, false);
        }
        auditLogsRequestedCounter.increment();
        final HttpHeaders headers = new HttpHeaders();

        return auditLogFetchLatencyTimer.record(() -> {
            try {
                String response = RetryHandler.executeWithRetry(() -> {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    ResponseEntity<String> responseEntity = restTemplate.exchange(
                            contentUri,
                            HttpMethod.GET,
                            new HttpEntity<>(headers),
                            String.class
                    );

                    // Record audit log request size from response body
                    String responseBody = responseEntity.getBody();
                    if (responseBody != null) {
                        auditLogResponseSizeSummary.record(responseBody.getBytes(StandardCharsets.UTF_8).length);
                    }

                    return responseBody;
                }, authConfig::renewCredentials, auditLogRequestsFailedCounter);
                auditLogRequestsSuccessCounter.increment();
                return response;
            } catch (HttpClientErrorException | HttpServerErrorException e) {
                HttpStatus statusCode = e.getStatusCode();
                publishErrorTypeMetricCounter(statusCode.getReasonPhrase(), this.errorTypeMetricCounterMap);
                log.error(NOISY, "Error while fetching audit log content from URI: {}", contentUri, e);
                throw new RuntimeException("Failed to fetch audit log", e);
            } catch (Exception e) {
                // FORBIDDEN throws SecurityException in RetryHandler
                if (e instanceof SecurityException) {
                    publishErrorTypeMetricCounter(HttpStatus.FORBIDDEN.getReasonPhrase(), this.errorTypeMetricCounterMap);
                }
                log.error(NOISY, "Error while fetching audit log content from URI: {}", contentUri, e);
                throw new RuntimeException("Failed to fetch audit log", e);
            }
        });
    }
}
